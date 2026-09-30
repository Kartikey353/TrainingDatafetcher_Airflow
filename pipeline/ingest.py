"""Ingest one Lemmy community into MongoDB.

Design: idempotent and restartable, no fragile "high-water mark" bookkeeping.
  * Every item is stored under a deterministic _id, so re-fetching never duplicates.
  * A post counts as "known" once it exists in raw_items; known posts are never refetched.
  * Posts younger than min_age_hours are skipped until their discussion has had time to grow.
  * First run = backfill (scan up to backfill_max_posts). After that = incremental
    (scan newest-first, stop after `stop_after_known` consecutive already-stored posts).
  * A post is written LAST, after its comments, so a crash mid-post leaves nothing that looks "known".
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from pymongo import ReplaceOne
from pymongo.database import Database

from .models import RawRecord
from .sources.lemmy import (
    PAGE_SIZE,
    LemmyClient,
    comment_to_record,
    is_dropped_author,
    parse_ts,
    post_record_id,
    post_to_record,
)

log = logging.getLogger(__name__)

RAW = "raw_items"
STATE = "state"
INCREMENTAL_SCAN_CAP = 1000  # safety net: never scan more posts than this in a normal run


def ensure_indexes(db: Database) -> None:
    db[RAW].create_index([("source", 1), ("community", 1), ("created_utc", -1)])
    db[RAW].create_index([("kind", 1)])
    db[RAW].create_index([("meta.post_id", 1), ("community", 1)])  # find all comments of a post


def _known_ids(db: Database, ids: list[str]) -> set[str]:
    return {d["_id"] for d in db[RAW].find({"_id": {"$in": ids}}, {"_id": 1})}


def _fetch_comments(
    client: LemmyClient,
    instance: str,
    name: str,
    post_id: int,
    cfg: dict[str, Any],
    drop_authors: list[str],
) -> list[RawRecord]:
    max_comments = cfg.get("max_comments_per_post", 300)
    min_score = cfg.get("min_score", 1)
    out: list[RawRecord] = []
    page = 1
    while len(out) < max_comments:
        views = client.list_comments(post_id, page)
        for cv in views:
            c = cv["comment"]
            if c.get("deleted") or c.get("removed"):
                continue
            if is_dropped_author(cv["creator"], drop_authors):
                continue
            if cv["counts"]["score"] < min_score:
                continue
            out.append(comment_to_record(cv, instance, name))
            if len(out) >= max_comments:
                break
        if len(views) < PAGE_SIZE:
            break
        page += 1
    return out


def ingest_community(
    *,
    client: LemmyClient,
    db: Database,
    instance: str,
    name: str,
    cfg: dict[str, Any],
    drop_nsfw: bool = True,
    drop_authors: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    drop_authors = drop_authors or []
    state_id = f"lemmy:{instance}:{name}"

    state = db[STATE].find_one({"_id": state_id}) or {}
    backfill = not state.get("backfill_done", False)

    min_age = cfg.get("min_age_hours", 6) * 3600
    min_score = cfg.get("min_score", 1)
    posts_per_run = cfg.get("posts_per_run", 50)
    stop_after_known = cfg.get("stop_after_known", 20)
    scan_cap = cfg.get("backfill_max_posts", 2000) if backfill else INCREMENTAL_SCAN_CAP

    stats: Counter[str] = Counter()
    known_streak = 0
    page = 1
    stop = False
    log.info("Ingesting %s@%s in %s mode", name, instance, "BACKFILL" if backfill else "incremental")

    while not stop:
        views = client.list_posts(name, page)
        if not views:
            break  # ran out of history
        known = _known_ids(db, [post_record_id(instance, pv["post"]["id"]) for pv in views])

        for pv in views:
            post = pv["post"]
            stats["scanned"] += 1
            if stats["scanned"] > scan_cap:
                stop = True
                break

            if now.timestamp() - parse_ts(post["published"]) < min_age:
                stats["too_young"] += 1  # picked up by a later run once mature
                continue

            if post_record_id(instance, post["id"]) in known:
                stats["known"] += 1
                known_streak += 1
                if not backfill and known_streak >= stop_after_known:
                    stop = True
                    break
                continue

            if (
                post.get("deleted")
                or post.get("removed")
                or (drop_nsfw and (post.get("nsfw") or pv.get("community", {}).get("nsfw")))
                or is_dropped_author(pv["creator"], drop_authors)
                or pv["counts"]["score"] < min_score
            ):
                stats["filtered"] += 1
                continue

            comments = (
                _fetch_comments(client, instance, name, post["id"], cfg, drop_authors)
                if pv["counts"].get("comments", 0) > 0
                else []
            )
            records = comments + [post_to_record(pv, instance, name)]  # post last, see module docstring
            db[RAW].bulk_write(
                [ReplaceOne({"_id": r.id}, r.to_document(now), upsert=True) for r in records],
                ordered=True,
            )
            stats["new_posts"] += 1
            stats["new_comments"] += len(comments)
            known_streak = 0

            if not backfill and stats["new_posts"] >= posts_per_run:
                stop = True
                break
        page += 1

    # Only reached if nothing raised, i.e. the backfill scan really completed.
    db[STATE].update_one(
        {"_id": state_id},
        {"$set": {"backfill_done": True, "last_run": now, "last_stats": dict(stats)}},
        upsert=True,
    )
    result = {"community": f"{name}@{instance}", "mode": "backfill" if backfill else "incremental", **stats}
    log.info("Done: %s", result)
    return result
