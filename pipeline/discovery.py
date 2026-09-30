"""Find communities worth ingesting on a Lemmy instance, and check that the instance is usable at all."""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import httpx
from pymongo.database import Database

from .sources.lemmy import LemmyClient

log = logging.getLogger(__name__)

COMMUNITIES = "communities"
INSTANCES = "instances"
_WORD = re.compile(r"[a-z0-9]+")
SUBSTRING_MIN_LEN = 8  # shorter keywords must match a whole word, or "dating" would match "updating"


def ensure_registry_indexes(db: Database) -> None:
    db[COMMUNITIES].create_index([("instance", 1), ("status", 1)])


def _stem(word: str) -> str:
    """Crude plural -> singular, applied to BOTH community words and config keywords, so "recipes" matches
    "recipe" and the keywords "job" and "jobs" count as one hit instead of two."""
    return word[:-1] if word.endswith("s") and len(word) > 3 else word


def _words(*texts: str | None) -> set[str]:
    return {_stem(w) for t in texts for w in _WORD.findall((t or "").lower())}


def match_theme(community: dict[str, Any], themes: dict[str, list[str]], exclude: list[str]) -> tuple[str, list[str]] | None:
    """Return (category, matched keywords) for the best-matching theme, or None."""
    name = (community.get("name") or "").lower()
    title = (community.get("title") or "").lower()
    if any(x in name or x in title for x in exclude):
        return None
    words = _words(name, title)
    best: tuple[str, list[str]] | None = None
    for theme, keywords in themes.items():  # dict order = tie-break order
        hits: list[str] = []
        for k in keywords:
            stem = _stem(k)
            if stem not in hits and (stem in words or (len(k) >= SUBSTRING_MIN_LEN and k in name)):
                hits.append(stem)
        if hits and (best is None or len(hits) > len(best[1])):
            best = (theme, hits)
    return best


def passes_gates(counts: dict[str, Any], cfg: dict[str, Any]) -> bool:
    comments, posts = counts.get("comments", 0), counts.get("posts", 0)
    if posts <= 0 or comments < cfg.get("min_comments", 300):
        return False
    if comments / posts < cfg.get("min_comments_per_post", 2):
        return False
    return counts.get("users_active_month", 0) >= cfg.get("min_active_users", 5)


def discover_instance(
    client: LemmyClient,
    instance: str,
    disc_cfg: dict[str, Any],
    themes: dict[str, list[str]],
    exclude: list[str],
) -> list[dict[str, Any]]:
    found: dict[str, dict[str, Any]] = {}
    for page in range(1, disc_cfg.get("pages", 4) + 1):
        views = client.list_communities(page, disc_cfg.get("sort", "TopMonth"))
        if not views:
            break
        for cv in views:
            c, counts = cv["community"], cv.get("counts", {})
            if c.get("nsfw") or c.get("deleted") or c.get("removed") or c.get("hidden"):
                continue
            match = match_theme(c, themes, exclude)
            if not match or not passes_gates(counts, disc_cfg):
                continue
            found[c["name"]] = {
                "_id": f"{c['name']}@{instance}",
                "instance": instance,
                "name": c["name"],
                "title": c.get("title", ""),
                "category": match[0],
                "matched": match[1],
                "subscribers": counts.get("subscribers", 0),
                "posts": counts.get("posts", 0),
                "comments": counts.get("comments", 0),
                "users_active_month": counts.get("users_active_month", 0),
            }
    best = sorted(found.values(), key=lambda d: -d["comments"])
    return best[: disc_cfg.get("max_per_instance", 15)]


def upsert_communities(db: Database, docs: list[dict[str, Any]], *, auto_approve: bool, now: datetime) -> int:
    """Store discoveries. Stats refresh every run, but `category` and `status` are only set on first
    insert, so anything you edit by hand in MongoDB (rename a category, reject a community) sticks."""
    new = 0
    for d in docs:
        res = db[COMMUNITIES].update_one(
            {"_id": d["_id"]},
            {
                "$set": {
                    "source": "lemmy", "instance": d["instance"], "name": d["name"], "title": d["title"],
                    "matched": d["matched"], "subscribers": d["subscribers"], "posts": d["posts"],
                    "comments": d["comments"], "users_active_month": d["users_active_month"], "last_seen": now,
                },
                "$setOnInsert": {
                    "category": d["category"],
                    "status": "approved" if auto_approve else "pending",
                    "discovered_at": now,
                },
            },
            upsert=True,
        )
        if res.upserted_id is not None:
            new += 1
    return new


def probe_instance(client: LemmyClient, instance: str, now: datetime) -> dict[str, Any]:
    """Is this a Lemmy v3 API we can read anonymously? Result is stored in the `instances` collection."""
    doc: dict[str, Any] = {"_id": instance, "checked_at": now}
    try:
        site = client.get_site()
        client.list_local_posts(1)
        doc.update(
            status="ok",
            reason="",
            version=site.get("version"),
            site_name=((site.get("site_view") or {}).get("site") or {}).get("name"),
        )
    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        hint = " (login required?)" if code in (401, 403) else " (not a Lemmy v3 API?)" if code == 404 else ""
        doc.update(status="unsupported", reason=f"HTTP {code}{hint}")
    except ValueError:  # response was not JSON
        doc.update(status="unsupported", reason="response was not JSON (not a Lemmy API?)")
    except Exception as e:  # network trouble: might be temporary, so ingest still tries this instance
        doc.update(status="unreachable", reason=f"{type(e).__name__}: {str(e)[:100]}")
    return doc
