"""Assemble structured threads from the flat raw layer.

raw_items (flat: one doc per post / comment)  --->  threads (one doc per post, comments nested)

    {
      "category": "Relationships",
      "topic": "<post title>",
      "description": "<post body>",
      "comments": [ {"text": ..., "replies": [ {"text": ..., "replies": [...]} ]} ]
    }

Only posts that are not yet in `threads` are built, so runs are cheap. A raw post is written AFTER its
comments (see ingest.py), so a post that exists in raw_items always has its full comment set.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from pymongo import ReplaceOne
from pymongo.database import Database

from .ingest import RAW

log = logging.getLogger(__name__)

THREADS = "threads"
UNCATEGORIZED = "Uncategorized"


def ensure_thread_indexes(db: Database) -> None:
    db[THREADS].create_index([("category", 1), ("created_utc", -1)])
    db[THREADS].create_index([("community", 1)])


def category_map(lemmy_cfg: dict[str, Any]) -> dict[str, str]:
    """'cooking@lemmy.world' -> 'Food', from config/sources.yaml."""
    return {
        f"{c['name']}@{c['instance']}": c.get("category", UNCATEGORIZED)
        for c in lemmy_cfg.get("communities", [])
    }


def _node(doc: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": doc["_id"],
        "author_hash": doc.get("author_hash", ""),
        "score": doc.get("score", 0),
        "created_utc": doc.get("created_utc", 0),
        "text": doc.get("body", ""),
        "replies": [],
    }


def build_tree(comment_docs: list[dict[str, Any]], post_id: str) -> list[dict[str, Any]]:
    """Nest flat comments using parent_id. Replies whose parent is missing (it was removed, filtered
    or beyond the per-post cap) are kept at the top level and flagged `orphaned: true`, so the
    cleaning stage can decide what to do with them instead of silently losing them."""
    nodes = {d["_id"]: _node(d) for d in comment_docs}
    roots: list[dict[str, Any]] = []
    for d in comment_docs:
        node, parent = nodes[d["_id"]], d.get("parent_id")
        if parent == post_id:
            roots.append(node)
        elif parent in nodes and parent != d["_id"]:
            nodes[parent]["replies"].append(node)
        else:
            node["orphaned"] = True
            roots.append(node)

    def order(level: list[dict[str, Any]]) -> None:
        level.sort(key=lambda n: (-n["score"], n["created_utc"]))  # best-scored first
        for n in level:
            order(n["replies"])

    order(roots)
    return roots


def build_thread(post: dict[str, Any], comments: list[dict[str, Any]], category: str, now: datetime) -> dict[str, Any]:
    return {
        "_id": post["_id"],
        "category": category,
        "community": post["community"],
        "topic": post.get("title", ""),
        "description": post.get("body", ""),
        "score": post.get("score", 0),
        "created_utc": post.get("created_utc", 0),
        "author_hash": post.get("author_hash", ""),
        "url": post.get("url", ""),
        "link": (post.get("meta") or {}).get("link"),
        "num_comments": len(comments),
        "comments": build_tree(comments, post["_id"]),
        "built_at": now,
    }


def assemble_threads(
    db: Database,
    categories: dict[str, str],
    *,
    rebuild: bool = False,
    batch_size: int = 200,
    now: datetime | None = None,
) -> dict[str, int]:
    """Build a thread for every raw post that has none yet. rebuild=True wipes and rebuilds all
    (use after changing a category in the config)."""
    now = now or datetime.now(timezone.utc)
    if rebuild:
        db[THREADS].delete_many({})

    stats: Counter[str] = Counter()
    batch: list[dict[str, Any]] = []

    def flush() -> None:
        if not batch:
            return
        existing = {d["_id"] for d in db[THREADS].find({"_id": {"$in": [p["_id"] for p in batch]}}, {"_id": 1})}
        ops = []
        for post in batch:
            if post["_id"] in existing:
                stats["already_built"] += 1
                continue
            comments = list(db[RAW].find({
                "kind": "comment",
                "community": post["community"],
                "meta.post_id": post["meta"]["post_id"],
            }))
            category = categories.get(post["community"], UNCATEGORIZED)
            thread = build_thread(post, comments, category, now)
            ops.append(ReplaceOne({"_id": thread["_id"]}, thread, upsert=True))
            stats["built"] += 1
            stats["comments"] += len(comments)
            stats["orphaned_comments"] += sum(1 for _ in _iter_orphans(thread["comments"]))
        if ops:
            db[THREADS].bulk_write(ops, ordered=False)
        batch.clear()

    for post in db[RAW].find({"kind": "post"}):
        batch.append(post)
        if len(batch) >= batch_size:
            flush()
    flush()

    log.info("Threads: %s", dict(stats))
    return dict(stats)


def _iter_orphans(nodes: list[dict[str, Any]]):
    for n in nodes:
        if n.get("orphaned"):
            yield n
        yield from _iter_orphans(n["replies"])
