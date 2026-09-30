"""The one record shape every stage shares (and, later, every Kafka topic)."""
from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


def hash_author(key: str | None) -> str:
    """Salted hash so usernames are never stored, but repeat authors/bots stay detectable."""
    if not key:
        return ""
    salt = os.environ.get("AUTHOR_HASH_SALT", "")
    return hashlib.sha256(f"{salt}:{key}".encode()).hexdigest()[:16]


@dataclass
class RawRecord:
    id: str                    # globally unique, e.g. "lemmy:lemmy.world:post:123"
    source: str                # "lemmy"
    kind: str                  # "post" | "comment"
    community: str             # "cooking@lemmy.world"
    created_utc: int           # epoch seconds
    title: str = ""            # posts only
    body: str = ""             # raw text, markdown as written by the user
    author_hash: str = ""
    score: int = 0
    parent_id: str = ""        # comments: id of the parent comment, or of the post for top-level comments
    url: str = ""              # canonical link to the item
    meta: dict[str, Any] = field(default_factory=dict)

    def to_document(self, ingested_at: datetime) -> dict[str, Any]:
        """MongoDB document: our id becomes _id, so re-ingesting the same item overwrites, never duplicates."""
        d = asdict(self)
        d["_id"] = d.pop("id")
        d["ingested_at"] = ingested_at
        return d
