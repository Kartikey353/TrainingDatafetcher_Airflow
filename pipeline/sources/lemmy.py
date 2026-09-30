"""Lemmy API client, plus pure functions that turn API objects into RawRecords."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse

import httpx

from ..models import RawRecord, hash_author

log = logging.getLogger(__name__)

USER_AGENT = "social-data-pipeline/0.1 (personal research project)"
PAGE_SIZE = 50  # Lemmy's maximum page size


def parse_ts(s: str) -> int:
    """Lemmy timestamps are ISO 8601, sometimes without a timezone (they are UTC)."""
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


class LemmyClient:
    """Polite HTTP client for one Lemmy instance: fixed delay between calls, retries with backoff."""

    def __init__(
        self,
        instance: str,
        *,
        delay: float = 1.0,
        timeout: float = 20.0,
        max_retries: int = 4,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.instance = instance
        self._delay = delay
        self._max_retries = max_retries
        self._sleep = sleep
        self._first_call = True
        self._http = httpx.Client(
            base_url=f"https://{instance}/api/v3",
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "LemmyClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            if not self._first_call:
                self._sleep(self._delay)
            self._first_call = False

            wait = 0.0
            try:
                r = self._http.get(path, params=params)
            except (httpx.TimeoutException, httpx.TransportError) as e:
                last_error = e
            else:
                if r.status_code == 200:
                    return r.json()
                if r.status_code not in (429, 500, 502, 503, 504):
                    r.raise_for_status()  # 4xx like 404 = our config is wrong; retrying won't help
                last_error = RuntimeError(f"{self.instance}{path} returned HTTP {r.status_code}")
                try:
                    wait = float(r.headers.get("Retry-After", 0))
                except ValueError:
                    wait = 0.0

            if attempt < self._max_retries:
                backoff = max(wait, 2.0 ** attempt)
                log.warning("Retrying %s in %.1fs (attempt %d): %s", path, backoff, attempt + 1, last_error)
                self._sleep(backoff)
        raise RuntimeError(f"Giving up on {self.instance}{path}") from last_error

    def list_posts(self, community_name: str, page: int) -> list[dict[str, Any]]:
        data = self._get(
            "/post/list",
            {"community_name": community_name, "sort": "New", "limit": PAGE_SIZE, "page": page},
        )
        return data.get("posts", [])

    def list_comments(self, post_id: int, page: int) -> list[dict[str, Any]]:
        data = self._get(
            "/comment/list",
            {"post_id": post_id, "sort": "Top", "max_depth": 8, "limit": PAGE_SIZE, "page": page, "type_": "All"},
        )
        return data.get("comments", [])

    def get_site(self) -> dict[str, Any]:
        return self._get("/site", {})

    def list_local_posts(self, limit: int = 1) -> list[dict[str, Any]]:
        return self._get("/post/list", {"type_": "Local", "sort": "New", "limit": limit}).get("posts", [])

    def list_communities(self, page: int, sort: str = "TopMonth") -> list[dict[str, Any]]:
        data = self._get(
            "/community/list",
            {"type_": "Local", "sort": sort, "limit": PAGE_SIZE, "page": page, "show_nsfw": "false"},
        )
        return data.get("communities", [])


# ---------------------------------------------------------------- mapping (pure functions)

def _author_key(creator: dict[str, Any]) -> str:
    """'name@home-instance' is unique across the fediverse; only its hash is ever stored."""
    host = urlparse(creator.get("actor_id", "")).netloc
    return f"{creator.get('name', '')}@{host}"


def is_dropped_author(creator: dict[str, Any], drop_authors: list[str]) -> bool:
    if creator.get("bot_account") or creator.get("deleted") or creator.get("banned"):
        return True
    return creator.get("name", "").lower() in {a.lower() for a in drop_authors}


def post_record_id(instance: str, post_id: int | str) -> str:
    return f"lemmy:{instance}:post:{post_id}"


def comment_record_id(instance: str, comment_id: int | str) -> str:
    return f"lemmy:{instance}:comment:{comment_id}"


def post_to_record(pv: dict[str, Any], instance: str, community_name: str) -> RawRecord:
    post = pv["post"]
    return RawRecord(
        id=post_record_id(instance, post["id"]),
        source="lemmy",
        kind="post",
        community=f"{community_name}@{instance}",
        created_utc=parse_ts(post["published"]),
        title=post.get("name", ""),
        body=post.get("body") or "",
        author_hash=hash_author(_author_key(pv["creator"])),
        score=pv["counts"]["score"],
        url=post.get("ap_id", ""),
        meta={
            "post_id": post["id"],
            "link": post.get("url"),
            "num_comments": pv["counts"].get("comments", 0),
            "language_id": post.get("language_id"),
        },
    )


def comment_to_record(cv: dict[str, Any], instance: str, community_name: str) -> RawRecord:
    c = cv["comment"]
    # path looks like "0.<top-level id>.<reply id>..."; the second-to-last entry is the parent
    parts = str(c.get("path", "0")).split(".")
    parent = parts[-2] if len(parts) >= 3 else None
    return RawRecord(
        id=comment_record_id(instance, c["id"]),
        source="lemmy",
        kind="comment",
        community=f"{community_name}@{instance}",
        created_utc=parse_ts(c["published"]),
        body=c.get("content", ""),
        author_hash=hash_author(_author_key(cv["creator"])),
        score=cv["counts"]["score"],
        parent_id=comment_record_id(instance, parent) if parent else post_record_id(instance, c["post_id"]),
        url=c.get("ap_id", ""),
        meta={"post_id": c["post_id"], "depth": len(parts) - 2, "language_id": c.get("language_id")},
    )
