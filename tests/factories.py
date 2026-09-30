"""Builders for fake Lemmy API objects, plus a fake client, so ingest logic is tested without the network."""
from __future__ import annotations

from datetime import datetime, timedelta


def iso(now: datetime, hours_ago: float) -> str:
    return (now - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def creator(name: str = "alice", bot: bool = False) -> dict:
    return {"name": name, "actor_id": f"https://lemmy.world/u/{name}", "bot_account": bot, "deleted": False, "banned": False}


def post_view(pid, published, *, title="A title", body="Some body text", score=5, comments=2,
              deleted=False, removed=False, nsfw=False, author="alice", bot=False, community_nsfw=False) -> dict:
    return {
        "post": {"id": pid, "name": title, "body": body, "url": None, "published": published,
                 "deleted": deleted, "removed": removed, "nsfw": nsfw,
                 "ap_id": f"https://lemmy.world/post/{pid}", "language_id": 37},
        "creator": creator(author, bot),
        "community": {"name": "cooking", "nsfw": community_nsfw},
        "counts": {"score": score, "comments": comments},
    }


def comment_view(cid, post_id, published, *, path=None, content="a comment", score=3,
                 author="bob", deleted=False, removed=False, bot=False) -> dict:
    return {
        "comment": {"id": cid, "post_id": post_id, "content": content, "published": published,
                    "path": path or f"0.{cid}", "deleted": deleted, "removed": removed,
                    "ap_id": f"https://lemmy.world/comment/{cid}", "language_id": 37},
        "creator": creator(author, bot),
        "counts": {"score": score},
    }


class FakeClient:
    """Stands in for LemmyClient. `posts` must be newest-first, like the real API with sort=New."""

    def __init__(self, posts, comments=None, page_size=3, fail_comments_for=()):
        self.posts = posts
        self.comments = comments or {}
        self.page_size = page_size
        self.fail_comments_for = set(fail_comments_for)
        self.post_pages_requested = 0
        self.comment_calls: list[int] = []

    def list_posts(self, community_name, page):
        self.post_pages_requested += 1
        start = (page - 1) * self.page_size
        return self.posts[start:start + self.page_size]

    def list_comments(self, post_id, page):
        self.comment_calls.append(post_id)
        if post_id in self.fail_comments_for:
            raise RuntimeError("boom")
        return self.comments.get(post_id, []) if page == 1 else []
