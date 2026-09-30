from datetime import datetime, timedelta, timezone

import pytest

from pipeline.ingest import RAW, STATE, ingest_community
from tests.factories import FakeClient, comment_view, iso, post_view

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
CFG = {"min_age_hours": 6, "min_score": 1, "posts_per_run": 50, "backfill_max_posts": 100,
       "stop_after_known": 2, "max_comments_per_post": 300}


def run(db, client, now=NOW, cfg=CFG):
    return ingest_community(client=client, db=db, instance="lemmy.world", name="cooking", cfg=cfg,
                            drop_nsfw=True, drop_authors=["AutoModerator"], now=now)


def ids(db, kind):
    return {d["_id"] for d in db[RAW].find({"kind": kind})}


def make_posts():
    # newest first
    return [
        post_view(1, iso(NOW, 2)),                        # too young
        post_view(2, iso(NOW, 8), comments=2),            # good, 2 comments
        post_view(3, iso(NOW, 9), nsfw=True),             # nsfw -> dropped
        post_view(4, iso(NOW, 10), deleted=True),         # deleted -> dropped
        post_view(5, iso(NOW, 11), bot=True),             # bot -> dropped
        post_view(6, iso(NOW, 12), score=0),              # below min_score -> dropped
        post_view(7, iso(NOW, 13), comments=0),           # good, no comments
        post_view(8, iso(NOW, 14), author="AutoModerator"),  # dropped author
    ]


def make_comments():
    return {2: [
        comment_view(21, 2, iso(NOW, 7), path="0.21"),
        comment_view(22, 2, iso(NOW, 7), path="0.21.22", content="reply"),
        comment_view(23, 2, iso(NOW, 7), removed=True),     # removed -> dropped
        comment_view(24, 2, iso(NOW, 7), score=0),          # low score -> dropped
        comment_view(25, 2, iso(NOW, 7), bot=True),         # bot -> dropped
    ]}


def test_first_run_backfills_and_filters(db):
    stats = run(db, FakeClient(make_posts(), make_comments()))
    assert ids(db, "post") == {"lemmy:lemmy.world:post:2", "lemmy:lemmy.world:post:7"}
    assert ids(db, "comment") == {"lemmy:lemmy.world:comment:21", "lemmy:lemmy.world:comment:22"}
    assert stats["mode"] == "backfill" and stats["new_posts"] == 2 and stats["too_young"] == 1
    assert db[STATE].find_one({"_id": "lemmy:lemmy.world:cooking"})["backfill_done"] is True


def test_second_run_is_idempotent_and_skips_known_posts(db):
    run(db, FakeClient(make_posts(), make_comments()))
    before = db[RAW].count_documents({})
    client2 = FakeClient(make_posts(), make_comments())
    stats = run(db, client2)
    assert stats["mode"] == "incremental" and stats.get("new_posts", 0) == 0
    assert client2.comment_calls == []          # never re-fetched comments
    assert db[RAW].count_documents({}) == before


def test_incremental_picks_up_new_and_matured_posts_then_stops_early(db):
    run(db, FakeClient(make_posts(), make_comments()))
    later = NOW + timedelta(hours=5)             # post 1 is now 7h old -> mature
    posts = [
        post_view(1, iso(NOW, 2)),
        post_view(9, iso(later, 8)),                  # brand-new mature post, published 3h after the first run
    ] + [p for p in make_posts()[1:]]
    posts.sort(key=lambda pv: pv["post"]["published"], reverse=True)
    client = FakeClient(posts, {}, page_size=2)
    stats = run(db, client, now=later)
    assert {"lemmy:lemmy.world:post:1", "lemmy:lemmy.world:post:9"} <= ids(db, "post")
    assert stats["new_posts"] == 2
    # 9 posts / 2 per page = 5 pages. Filtered posts (nsfw, deleted, ...) are neutral for the "known streak",
    # so the scan stops on page 4 after two known posts in a row, without reading the last page.
    assert client.post_pages_requested == 4


def test_comment_cap(db):
    comments = {2: [comment_view(100 + i, 2, iso(NOW, 7)) for i in range(5)]}
    cfg = {**CFG, "max_comments_per_post": 2}
    run(db, FakeClient([post_view(2, iso(NOW, 8), comments=5)], comments), cfg=cfg)
    assert len(ids(db, "comment")) == 2


def test_failure_mid_post_leaves_no_partial_post_and_retry_recovers(db):
    with pytest.raises(RuntimeError):
        run(db, FakeClient(make_posts(), make_comments(), fail_comments_for={2}))
    assert "lemmy:lemmy.world:post:2" not in ids(db, "post")
    assert db[STATE].find_one({"_id": "lemmy:lemmy.world:cooking"}) is None   # backfill not marked done
    run(db, FakeClient(make_posts(), make_comments()))
    assert "lemmy:lemmy.world:post:2" in ids(db, "post")


def test_no_usernames_are_stored(db):
    run(db, FakeClient(make_posts(), make_comments()))
    dump = str(list(db[RAW].find()))
    assert "alice" not in dump and "bob" not in dump
