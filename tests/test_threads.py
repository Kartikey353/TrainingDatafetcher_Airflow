from datetime import datetime, timezone

from pipeline.ingest import RAW, ingest_community
from pipeline.threads import THREADS, UNCATEGORIZED, assemble_threads, build_tree, category_map
from tests.factories import FakeClient, comment_view, iso, post_view

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
CFG = {"min_age_hours": 6, "min_score": 1, "posts_per_run": 50, "backfill_max_posts": 100,
       "stop_after_known": 2, "max_comments_per_post": 300}
CATEGORIES = {"cooking@lemmy.world": "Food"}


def ingest(db, posts, comments):
    return ingest_community(client=FakeClient(posts, comments), db=db, instance="lemmy.world", name="cooking",
                            cfg=CFG, drop_nsfw=True, drop_authors=[], now=NOW)


def doc(cid, parent, score=1, created=0, body="x"):
    return {"_id": cid, "parent_id": parent, "score": score, "created_utc": created, "body": body, "author_hash": "h"}


def test_build_tree_nests_replies_and_sorts_by_score():
    docs = [doc("c1", "P", score=2), doc("c2", "P", score=9), doc("c3", "c1", score=1), doc("c4", "c3", score=5)]
    roots = build_tree(docs, "P")
    assert [r["id"] for r in roots] == ["c2", "c1"]                    # best score first
    assert roots[1]["replies"][0]["id"] == "c3"
    assert roots[1]["replies"][0]["replies"][0]["id"] == "c4"          # 3 levels deep
    assert not any("orphaned" in r for r in roots)


def test_orphans_are_kept_and_flagged():
    roots = build_tree([doc("c1", "P"), doc("c9", "missing-parent")], "P")
    orphan = next(r for r in roots if r["id"] == "c9")
    assert orphan["orphaned"] is True


def test_category_map_and_default():
    cfg = {"communities": [{"instance": "lemmy.world", "name": "cooking", "category": "Food"},
                           {"instance": "lemmy.world", "name": "x"}]}
    m = category_map(cfg)
    assert m["cooking@lemmy.world"] == "Food" and m["x@lemmy.world"] == UNCATEGORIZED


def test_end_to_end_thread_shape(db):
    posts = [post_view(2, iso(NOW, 8), title="Best pasta?", body="Looking for ideas", comments=3)]
    comments = {2: [
        comment_view(21, 2, iso(NOW, 7), path="0.21", content="Carbonara", score=10),
        comment_view(22, 2, iso(NOW, 7), path="0.21.22", content="Agreed", score=4),
        comment_view(23, 2, iso(NOW, 7), path="0.23", content="Try cacio e pepe", score=2),
    ]}
    ingest(db, posts, comments)
    stats = assemble_threads(db, CATEGORIES, now=NOW)
    assert stats["built"] == 1 and stats["comments"] == 3

    t = db[THREADS].find_one({"_id": "lemmy:lemmy.world:post:2"})
    assert t["category"] == "Food"
    assert t["topic"] == "Best pasta?" and t["description"] == "Looking for ideas"
    assert t["num_comments"] == 3
    assert [c["text"] for c in t["comments"]] == ["Carbonara", "Try cacio e pepe"]
    assert t["comments"][0]["replies"][0]["text"] == "Agreed"           # nested reply
    assert "alice" not in str(t) and "bob" not in str(t)


def test_is_incremental_and_rebuild(db):
    ingest(db, [post_view(2, iso(NOW, 8), comments=0), post_view(3, iso(NOW, 9), comments=0)], {})
    assert assemble_threads(db, CATEGORIES, now=NOW)["built"] == 2
    assert assemble_threads(db, CATEGORIES, now=NOW) == {"already_built": 2}        # nothing new -> nothing rebuilt
    # change a category and rebuild everything
    assemble_threads(db, {"cooking@lemmy.world": "Recipes"}, rebuild=True, now=NOW)
    assert {t["category"] for t in db[THREADS].find()} == {"Recipes"}
    assert db[THREADS].count_documents({}) == 2


def test_post_without_comments_has_empty_list_and_unknown_community_is_uncategorized(db):
    ingest(db, [post_view(2, iso(NOW, 8), comments=0)], {})
    assemble_threads(db, {}, now=NOW)
    t = db[THREADS].find_one({})
    assert t["comments"] == [] and t["category"] == UNCATEGORIZED


def test_raw_layer_untouched(db):
    ingest(db, [post_view(2, iso(NOW, 8), comments=1)], {2: [comment_view(21, 2, iso(NOW, 7))]})
    before = db[RAW].count_documents({})
    assemble_threads(db, CATEGORIES, now=NOW)
    assert db[RAW].count_documents({}) == before
