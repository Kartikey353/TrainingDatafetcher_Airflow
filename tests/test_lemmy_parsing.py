from datetime import datetime, timezone

from pipeline.sources.lemmy import comment_to_record, is_dropped_author, parse_ts, post_to_record
from tests.factories import comment_view, creator, iso, post_view

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def test_parse_ts_handles_z_and_naive():
    assert parse_ts("2026-09-20T12:00:00.123456Z") == parse_ts("2026-09-20T12:00:00.123456")
    assert parse_ts("2026-09-20T12:00:00Z") == int(NOW.timestamp())


def test_post_record_fields_and_no_username(monkeypatch):
    monkeypatch.setenv("AUTHOR_HASH_SALT", "salt")
    rec = post_to_record(post_view(7, iso(NOW, 10), title="T", body="B", author="alice"), "lemmy.world", "cooking")
    assert rec.id == "lemmy:lemmy.world:post:7"
    assert rec.community == "cooking@lemmy.world"
    assert rec.title == "T" and rec.body == "B" and rec.kind == "post"
    assert rec.author_hash and "alice" not in str(rec.to_document(NOW))


def test_same_author_same_hash_different_author_different_hash():
    a1 = post_to_record(post_view(1, iso(NOW, 9), author="alice"), "lemmy.world", "c")
    a2 = post_to_record(post_view(2, iso(NOW, 9), author="alice"), "lemmy.world", "c")
    b = post_to_record(post_view(3, iso(NOW, 9), author="carol"), "lemmy.world", "c")
    assert a1.author_hash == a2.author_hash != b.author_hash


def test_comment_parent_ids():
    top = comment_to_record(comment_view(11, 7, iso(NOW, 9), path="0.11"), "lemmy.world", "cooking")
    reply = comment_to_record(comment_view(12, 7, iso(NOW, 9), path="0.11.12"), "lemmy.world", "cooking")
    assert top.parent_id == "lemmy:lemmy.world:post:7" and top.meta["depth"] == 0
    assert reply.parent_id == "lemmy:lemmy.world:comment:11" and reply.meta["depth"] == 1


def test_dropped_authors():
    assert is_dropped_author(creator("x", bot=True), [])
    assert is_dropped_author(creator("AutoModerator"), ["automoderator"])
    assert not is_dropped_author(creator("alice"), ["AutoModerator"])
