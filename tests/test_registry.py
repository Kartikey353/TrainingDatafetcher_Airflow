from datetime import datetime, timezone

from pipeline.discovery import COMMUNITIES, INSTANCES
from pipeline.registry import all_categories, communities_for_instance, configured_instances, norm_host

NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)
CFG = {
    "instances": ["lemmy.world", "https://Beehaw.org/", "lemmy.world"],
    "communities": [{"instance": "lemmy.world", "name": "cooking", "category": "Food"},
                    {"instance": "programming.dev", "name": "cs_career_questions", "category": "Career"}],
}


def add(db, name, instance, category, status):
    db[COMMUNITIES].insert_one({"_id": f"{name}@{instance}", "source": "lemmy", "instance": instance,
                                "name": name, "category": category, "status": status})


def test_norm_host():
    assert norm_host(" HTTPS://Beehaw.org/ ") == "beehaw.org"


def test_configured_instances_dedupes_and_includes_pinned_hosts():
    assert configured_instances(CFG) == ["lemmy.world", "beehaw.org", "programming.dev"]


def test_communities_for_instance_merges_pinned_and_approved_only(db):
    add(db, "dating", "lemmy.world", "Relationships", "approved")
    add(db, "memes", "lemmy.world", "Food", "rejected")
    add(db, "chat", "lemmy.world", "Casual Conversation", "pending")
    names = {c["name"] for c in communities_for_instance(db, CFG, "lemmy.world")}
    assert names == {"cooking", "dating"}


def test_pinned_category_wins_over_discovered(db):
    add(db, "cooking", "lemmy.world", "Something Else", "approved")
    assert all_categories(db, CFG)["cooking@lemmy.world"] == "Food"


def test_unsupported_instance_is_skipped_but_unreachable_is_not(db):
    db[INSTANCES].insert_one({"_id": "lemmy.world", "status": "unsupported"})
    assert communities_for_instance(db, CFG, "lemmy.world") == []
    db[INSTANCES].replace_one({"_id": "lemmy.world"}, {"_id": "lemmy.world", "status": "unreachable"})
    assert len(communities_for_instance(db, CFG, "lemmy.world")) == 1


def test_all_categories_covers_every_ingested_community(db):
    add(db, "dating", "beehaw.org", "Relationships", "approved")
    cats = all_categories(db, CFG)
    assert cats == {"cooking@lemmy.world": "Food", "cs_career_questions@programming.dev": "Career",
                    "dating@beehaw.org": "Relationships"}
