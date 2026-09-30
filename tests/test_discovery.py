from datetime import datetime, timezone

import httpx

from pipeline.discovery import (
    COMMUNITIES,
    discover_instance,
    match_theme,
    passes_gates,
    probe_instance,
    upsert_communities,
)

NOW = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)
THEMES = {
    "Relationships": ["relationship", "dating", "love"],
    "Food": ["food", "cooking", "recipe"],
    "Casual Conversation": ["casualconversation", "conversation", "chat"],
    "Career": ["career", "job", "jobs", "workplace"],
}
EXCLUDE = ["meme", "porn", "nsfw"]
GATES = {"min_comments": 300, "min_comments_per_post": 2, "min_active_users": 5, "max_per_instance": 2, "pages": 3}


def comm(name, title="", nsfw=False):
    return {"name": name, "title": title, "nsfw": nsfw}


def view(name, comments=1000, posts=100, users=50, title="", nsfw=False):
    return {"community": comm(name, title, nsfw),
            "counts": {"comments": comments, "posts": posts, "users_active_month": users, "subscribers": 10}}


# ---- theme matching
def test_matches_words_in_name_and_title():
    assert match_theme(comm("relationship_advice"), THEMES, EXCLUDE) == ("Relationships", ["relationship"])
    assert match_theme(comm("cooking"), THEMES, EXCLUDE)[0] == "Food"
    assert match_theme(comm("x", "Recipes & Baking"), THEMES, EXCLUDE)[0] == "Food"     # plural -> singular
    assert match_theme(comm("casualconversation"), THEMES, EXCLUDE)[0] == "Casual Conversation"


def test_long_keywords_match_inside_names_short_ones_do_not():
    assert match_theme(comm("relationshipsupport"), THEMES, EXCLUDE)[0] == "Relationships"
    assert match_theme(comm("updating_news"), THEMES, EXCLUDE) is None      # "dating" must not match "updating"
    assert match_theme(comm("legaladvice"), THEMES, EXCLUDE) is None


def test_excluded_words_win():
    assert match_theme(comm("relationshipmemes"), THEMES, EXCLUDE) is None
    assert match_theme(comm("cooking", "NSFW cooking"), THEMES, EXCLUDE) is None


def test_tie_goes_to_first_theme_in_config_order():
    assert match_theme(comm("food_jobs"), THEMES, EXCLUDE)[0] == "Food"


def test_more_hits_beats_order():
    assert match_theme(comm("career_job_workplace"), THEMES, EXCLUDE)[0] == "Career"


# ---- gates
def test_gates():
    ok = {"comments": 500, "posts": 100, "users_active_month": 10}
    assert passes_gates(ok, GATES)
    assert not passes_gates({**ok, "comments": 100}, GATES)                 # too little discussion
    assert not passes_gates({**ok, "posts": 1000}, GATES)                   # link dump: 0.5 comments/post
    assert not passes_gates({**ok, "users_active_month": 1}, GATES)
    assert not passes_gates({**ok, "posts": 0}, GATES)


# ---- discovery over a fake instance
class FakeClient:
    def __init__(self, pages):
        self.pages = pages

    def list_communities(self, page, sort):
        return self.pages[page - 1] if page <= len(self.pages) else []


def test_discover_filters_ranks_and_limits():
    client = FakeClient([[
        view("cooking", comments=5000),
        view("nsfwfood", nsfw=True),                     # nsfw flag -> dropped
        view("recipes", comments=200, posts=50),         # fails min_comments
        view("dating", comments=900),
        view("relationship_advice", comments=3000),
        view("politics", comments=9000),                 # no theme
    ]])
    docs = discover_instance(client, "lemmy.world", GATES, THEMES, EXCLUDE)
    assert [d["_id"] for d in docs] == ["cooking@lemmy.world", "relationship_advice@lemmy.world"]   # top 2 by comments
    assert docs[0]["category"] == "Food" and docs[0]["instance"] == "lemmy.world"


# ---- storing discoveries
def test_upsert_keeps_manual_edits(db):
    doc = {"_id": "cooking@lemmy.world", "instance": "lemmy.world", "name": "cooking", "title": "", "category": "Food",
           "matched": ["cooking"], "subscribers": 1, "posts": 10, "comments": 100, "users_active_month": 5}
    assert upsert_communities(db, [doc], auto_approve=True, now=NOW) == 1
    db[COMMUNITIES].update_one({"_id": doc["_id"]}, {"$set": {"category": "My Category", "status": "rejected"}})

    assert upsert_communities(db, [{**doc, "comments": 999}], auto_approve=True, now=NOW) == 0   # not new
    stored = db[COMMUNITIES].find_one({"_id": doc["_id"]})
    assert stored["comments"] == 999                                     # stats refreshed
    assert stored["category"] == "My Category" and stored["status"] == "rejected"   # edits survive


def test_pending_when_not_auto_approved(db):
    doc = {"_id": "a@x", "instance": "x", "name": "a", "title": "", "category": "Food", "matched": [],
           "subscribers": 0, "posts": 1, "comments": 1, "users_active_month": 1}
    upsert_communities(db, [doc], auto_approve=False, now=NOW)
    assert db[COMMUNITIES].find_one({"_id": "a@x"})["status"] == "pending"


# ---- probing an instance
class ProbeClient:
    def __init__(self, site=None, error=None):
        self.site, self.error = site, error

    def get_site(self):
        if self.error:
            raise self.error
        return self.site

    def list_local_posts(self, limit=1):
        return []


def status_error(code):
    return httpx.HTTPStatusError("x", request=httpx.Request("GET", "https://x"), response=httpx.Response(code))


def test_probe_ok():
    r = probe_instance(ProbeClient(site={"version": "0.19.5", "site_view": {"site": {"name": "Lemmy World"}}}), "lemmy.world", NOW)
    assert r["status"] == "ok" and r["version"] == "0.19.5" and r["site_name"] == "Lemmy World"


def test_probe_classifies_failures():
    assert probe_instance(ProbeClient(error=status_error(403)), "x", NOW)["status"] == "unsupported"
    assert "login" in probe_instance(ProbeClient(error=status_error(401)), "x", NOW)["reason"]
    assert "Lemmy" in probe_instance(ProbeClient(error=status_error(404)), "x", NOW)["reason"]
    assert probe_instance(ProbeClient(error=ValueError("not json")), "x", NOW)["status"] == "unsupported"
    assert probe_instance(ProbeClient(error=httpx.ConnectError("down")), "x", NOW)["status"] == "unreachable"
