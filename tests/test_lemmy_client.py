import httpx
import pytest

from pipeline.sources.lemmy import LemmyClient


def make_client(handler, sleeps):
    return LemmyClient("lemmy.world", delay=0.5, transport=httpx.MockTransport(handler), sleep=sleeps.append)


def test_retries_on_429_then_succeeds():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(200, json={"posts": [{"post": {"id": 1}}]})

    sleeps: list[float] = []
    posts = make_client(handler, sleeps).list_posts("cooking", 1)
    assert posts == [{"post": {"id": 1}}] and calls["n"] == 2
    assert 3.0 in sleeps                       # honoured Retry-After


def test_404_is_not_retried():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(404, json={"error": "couldnt_find_community"})

    with pytest.raises(httpx.HTTPStatusError):
        make_client(handler, []).list_posts("nope", 1)
    assert calls["n"] == 1


def test_gives_up_after_max_retries():
    def handler(request):
        return httpx.Response(503)

    with pytest.raises(RuntimeError):
        LemmyClient("lemmy.world", max_retries=2, transport=httpx.MockTransport(handler), sleep=lambda s: None).list_posts("x", 1)


def test_request_shape_and_user_agent():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["ua"] = request.headers["user-agent"]
        return httpx.Response(200, json={"comments": []})

    make_client(handler, []).list_comments(42, 2)
    assert "post_id=42" in seen["url"] and "page=2" in seen["url"] and "limit=50" in seen["url"]
    assert seen["ua"].startswith("social-data-pipeline/")


def test_site_and_community_list_request_shape():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, json={"communities": [], "site_view": {}, "posts": []})

    client = make_client(handler, [])
    client.get_site()
    client.list_communities(3, "TopMonth")
    client.list_local_posts(1)
    assert seen[0].endswith("/api/v3/site")
    assert "type_=Local" in seen[1] and "sort=TopMonth" in seen[1] and "page=3" in seen[1] and "show_nsfw=false" in seen[1]
    assert "/post/list" in seen[2] and "type_=Local" in seen[2]
