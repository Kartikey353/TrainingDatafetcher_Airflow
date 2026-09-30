"""Stage 1 smoke test. Trigger manually from the Airflow UI.

Proves three things from inside the Airflow containers:
  1. our config file loads and lists the Lemmy communities
  2. MongoDB is reachable and writable
  3. the Lemmy API is reachable
"""
from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import dag, task

USER_AGENT = "social-data-pipeline/0.1 (personal research project)"


@dag(
    dag_id="stage1_smoke_test",
    schedule=None,  # manual trigger only
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["stage1"],
)
def stage1_smoke_test():
    @task
    def check_config() -> list[dict]:
        from pipeline.config import Config

        cfg = Config.load()
        communities = cfg.sources["lemmy"]["communities"]
        for c in communities:
            print(f"community: {c['name']}@{c['instance']}")
        return communities

    @task
    def check_mongo() -> int:
        from pipeline.db import get_db

        db = get_db()
        db.command("ping")
        db["_smoke"].insert_one({"ts": datetime.now(timezone.utc), "note": "stage1 smoke test"})
        n = db["_smoke"].count_documents({})
        print(f"MongoDB OK, _smoke collection now has {n} document(s)")
        return n

    @task
    def check_lemmy(communities: list[dict]) -> dict:
        import httpx

        first = communities[0]
        r = httpx.get(
            f"https://{first['instance']}/api/v3/post/list",
            params={"community_name": first["name"], "sort": "New", "limit": 2},
            headers={"User-Agent": USER_AGENT},
            timeout=20,
        )
        r.raise_for_status()
        posts = r.json()["posts"]
        result = {
            "community": f"{first['name']}@{first['instance']}",
            "posts_returned": len(posts),
            "first_title": posts[0]["post"]["name"] if posts else None,
        }
        print(result)
        return result

    communities = check_config()
    check_mongo()
    check_lemmy(communities)


stage1_smoke_test()
