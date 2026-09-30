"""Probe every configured Lemmy instance, then discover communities matching your themes.

Run it once by hand before the first ingest, then it repeats weekly. Results land in MongoDB:
  * `instances`   - one doc per instance: status ok / unsupported / unreachable, version, reason
  * `communities` - discovered communities with category, stats and status (approved / pending / rejected)
To reject a community or rename its category, just edit that document (Compass works); it sticks.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from pipeline.config import Config

DEFAULT_CRON = "0 2 * * 0"


def _schedule() -> str:
    return str(Config.load().schedules.get("discovery", DEFAULT_CRON))


@dag(
    dag_id="lemmy_discovery",
    schedule=_schedule(),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["lemmy", "discovery"],
)
def lemmy_discovery():
    @task
    def list_instances() -> list[str]:
        from pipeline.registry import configured_instances

        return configured_instances(Config.load().sources["lemmy"])

    @task(pool="ingest", execution_timeout=timedelta(minutes=30))
    def probe_and_discover(instance: str) -> dict:
        from pipeline.db import get_db
        from pipeline.discovery import (
            INSTANCES,
            discover_instance,
            ensure_registry_indexes,
            probe_instance,
            upsert_communities,
        )
        from pipeline.sources.lemmy import LemmyClient

        lemmy = Config.load().sources["lemmy"]
        disc = lemmy.get("discovery", {})
        db = get_db()
        ensure_registry_indexes(db)
        now = datetime.now(timezone.utc)

        with LemmyClient(instance, delay=lemmy.get("request_delay_seconds", 1.0), max_retries=2) as client:
            probe = probe_instance(client, instance, now)
            db[INSTANCES].replace_one({"_id": instance}, probe, upsert=True)
            if probe["status"] != "ok":
                return {"instance": instance, "status": probe["status"], "reason": probe["reason"]}

            docs = discover_instance(client, instance, disc, lemmy.get("themes", {}), lemmy.get("exclude_keywords", []))

        new = upsert_communities(db, docs, auto_approve=disc.get("auto_approve", True), now=now)
        return {"instance": instance, "status": "ok", "matched": len(docs), "new": new,
                "communities": [d["_id"] for d in docs]}

    probe_and_discover.expand(instance=list_instances())


lemmy_discovery()
