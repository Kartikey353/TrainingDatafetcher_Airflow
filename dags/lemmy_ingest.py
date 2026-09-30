"""One ingest DAG per Lemmy instance:  ingest_lemmy__<instance>

Why one DAG per instance instead of one big DAG:
  * each instance has its own schedule (staggered, so 12 instances don't hit the machine at once)
  * a slow, down or login-walled instance only fails its own DAG
  * you can pause or trigger a single instance from the UI

Each successful `ingest_one` announces the RAW_ITEMS asset, which automatically starts
lemmy_build_threads (see dags/lemmy_build_threads.py).
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from pipeline.assets import RAW_ITEMS
from pipeline.config import Config


def slug(instance: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", instance.lower()).strip("_")


def cron_for(index: int, every_minutes: int) -> str:
    """Stagger instances: instance i starts 7 minutes after instance i-1 (wrapping inside the interval)."""
    if every_minutes >= 60:
        return f"{(index * 7) % 60} */{max(1, every_minutes // 60)} * * *"
    minute = (index * 7) % every_minutes
    return f"{minute}-59/{every_minutes} * * * *"


def make_dag(instance: str, index: int, every_minutes: int):
    @dag(
        dag_id=f"ingest_lemmy__{slug(instance)}",
        schedule=cron_for(index, every_minutes),
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,  # a long first backfill must not overlap with the next scheduled run
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=["ingest", "lemmy", instance],
    )
    def ingest_instance():
        @task
        def list_communities() -> list[dict]:
            from pipeline.db import get_db
            from pipeline.registry import communities_for_instance

            return communities_for_instance(get_db(), Config.load().sources["lemmy"], instance)

        @task(pool="ingest", execution_timeout=timedelta(hours=3), max_active_tis_per_dag=2, outlets=[RAW_ITEMS])
        def ingest_one(community: dict) -> dict:
            from pipeline.db import get_db
            from pipeline.ingest import ensure_indexes, ingest_community
            from pipeline.sources.lemmy import LemmyClient

            cfg = Config.load()
            lemmy_cfg = cfg.sources["lemmy"]
            db = get_db()
            ensure_indexes(db)
            with LemmyClient(instance, delay=lemmy_cfg.get("request_delay_seconds", 1.0)) as client:
                return ingest_community(
                    client=client,
                    db=db,
                    instance=instance,
                    name=community["name"],
                    cfg=lemmy_cfg,
                    drop_nsfw=cfg.cleaning.drop_nsfw,
                    drop_authors=cfg.cleaning.drop_authors,
                )

        ingest_one.expand(community=list_communities())

    return ingest_instance()


def _build_all() -> None:
    from pipeline.registry import configured_instances

    cfg = Config.load()
    lemmy = cfg.sources.get("lemmy", {})
    if not lemmy.get("enabled"):
        return
    every = int(cfg.schedules.get("ingest_every_minutes", 30))
    for i, instance in enumerate(configured_instances(lemmy)):
        globals()[f"dag_{slug(instance)}"] = make_dag(instance, i, every)  # Airflow finds DAGs in module globals


_build_all()
