"""Turn flat raw posts/comments into structured threads: category > topic > description > nested comments.

TRIGGER: event-driven. It starts by itself whenever any ingest_lemmy__* DAG finishes a task (RAW_ITEMS asset
updated) and announces the THREADS asset when done, which is what Stage 3 (cleaning) will listen to.
You can still trigger it by hand; tick `rebuild` after editing categories.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task

from pipeline.assets import RAW_ITEMS, THREADS


@dag(
    dag_id="lemmy_build_threads",
    schedule=[RAW_ITEMS],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"rebuild": Param(False, type="boolean", description="Wipe and rebuild ALL threads (use after editing categories)")},
    tags=["lemmy", "threads"],
)
def lemmy_build_threads():
    @task(outlets=[THREADS])
    def build() -> dict:
        from pipeline.config import Config
        from pipeline.db import get_db
        from pipeline.ingest import ensure_indexes
        from pipeline.registry import all_categories
        from pipeline.threads import assemble_threads, ensure_thread_indexes

        rebuild = bool(get_current_context()["params"].get("rebuild", False))
        cfg = Config.load()
        db = get_db()
        ensure_indexes(db)
        ensure_thread_indexes(db)
        return assemble_threads(db, all_categories(db, cfg.sources.get("lemmy", {})), rebuild=rebuild)

    build()


lemmy_build_threads()
