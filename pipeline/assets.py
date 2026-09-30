"""Airflow Assets = the "wires" between stages. A task that lists an asset in `outlets` announces
"this data changed"; any DAG whose `schedule` lists that asset starts automatically.

    ingest_lemmy__<instance>  --RAW_ITEMS-->  lemmy_build_threads  --THREADS-->  (Stage 3: clean -> export)
"""
from airflow.sdk import Asset

RAW_ITEMS = Asset(name="raw_items", uri="mongo://lemmy_pipeline/raw_items")
THREADS = Asset(name="threads", uri="mongo://lemmy_pipeline/threads")
