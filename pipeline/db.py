"""MongoDB access. Connection settings come from env vars set in docker-compose.yml."""
from __future__ import annotations

import os

from pymongo import MongoClient
from pymongo.database import Database


def get_db() -> Database:
    client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=5000)
    return client[os.environ.get("MONGO_DB", "lemmy_pipeline")]
