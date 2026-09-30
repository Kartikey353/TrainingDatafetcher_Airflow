import mongomock
import pytest

from pipeline.ingest import ensure_indexes
from pipeline.threads import ensure_thread_indexes


@pytest.fixture
def db(monkeypatch):
    # mongomock 4.3 can't run the newest pymongo's ReplaceOne through bulk_write (it rejects a `sort`
    # argument). Real MongoDB is fine, so in tests we apply the same operations one by one, in order.
    def bulk_write(self, ops, ordered=True):
        for op in ops:
            self.replace_one(op._filter, op._doc, upsert=op._upsert)

    monkeypatch.setattr(mongomock.collection.Collection, "bulk_write", bulk_write)
    d = mongomock.MongoClient().db
    ensure_indexes(d)
    ensure_thread_indexes(d)
    return d
