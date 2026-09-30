"""Which communities should be ingested? Hand-picked ones from the config plus approved discoveries."""
from __future__ import annotations

from typing import Any

from pymongo.database import Database

from .discovery import COMMUNITIES, INSTANCES
from .threads import UNCATEGORIZED


def norm_host(host: str) -> str:
    return host.strip().lower().removeprefix("https://").removeprefix("http://").rstrip("/")


def configured_instances(cfg: dict[str, Any]) -> list[str]:
    """Instances from config, plus any instance that has a hand-picked community. Order kept, no duplicates."""
    hosts = list(cfg.get("instances", [])) + [c["instance"] for c in cfg.get("communities", [])]
    seen: list[str] = []
    for h in map(norm_host, hosts):
        if h and h not in seen:
            seen.append(h)
    return seen


def _pinned(cfg: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"instance": norm_host(c["instance"]), "name": c["name"], "category": c.get("category", UNCATEGORIZED)}
        for c in cfg.get("communities", [])
    ]


def _approved(db: Database) -> list[dict[str, str]]:
    return [
        {"instance": d["instance"], "name": d["name"], "category": d.get("category", UNCATEGORIZED)}
        for d in db[COMMUNITIES].find({"source": "lemmy", "status": "approved"})
    ]


def all_communities(db: Database, cfg: dict[str, Any]) -> list[dict[str, str]]:
    merged: dict[tuple[str, str], dict[str, str]] = {}
    for c in _approved(db) + _pinned(cfg):  # pinned last, so the config wins
        merged[(c["instance"], c["name"])] = c
    return list(merged.values())


def communities_for_instance(db: Database, cfg: dict[str, Any], instance: str) -> list[dict[str, str]]:
    instance = norm_host(instance)
    probe = db[INSTANCES].find_one({"_id": instance})
    if probe and probe.get("status") == "unsupported":
        return []  # probed and not a usable Lemmy API: don't waste runs on it
    return [c for c in all_communities(db, cfg) if c["instance"] == instance]


def all_categories(db: Database, cfg: dict[str, Any]) -> dict[str, str]:
    """'cooking@lemmy.world' -> 'Food', for every community we ingest."""
    return {f"{c['name']}@{c['instance']}": c["category"] for c in all_communities(db, cfg)}
