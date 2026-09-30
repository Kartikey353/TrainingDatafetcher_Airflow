"""Loads config/sources.yaml (path from CONFIG_PATH env var)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

import yaml


def data_dir() -> Path:
    p = Path(os.environ.get("DATA_DIR", "./data"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def config_path() -> Path:
    return Path(os.environ.get("CONFIG_PATH", "config/sources.yaml"))


@dataclass
class CleaningConfig:
    min_words: int = 30
    max_words: int = 20000
    min_alpha_ratio: float = 0.6
    max_repeat_line_ratio: float = 0.3
    max_top_ngram_char_ratio: float = 0.25
    max_url_ratio: float = 0.2
    languages: list[str] = field(default_factory=lambda: ["en"])
    min_lang_confidence: float = 0.8
    scrub_pii: bool = True
    drop_nsfw: bool = True
    drop_authors: list[str] = field(default_factory=lambda: ["AutoModerator", "[deleted]"])
    near_dup_threshold: float = 0.85
    valid_fraction: float = 0.02


@dataclass
class Config:
    schedules: dict[str, str]
    sources: dict[str, dict[str, Any]]
    cleaning: CleaningConfig

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        raw = yaml.safe_load((path or config_path()).read_text())
        known = {f.name for f in fields(CleaningConfig)}
        unknown = set(raw.get("cleaning", {})) - known
        if unknown:  # fail loudly on typos instead of silently ignoring a setting
            raise ValueError(f"Unknown cleaning settings in config: {sorted(unknown)}")
        return cls(
            schedules=raw.get("schedules", {}),
            sources=raw.get("sources", {}),
            cleaning=CleaningConfig(**raw.get("cleaning", {})),
        )
