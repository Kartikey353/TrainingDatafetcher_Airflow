from pathlib import Path

import pytest

from pipeline.config import Config

CONFIG = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"


def test_config_loads_and_lists_communities():
    cfg = Config.load(CONFIG)
    communities = cfg.sources["lemmy"]["communities"]
    assert len(communities) == 5
    for c in communities:
        assert c["instance"] and c["name"]


def test_cleaning_defaults_sane():
    cfg = Config.load(CONFIG)
    assert cfg.cleaning.min_words > 0
    assert "en" in cfg.cleaning.languages


def test_unknown_cleaning_key_fails(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("sources: {}\ncleaning:\n  min_wordz: 5\n")
    with pytest.raises(ValueError):
        Config.load(bad)


def test_every_community_has_a_category():
    cfg = Config.load(CONFIG)
    for c in cfg.sources["lemmy"]["communities"]:
        assert c.get("category"), f"{c['name']} has no category"


def test_multi_instance_and_discovery_settings():
    from pipeline.registry import configured_instances

    lemmy = Config.load(CONFIG).sources["lemmy"]
    instances = configured_instances(lemmy)
    assert len(instances) == len(set(instances)) >= 12
    assert {"lemmy.blahaj.zone", "hexbear.net", "lemmy.vg", "lazysoci.al"} <= set(instances)
    assert lemmy["themes"] and lemmy["discovery"]["max_per_instance"] > 0
