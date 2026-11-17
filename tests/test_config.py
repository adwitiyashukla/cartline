from __future__ import annotations

import copy

import pytest

from cartline.config import Config, ConfigError


def test_loads_project_config(raw_config):
    config = Config.from_dict(copy.deepcopy(raw_config), ".")
    assert config.name == "cartline"
    assert config.source.months
    assert config.silver.session_gap_minutes > 0
    assert len(config.fingerprint) == 12


def test_fingerprint_changes_with_values(raw_config):
    first = Config.from_dict(copy.deepcopy(raw_config), ".")
    other = copy.deepcopy(raw_config)
    other["silver"]["session_gap_minutes"] = 45
    assert Config.from_dict(other, ".").fingerprint != first.fingerprint


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [
        ("spark", "driver_memory", "5gb"),
        ("spark", "shuffle_partitions", 0),
        ("spark", "parquet_compression", "brotli"),
        ("silver", "session_gap_minutes", -1),
        ("gold", "rfm_buckets", 1),
        ("ingest", "max_quarantine_rate", 1.5),
        ("stream", "trigger", "continuous"),
        ("generate", "black_friday", "2020-01-01"),
        ("generate", "events", 0),
    ],
)
def test_rejects_bad_values(raw_config, section, key, value):
    broken = copy.deepcopy(raw_config)
    broken[section][key] = value
    with pytest.raises(ConfigError):
        Config.from_dict(broken, ".")


def test_rejects_missing_section(raw_config):
    broken = copy.deepcopy(raw_config)
    del broken["quality"]
    with pytest.raises(ConfigError):
        Config.from_dict(broken, ".")


def test_file_pattern_requires_month(raw_config):
    broken = copy.deepcopy(raw_config)
    broken["source"]["file_pattern"] = "events.csv.gz"
    with pytest.raises(ConfigError):
        Config.from_dict(broken, ".")
