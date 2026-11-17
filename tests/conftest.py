from __future__ import annotations

import copy
import tomllib
from pathlib import Path

import pytest

from cartline.config import Config
from cartline.gold import build_gold
from cartline.ingest import ingest
from cartline.session import build_session
from cartline.silver import build_silver

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _raw_config() -> dict:
    with (PROJECT_ROOT / "config.toml").open("rb") as handle:
        return tomllib.load(handle)


@pytest.fixture(scope="session")
def raw_config() -> dict:
    return _raw_config()


@pytest.fixture(scope="session")
def config(tmp_path_factory, raw_config) -> Config:
    base = tmp_path_factory.mktemp("cartline")
    raw = copy.deepcopy(raw_config)
    raw["source"]["months"] = ["2019-Oct"]
    raw["spark"]["master"] = "local[2]"
    raw["spark"]["driver_memory"] = "2g"
    raw["spark"]["shuffle_partitions"] = 4
    raw["gold"]["output_partitions"] = 1
    raw["ingest"]["max_records_per_file"] = 5000
    raw["silver"]["output_partitions"] = 2
    raw["stream"]["max_files_per_trigger"] = 2
    raw["generate"].update(
        {
            "events": 20000,
            "products": 400,
            "users": 900,
            "start_date": "2019-10-01",
            "end_date": "2019-10-06",
            "black_friday": "2019-10-04",
            "output_partitions": 2,
        }
    )
    return Config.from_dict(raw, base)


@pytest.fixture(scope="session")
def spark(config):
    session = build_session(config, "tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def lake(spark, config) -> Config:
    from cartline.generate import generate

    generate(spark, config, force=True)
    ingest(spark, config)
    build_silver(spark, config)
    build_gold(spark, config)
    return config
