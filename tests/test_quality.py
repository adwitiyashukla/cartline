from __future__ import annotations

import copy
from datetime import UTC, date, datetime

import pytest

from cartline.config import Config
from cartline.lake import write_table
from cartline.quality import ERROR, report, run_checks
from cartline.schema import EVENT_SCHEMA, SILVER_PARTITIONS

EXPECTED_CHECKS = {
    "bronze_quarantine_rate",
    "silver_rows_present",
    "silver_null_keys",
    "silver_negative_price",
    "silver_unknown_event_type",
    "silver_duplicate_keys",
    "silver_date_continuity",
    "silver_null_brand_rate",
    "silver_null_category_rate",
    "dim_single_current_row",
    "dim_valid_intervals",
    "session_event_reconciliation",
    "funnel_daily_reconciliation",
    "funnel_rates_non_negative",
    "traffic_daily_reconciliation",
    "abandonment_bounds",
}


def test_every_check_passes_on_a_clean_lake(lake, spark):
    checks = run_checks(spark, lake)
    assert {check["check"] for check in checks} == EXPECTED_CHECKS
    failed = [check for check in checks if not check["passed"]]
    assert failed == []


def test_report_is_written_to_disk(lake, spark):
    payload = report(spark, lake, "test-run")
    assert payload["failed"] == 0
    assert (lake.paths.reports / "quality_test-run.json").is_file()
    assert lake.paths.quality.is_dir()


@pytest.fixture(scope="module")
def broken(tmp_path_factory, raw_config, spark) -> Config:
    base = tmp_path_factory.mktemp("broken")
    config = Config.from_dict(copy.deepcopy(raw_config), base)
    stamp = datetime(2019, 10, 1, 9, 0, tzinfo=UTC)
    rows = [
        (stamp, "view", 1, 2, "a.b", "a", "b", None, "lg", 5.0, 7, "s", 9, date(2019, 10, 1)),
        (stamp, "view", 1, 2, "a.b", "a", "b", None, "lg", -5.0, 7, "s", 9, date(2019, 10, 1)),
        (stamp, "browse", 1, 2, "a.b", "a", "b", None, "lg", 5.0, 7, "s", 9, date(2019, 10, 1)),
        (stamp, "view", 1, 2, "a.b", "a", "b", None, "lg", 5.0, None, "s", 9, date(2019, 10, 1)),
    ]
    frame = spark.createDataFrame(rows, EVENT_SCHEMA)
    write_table(frame, config.paths.silver_events, SILVER_PARTITIONS, files=1)
    return config


def test_broken_silver_fails_the_gate(broken, spark):
    failures = {
        check["check"]
        for check in run_checks(spark, broken)
        if not check["passed"] and check["level"] == ERROR
    }
    assert "silver_negative_price" in failures
    assert "silver_unknown_event_type" in failures
    assert "silver_null_keys" in failures
    assert "silver_duplicate_keys" in failures
