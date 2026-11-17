from __future__ import annotations

from pyspark.sql import functions as F

from cartline.ingest import ingest, quarantine_rate
from cartline.lake import read_table


def test_bronze_is_written_with_partitions(lake, spark):
    bronze = read_table(spark, lake.paths.bronze_events)
    assert bronze.count() > 0
    assert set(bronze.columns) >= {"event_time", "product_id", "event_date", "source_month"}
    assert bronze.select("source_month").distinct().count() == len(lake.source.months)
    assert bronze.filter(F.col("event_date").isNull()).count() == 0


def test_quarantine_holds_broken_rows(lake, spark):
    rejected = read_table(spark, lake.paths.bronze_quarantine)
    assert rejected.count() > 0
    assert rejected.filter(F.col("reject_reason").isNull()).count() == 0
    assert quarantine_rate(spark, lake) < lake.ingest.max_quarantine_rate


def test_ingest_is_idempotent(lake, spark):
    bronze = read_table(spark, lake.paths.bronze_events)
    before = bronze.count()
    files_before = sorted(path.name for path in lake.paths.bronze_events.rglob("*.parquet"))
    ingest(spark, lake)
    after = read_table(spark, lake.paths.bronze_events).count()
    assert after == before
    assert len(sorted(path.name for path in lake.paths.bronze_events.rglob("*.parquet"))) == len(
        files_before
    )


def test_unknown_month_is_rejected(lake, spark):
    try:
        ingest(spark, lake, ("2020-Jan",))
    except ValueError as error:
        assert "2020-Jan" in str(error)
    else:
        raise AssertionError("expected a ValueError for an unlisted month")
