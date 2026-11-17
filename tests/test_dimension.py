from __future__ import annotations

from datetime import date

from pyspark.sql import functions as F

from cartline.dimension import DIMENSION_TABLE, daily_attributes, scd2
from cartline.lake import read_table
from cartline.silver import silver_frame

ATTRIBUTES = ("brand", "price")


def _snapshots(spark, rows):
    return spark.createDataFrame(
        rows, "product_id long, event_date date, events long, brand string, price double"
    )


def test_scd2_closes_previous_version(spark):
    frame = _snapshots(
        spark,
        [
            (1, date(2019, 10, 1), 3, "samsung", 100.0),
            (1, date(2019, 10, 2), 2, "samsung", 100.0),
            (1, date(2019, 10, 3), 4, "samsung", 120.0),
        ],
    )
    versions = {row["version"]: row for row in scd2(frame, ATTRIBUTES).collect()}
    assert len(versions) == 2
    assert versions[1]["valid_from"] == date(2019, 10, 1)
    assert versions[1]["valid_to"] == date(2019, 10, 2)
    assert versions[1]["is_current"] is False
    assert versions[1]["events"] == 5
    assert versions[2]["valid_from"] == date(2019, 10, 3)
    assert versions[2]["valid_to"] is None
    assert versions[2]["is_current"] is True


def test_scd2_treats_null_as_a_value(spark):
    frame = _snapshots(
        spark,
        [
            (2, date(2019, 10, 1), 1, None, 10.0),
            (2, date(2019, 10, 2), 1, None, 10.0),
            (2, date(2019, 10, 3), 1, "lg", 10.0),
        ],
    )
    assert scd2(frame, ATTRIBUTES).count() == 2


def test_daily_attributes_take_the_first_event_of_the_day(lake, spark):
    events = silver_frame(spark, lake)
    snapshots = daily_attributes(events, lake.dimension.attributes)
    assert snapshots.count() > 0
    assert (
        snapshots.groupBy("product_id", "event_date").count().filter(F.col("count") > 1).count()
        == 0
    )
    assert snapshots.agg(F.sum("events")).first()[0] == events.count()


def test_dimension_has_one_current_row_per_product(lake, spark):
    dimension = read_table(spark, lake.paths.gold_table(DIMENSION_TABLE))
    current = dimension.filter(F.col("is_current"))
    assert current.count() == dimension.select("product_id").distinct().count()
    assert dimension.filter(F.col("valid_to") < F.col("valid_from")).count() == 0
    assert dimension.filter(~F.col("is_current") & F.col("valid_to").isNull()).count() == 0
