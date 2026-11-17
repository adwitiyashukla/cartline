from __future__ import annotations

from datetime import UTC, date, datetime

from pyspark.sql import functions as F

from cartline.gold import SESSION_TABLE, session_scope, sessionize, sessions_table
from cartline.lake import read_table
from cartline.schema import EVENT_SCHEMA
from cartline.silver import silver_frame


def _utc(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp).replace(tzinfo=UTC)


def _events(spark, stamps, user=1):
    rows = [
        (
            _utc(stamp),
            "view",
            10,
            1,
            "electronics.smartphone",
            "electronics",
            "smartphone",
            None,
            "samsung",
            10.0,
            user,
            "s",
            _utc(stamp).hour,
            _utc(stamp).date(),
        )
        for stamp in stamps
    ]
    return spark.createDataFrame(rows, EVENT_SCHEMA)


def test_gap_starts_a_new_session(spark, config):
    frame = _events(
        spark,
        [
            "2019-10-01T10:00:00",
            "2019-10-01T10:20:00",
            "2019-10-01T11:30:00",
        ],
    )
    indexes = [row["session_index"] for row in sessionize(config, frame).collect()]
    assert sorted(indexes) == [1, 1, 2]


def test_session_crossing_midnight_stays_one_session(spark, config):
    frame = _events(spark, ["2019-10-01T23:50:00", "2019-10-02T00:05:00"])
    sessions = sessions_table(config, frame).collect()
    assert len(sessions) == 1
    assert sessions[0]["events"] == 2
    assert sessions[0]["event_date"] == date(2019, 10, 1)


def test_session_scope_widens_by_one_day():
    scope = session_scope((date(2019, 10, 5),))
    assert scope == (date(2019, 10, 4), date(2019, 10, 5), date(2019, 10, 6))
    assert session_scope(()) == ()


def test_windowed_rebuild_matches_full_build(lake, spark):
    stored = read_table(spark, lake.paths.gold_table(SESSION_TABLE))
    target = stored.select("event_date").orderBy("event_date").collect()[2]["event_date"]
    expected = stored.filter(F.col("event_date") == F.lit(target))
    rebuilt = sessions_table(lake, silver_frame(spark, lake, session_scope((target,)))).filter(
        F.col("event_date") == F.lit(target)
    )
    assert rebuilt.count() == expected.count()
    assert rebuilt.agg(F.sum("events")).first()[0] == expected.agg(F.sum("events")).first()[0]
    assert {row["session_id"] for row in rebuilt.collect()} == {
        row["session_id"] for row in expected.collect()
    }


def test_sessions_cover_every_silver_event(lake, spark):
    stored = read_table(spark, lake.paths.gold_table(SESSION_TABLE))
    assert stored.agg(F.sum("events")).first()[0] == silver_frame(spark, lake).count()
