from __future__ import annotations

import pytest

from cartline.lake import read_table
from cartline.stream import run_stream

HEADER = (
    "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session"
)
ROWS = (
    "2019-10-01 10:00:00 UTC,view,1000001,205301,electronics.smartphone,samsung,100.00,7001,s-1",
    "2019-10-01 10:00:30 UTC,view,1000002,205301,electronics.smartphone,samsung,120.00,7001,s-1",
    "2019-10-01 10:01:00 UTC,cart,1000002,205301,electronics.smartphone,samsung,120.00,7001,s-1",
    "2019-10-01 10:01:00 UTC,cart,1000002,205301,electronics.smartphone,samsung,120.00,7001,s-1",
    "not a timestamp,view,1000003,205301,electronics.smartphone,samsung,90.00,7002,s-2",
)


@pytest.fixture(scope="module")
def streamed(lake, spark):
    lake.paths.landing.mkdir(parents=True, exist_ok=True)
    (lake.paths.landing / "batch_01.csv").write_text("\n".join((HEADER,) + ROWS[:3]) + "\n")
    (lake.paths.landing / "batch_02.csv").write_text("\n".join((HEADER,) + ROWS[3:]) + "\n")
    run_stream(spark, lake)
    return lake


def test_stream_writes_valid_events(streamed, spark):
    events = read_table(spark, streamed.paths.stream_events)
    assert events.count() == 3
    assert events.select("user_id").distinct().count() == 1


def test_stream_drops_duplicates_within_the_watermark(streamed, spark):
    events = read_table(spark, streamed.paths.stream_events)
    carts = events.filter(events.event_type == "cart")
    assert carts.count() == 1


def test_stream_sends_broken_rows_to_quarantine(streamed, spark):
    rejected = read_table(spark, streamed.paths.bronze_quarantine).filter("source_month = 'stream'")
    assert rejected.count() == 1
    assert rejected.first()["reject_reason"] == "bad_event_time"


def test_stream_refreshes_the_traffic_table(streamed, spark):
    traffic = read_table(spark, streamed.paths.stream_traffic)
    assert traffic.count() == 1
    assert traffic.first()["events"] == 3


def test_restarting_the_stream_reprocesses_nothing(streamed, spark):
    before = read_table(spark, streamed.paths.stream_events).count()
    run_stream(spark, streamed)
    assert read_table(spark, streamed.paths.stream_events).count() == before
