from __future__ import annotations

from cartline.schema import SOURCE_SCHEMA
from cartline.transform import deduplicate, prepare, split_valid

GOOD = (
    "2019-10-01 00:00:01 UTC",
    "view",
    "1000001",
    "2053013555631882655",
    "electronics.smartphone.android",
    "Samsung",
    "489.07",
    "520088904",
    "9333dfbd-b87a-4708-9857-6336556b0fcc",
)


def _rows(spark, rows):
    return spark.createDataFrame(list(rows), SOURCE_SCHEMA)


def test_parses_and_normalises(spark):
    valid, rejected = split_valid(prepare(_rows(spark, [GOOD])))
    assert rejected.count() == 0
    row = valid.first()
    assert row["product_id"] == 1000001
    assert row["price"] == 489.07
    assert row["brand"] == "samsung"
    assert row["category_l1"] == "electronics"
    assert row["category_l2"] == "smartphone"
    assert row["category_l3"] == "android"
    assert str(row["event_date"]) == "2019-10-01"
    assert row["event_hour"] == 0


def test_blank_fields_become_null(spark):
    blank = list(GOOD)
    blank[4] = ""
    blank[5] = ""
    blank[8] = ""
    row = split_valid(prepare(_rows(spark, [blank])))[0].first()
    assert row["category_code"] is None
    assert row["brand"] is None
    assert row["user_session"] is None
    assert row["category_l1"] is None


def test_each_reject_reason(spark):
    cases = {
        "bad_event_time": (0, "yesterday"),
        "unknown_event_type": (1, "wishlist"),
        "bad_product_id": (2, "abc"),
        "bad_user_id": (7, ""),
        "bad_price": (6, "-1.00"),
    }
    rows = []
    for index, value in cases.values():
        broken = list(GOOD)
        broken[index] = value
        rows.append(broken)
    _, rejected = split_valid(prepare(_rows(spark, rows)))
    assert rejected.count() == len(cases)
    assert {row["reject_reason"] for row in rejected.collect()} == set(cases)


def test_deduplicate_keeps_one_row(spark, config):
    valid = split_valid(prepare(_rows(spark, [GOOD, GOOD, GOOD])))[0]
    assert valid.count() == 3
    assert deduplicate(valid, config.silver.dedup_keys).count() == 1
