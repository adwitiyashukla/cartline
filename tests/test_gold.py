from __future__ import annotations

from pyspark.sql import functions as F

from cartline.gold import (
    ABANDONED_PRODUCTS_TABLE,
    ABANDONMENT_TABLE,
    FUNNEL_TABLE,
    PRODUCT_TABLE,
    RFM_TABLE,
    TRAFFIC_TABLE,
)
from cartline.lake import read_table
from cartline.silver import silver_frame

SEGMENTS = {"champion", "recent", "loyal", "lapsed", "regular"}


def test_funnel_reconciles_with_silver(lake, spark):
    funnel = read_table(spark, lake.paths.gold_table(FUNNEL_TABLE))
    counted = funnel.agg(
        F.sum(F.col("views") + F.col("carts") + F.col("removes") + F.col("purchases"))
    ).first()[0]
    assert counted == silver_frame(spark, lake).count()


def test_funnel_rates_are_fractions(lake, spark):
    funnel = read_table(spark, lake.paths.gold_table(FUNNEL_TABLE))
    for column in ("view_to_cart", "cart_to_purchase", "view_to_purchase"):
        outside = funnel.filter(
            F.col(column).isNotNull() & ((F.col(column) < 0) | (F.col(column) > 1))
        )
        assert outside.count() == 0


def test_traffic_hours_are_valid(lake, spark):
    traffic = read_table(spark, lake.paths.gold_table(TRAFFIC_TABLE))
    assert traffic.filter((F.col("event_hour") < 0) | (F.col("event_hour") > 23)).count() == 0
    assert traffic.agg(F.sum("events")).first()[0] == silver_frame(spark, lake).count()


def test_product_daily_counts_match_silver(lake, spark):
    products = read_table(spark, lake.paths.gold_table(PRODUCT_TABLE))
    events = silver_frame(spark, lake)
    assert (
        products.agg(F.sum("views")).first()[0]
        == events.filter(F.col("event_type") == "view").count()
    )
    assert products.filter(F.col("revenue") < 0).count() == 0


def test_rfm_scores_stay_in_range(lake, spark):
    rfm = read_table(spark, lake.paths.gold_table(RFM_TABLE))
    buckets = lake.gold.rfm_buckets
    for column in ("recency_score", "frequency_score", "monetary_score"):
        assert rfm.filter((F.col(column) < 1) | (F.col(column) > buckets)).count() == 0
    assert {row["segment"] for row in rfm.select("segment").distinct().collect()} <= SEGMENTS
    assert rfm.select("user_id").distinct().count() == rfm.count()


def test_abandonment_is_bounded(lake, spark):
    abandonment = read_table(spark, lake.paths.gold_table(ABANDONMENT_TABLE))
    assert abandonment.filter(F.col("sessions_with_cart") > F.col("sessions")).count() == 0
    assert abandonment.filter(F.col("carted_and_bought") > F.col("sessions_with_cart")).count() == 0
    split = F.col("carted_and_bought") + F.col("direct_buys")
    assert abandonment.filter(split != F.col("sessions_with_purchase")).count() == 0
    products = read_table(spark, lake.paths.gold_table(ABANDONED_PRODUCTS_TABLE))
    assert products.count() <= lake.gold.top_products
    assert products.filter(F.col("abandoned_baskets") > F.col("carted_baskets")).count() == 0
