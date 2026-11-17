from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cartline.audit import new_run_id, record_run, started
from cartline.config import Config
from cartline.dimension import build_dimension
from cartline.lake import read_table, write_table
from cartline.schema import CART, PURCHASE, REMOVE, VIEW
from cartline.silver import silver_frame

SESSION_TABLE = "fct_session"
FUNNEL_TABLE = "fct_funnel_daily"
PRODUCT_TABLE = "fct_product_daily"
RFM_TABLE = "agg_user_rfm"
TRAFFIC_TABLE = "agg_hourly_traffic"
ABANDONMENT_TABLE = "agg_cart_abandonment"
ABANDONED_PRODUCTS_TABLE = "agg_abandoned_products"
UNKNOWN = "unknown"


def _count_of(event_type: str) -> F.Column:
    return F.sum(F.when(F.col("event_type") == F.lit(event_type), F.lit(1)).otherwise(F.lit(0)))


def _revenue() -> F.Column:
    return F.sum(
        F.when(F.col("event_type") == F.lit(PURCHASE), F.col("price")).otherwise(F.lit(0.0))
    )


def _ratio(numerator: str, denominator: str) -> F.Column:
    return F.round(
        F.when(F.col(denominator) == F.lit(0), F.lit(None).cast("double")).otherwise(
            F.col(numerator) / F.col(denominator)
        ),
        6,
    )


def session_scope(dates: tuple[date, ...]) -> tuple[date, ...]:
    if not dates:
        return ()
    widened = set()
    for day in dates:
        widened.update({day - timedelta(days=1), day, day + timedelta(days=1)})
    return tuple(sorted(widened))


def sessionize(config: Config, events: DataFrame) -> DataFrame:
    ordering = Window.partitionBy("user_id").orderBy("event_time")
    running = ordering.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous = F.lag("event_time").over(ordering)
    gap = config.silver.session_gap_minutes * 60
    boundary = F.when(previous.isNull(), F.lit(1)).otherwise(
        (F.unix_timestamp("event_time") - F.unix_timestamp(previous) > F.lit(gap)).cast("int")
    )
    return events.withColumn("session_index", F.sum(boundary).over(running))


def sessions_table(config: Config, events: DataFrame) -> DataFrame:
    grouped = (
        sessionize(config, events)
        .groupBy("user_id", "session_index")
        .agg(
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
            F.count(F.lit(1)).alias("events"),
            _count_of(VIEW).alias("views"),
            _count_of(CART).alias("carts"),
            _count_of(REMOVE).alias("removes"),
            _count_of(PURCHASE).alias("purchases"),
            F.round(_revenue(), 2).alias("revenue"),
            F.size(F.collect_set("product_id")).alias("products"),
            F.size(F.collect_set("user_session")).alias("source_sessions"),
        )
    )
    return grouped.select(
        F.sha2(F.concat_ws("|", F.col("user_id"), F.col("session_start")), 256)
        .substr(1, 16)
        .alias("session_id"),
        F.col("user_id"),
        F.col("session_start"),
        F.col("session_end"),
        F.col("events"),
        F.col("views"),
        F.col("carts"),
        F.col("removes"),
        F.col("purchases"),
        F.col("revenue"),
        F.col("products"),
        F.col("source_sessions"),
        (F.col("purchases") > F.lit(0)).alias("converted"),
        (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).alias(
            "duration_seconds"
        ),
        F.to_date(F.col("session_start")).alias("event_date"),
    )


def funnel_table(events: DataFrame) -> DataFrame:
    grouped = events.groupBy(
        "event_date",
        F.coalesce(F.col("category_l1"), F.lit(UNKNOWN)).alias("category_l1"),
        F.coalesce(F.col("brand"), F.lit(UNKNOWN)).alias("brand"),
    ).agg(
        _count_of(VIEW).alias("views"),
        _count_of(CART).alias("carts"),
        _count_of(REMOVE).alias("removes"),
        _count_of(PURCHASE).alias("purchases"),
        F.round(_revenue(), 2).alias("revenue"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("product_id").alias("products"),
    )
    return grouped.select(
        "category_l1",
        "brand",
        "views",
        "carts",
        "removes",
        "purchases",
        "revenue",
        "users",
        "products",
        _ratio("carts", "views").alias("view_to_cart"),
        _ratio("purchases", "carts").alias("cart_to_purchase"),
        _ratio("purchases", "views").alias("view_to_purchase"),
        "event_date",
    )


def product_table(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("event_date", "product_id")
        .agg(
            _count_of(VIEW).alias("views"),
            _count_of(CART).alias("carts"),
            _count_of(PURCHASE).alias("purchases"),
            F.round(_revenue(), 2).alias("revenue"),
            F.countDistinct("user_id").alias("users"),
            F.round(F.avg("price"), 2).alias("average_price"),
        )
        .select(
            "product_id",
            "views",
            "carts",
            "purchases",
            "revenue",
            "users",
            "average_price",
            _ratio("purchases", "views").alias("view_to_purchase"),
            "event_date",
        )
    )


def traffic_table(events: DataFrame) -> DataFrame:
    return events.groupBy("event_date", "event_hour").agg(
        F.count(F.lit(1)).alias("events"),
        _count_of(VIEW).alias("views"),
        _count_of(CART).alias("carts"),
        _count_of(PURCHASE).alias("purchases"),
        F.round(_revenue(), 2).alias("revenue"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("user_session").alias("sessions"),
    )


def rfm_table(config: Config, events: DataFrame) -> DataFrame:
    purchases = events.filter(F.col("event_type") == F.lit(PURCHASE))
    last_day = events.select(F.max("event_date")).first()[0]
    per_user = purchases.groupBy("user_id").agg(
        F.max("event_date").alias("last_purchase"),
        F.count(F.lit(1)).alias("frequency"),
        F.round(F.sum("price"), 2).alias("monetary"),
    )
    buckets = config.gold.rfm_buckets
    recency = F.datediff(F.lit(last_day), F.col("last_purchase"))
    scored = (
        per_user.withColumn("recency_days", recency)
        .withColumn(
            "recency_score",
            F.lit(buckets + 1) - F.ntile(buckets).over(Window.orderBy(F.col("recency_days"))),
        )
        .withColumn("frequency_score", F.least(F.lit(buckets), F.col("frequency")).cast("int"))
        .withColumn("monetary_score", F.ntile(buckets).over(Window.orderBy(F.col("monetary"))))
    )
    top = F.lit(buckets)
    segment = (
        F.when(
            (F.col("recency_score") >= top - 1) & (F.col("frequency_score") >= top - 1),
            F.lit("champion"),
        )
        .when(F.col("recency_score") >= top - 1, F.lit("recent"))
        .when(F.col("frequency_score") >= top - 1, F.lit("loyal"))
        .when(F.col("recency_score") <= F.lit(2), F.lit("lapsed"))
        .otherwise(F.lit("regular"))
    )
    return scored.withColumn("segment", segment).withColumn("as_of", F.lit(last_day))


def abandonment_table(sessions: DataFrame) -> DataFrame:
    carted = F.col("carts") > F.lit(0)
    return (
        sessions.groupBy("event_date")
        .agg(
            F.count(F.lit(1)).alias("sessions"),
            F.sum(F.when(carted, F.lit(1)).otherwise(F.lit(0))).alias("sessions_with_cart"),
            F.sum(F.when(F.col("converted"), F.lit(1)).otherwise(F.lit(0))).alias(
                "sessions_with_purchase"
            ),
            F.sum(F.when(carted & F.col("converted"), F.lit(1)).otherwise(F.lit(0))).alias(
                "carted_and_bought"
            ),
            F.sum(F.when(~carted & F.col("converted"), F.lit(1)).otherwise(F.lit(0))).alias(
                "direct_buys"
            ),
            F.round(F.avg("events"), 3).alias("events_per_session"),
            F.round(F.avg("duration_seconds"), 1).alias("seconds_per_session"),
            F.round(F.sum("revenue"), 2).alias("revenue"),
        )
        .select(
            "event_date",
            "sessions",
            "sessions_with_cart",
            "sessions_with_purchase",
            "carted_and_bought",
            "direct_buys",
            _ratio("sessions_with_cart", "sessions").alias("cart_rate"),
            _ratio("carted_and_bought", "sessions_with_cart").alias("cart_conversion"),
            _ratio("direct_buys", "sessions_with_purchase").alias("direct_buy_share"),
            "events_per_session",
            "seconds_per_session",
            "revenue",
        )
    )


def abandoned_products_table(config: Config, events: DataFrame) -> DataFrame:
    carted = events.filter(F.col("event_type").isin([CART, PURCHASE])).filter(
        F.col("user_session").isNotNull()
    )
    per_basket = carted.groupBy("user_session", "product_id").agg(
        F.max(F.when(F.col("event_type") == F.lit(CART), F.lit(1)).otherwise(F.lit(0))).alias(
            "carted"
        ),
        F.max(F.when(F.col("event_type") == F.lit(PURCHASE), F.lit(1)).otherwise(F.lit(0))).alias(
            "bought"
        ),
    )
    return (
        per_basket.filter(F.col("carted") == F.lit(1))
        .groupBy("product_id")
        .agg(
            F.count(F.lit(1)).alias("carted_baskets"),
            F.sum("bought").alias("bought_baskets"),
        )
        .withColumn("abandoned_baskets", F.col("carted_baskets") - F.col("bought_baskets"))
        .withColumn("abandon_rate", _ratio("abandoned_baskets", "carted_baskets"))
        .orderBy(F.desc("abandoned_baskets"))
        .limit(config.gold.top_products)
    )


def build_gold(
    spark: SparkSession, config: Config, dates: tuple[date, ...] = (), run_id: str = ""
) -> list[dict]:
    identifier = run_id or new_run_id()
    results = []
    if not dates:
        results.append(build_dimension(spark, config, identifier))
    events = silver_frame(spark, config, dates)
    around = silver_frame(spark, config, session_scope(dates))
    sessions_frame = sessions_table(config, around)
    if dates:
        sessions_frame = sessions_frame.filter(F.col("event_date").isin(list(dates)))
    daily = (
        (SESSION_TABLE, sessions_frame),
        (FUNNEL_TABLE, funnel_table(events)),
        (PRODUCT_TABLE, product_table(events)),
        (TRAFFIC_TABLE, traffic_table(events)),
    )
    for name, frame in daily:
        begin = started()
        write_table(
            frame,
            config.paths.gold_table(name),
            ("event_date",),
            files=config.gold.output_partitions,
            spread=("event_date",),
        )
        rows = read_table(spark, config.paths.gold_table(name)).count()
        results.append(record_run(spark, config, identifier, "gold", name, begin, rows_out=rows))
    if dates:
        return results
    sessions = read_table(spark, config.paths.gold_table(SESSION_TABLE))
    wide = (
        (RFM_TABLE, rfm_table(config, silver_frame(spark, config))),
        (ABANDONMENT_TABLE, abandonment_table(sessions)),
        (ABANDONED_PRODUCTS_TABLE, abandoned_products_table(config, silver_frame(spark, config))),
    )
    for name, frame in wide:
        begin = started()
        write_table(frame, config.paths.gold_table(name), files=config.gold.output_partitions)
        rows = read_table(spark, config.paths.gold_table(name)).count()
        results.append(record_run(spark, config, identifier, "gold", name, begin, rows_out=rows))
    return results
