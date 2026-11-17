from __future__ import annotations

from pyspark.sql import types as T

SOURCE_COLUMNS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)

SOURCE_SCHEMA = T.StructType([T.StructField(name, T.StringType(), True) for name in SOURCE_COLUMNS])

EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")

VIEW = "view"
CART = "cart"
REMOVE = "remove_from_cart"
PURCHASE = "purchase"

SOURCE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
SOURCE_TIME_SUFFIX = " UTC"

REJECT_RULES = (
    ("bad_event_time", "event_time IS NULL"),
    ("unknown_event_type", f"event_type NOT IN ({', '.join(repr(e) for e in EVENT_TYPES)})"),
    ("bad_product_id", "product_id IS NULL"),
    ("bad_user_id", "user_id IS NULL"),
    ("bad_price", "price IS NULL OR price < 0"),
)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_time", T.TimestampType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("product_id", T.LongType(), True),
        T.StructField("category_id", T.LongType(), True),
        T.StructField("category_code", T.StringType(), True),
        T.StructField("category_l1", T.StringType(), True),
        T.StructField("category_l2", T.StringType(), True),
        T.StructField("category_l3", T.StringType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("user_id", T.LongType(), True),
        T.StructField("user_session", T.StringType(), True),
        T.StructField("event_hour", T.IntegerType(), True),
        T.StructField("event_date", T.DateType(), True),
    ]
)

QUARANTINE_SCHEMA = T.StructType(
    [T.StructField(name, T.StringType(), True) for name in SOURCE_COLUMNS]
    + [
        T.StructField("reject_reason", T.StringType(), True),
        T.StructField("source_month", T.StringType(), True),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("ingest_date", T.DateType(), True),
    ]
)

AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("job", T.StringType(), False),
        T.StructField("target", T.StringType(), True),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("finished_at", T.TimestampType(), False),
        T.StructField("seconds", T.DoubleType(), False),
        T.StructField("rows_in", T.LongType(), True),
        T.StructField("rows_out", T.LongType(), True),
        T.StructField("rows_rejected", T.LongType(), True),
        T.StructField("rows_deduplicated", T.LongType(), True),
        T.StructField("partitions_written", T.IntegerType(), True),
        T.StructField("config_fingerprint", T.StringType(), True),
    ]
)

SILVER_PARTITIONS = ("event_date",)

BRONZE_COLUMNS = tuple(field.name for field in EVENT_SCHEMA.fields) + ("source_month",)
