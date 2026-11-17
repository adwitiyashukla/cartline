from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from cartline.schema import (
    EVENT_SCHEMA,
    REJECT_RULES,
    SOURCE_COLUMNS,
    SOURCE_TIME_FORMAT,
    SOURCE_TIME_SUFFIX,
)

EVENT_COLUMNS = tuple(field.name for field in EVENT_SCHEMA.fields)


def _blank_to_null(name: str) -> Column:
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == F.lit(""), F.lit(None).cast("string")).otherwise(trimmed)


def parse_source(frame: DataFrame) -> DataFrame:
    raw = F.struct(*[F.col(name).alias(name) for name in SOURCE_COLUMNS]).alias("_raw")
    stamp = F.regexp_replace(F.trim(F.col("event_time")), f"{SOURCE_TIME_SUFFIX}$", "")
    return frame.select(
        raw,
        F.try_to_timestamp(stamp, F.lit(SOURCE_TIME_FORMAT)).alias("event_time"),
        F.lower(_blank_to_null("event_type")).alias("event_type"),
        F.col("product_id").try_cast("long").alias("product_id"),
        F.col("category_id").try_cast("long").alias("category_id"),
        F.lower(_blank_to_null("category_code")).alias("category_code"),
        F.lower(_blank_to_null("brand")).alias("brand"),
        F.col("price").try_cast("double").alias("price"),
        F.col("user_id").try_cast("long").alias("user_id"),
        _blank_to_null("user_session").alias("user_session"),
    )


def with_reject_reason(frame: DataFrame) -> DataFrame:
    reason = F.lit(None).cast("string")
    for name, condition in reversed(REJECT_RULES):
        reason = F.when(F.expr(condition), F.lit(name)).otherwise(reason)
    return frame.withColumn("reject_reason", reason)


def enrich(frame: DataFrame) -> DataFrame:
    parts = F.split(F.col("category_code"), "\\.")
    return (
        frame.withColumn("category_l1", F.get(parts, F.lit(0)))
        .withColumn("category_l2", F.get(parts, F.lit(1)))
        .withColumn("category_l3", F.get(parts, F.lit(2)))
        .withColumn("event_hour", F.hour(F.col("event_time")))
        .withColumn("event_date", F.to_date(F.col("event_time")))
    )


def event_columns(frame: DataFrame) -> DataFrame:
    return frame.select(*[F.col(name) for name in EVENT_COLUMNS])


def quarantine_columns(frame: DataFrame) -> DataFrame:
    return frame.select(
        *[F.col("_raw").getField(name).alias(name) for name in SOURCE_COLUMNS],
        F.col("reject_reason"),
    )


def prepare(frame: DataFrame) -> DataFrame:
    return with_reject_reason(parse_source(frame))


def split_valid(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = event_columns(enrich(frame.filter(F.col("reject_reason").isNull())))
    rejected = quarantine_columns(frame.filter(F.col("reject_reason").isNotNull()))
    return valid, rejected


def deduplicate(frame: DataFrame, keys: tuple[str, ...]) -> DataFrame:
    return frame.dropDuplicates(list(keys))


def filter_dates(frame: DataFrame, dates: tuple[date, ...]) -> DataFrame:
    if not dates:
        return frame
    return frame.filter(F.col("event_date").isin([F.lit(day) for day in dates]))
