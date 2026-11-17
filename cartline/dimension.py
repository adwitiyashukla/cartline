from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cartline.audit import new_run_id, record_run, started
from cartline.config import Config
from cartline.lake import read_table, write_table
from cartline.silver import silver_frame

DIMENSION_TABLE = "dim_product"


def daily_attributes(frame: DataFrame, attributes: tuple[str, ...]) -> DataFrame:
    picked = F.min_by(F.struct(*[F.col(name) for name in attributes]), F.col("event_time"))
    return (
        frame.groupBy("product_id", "event_date")
        .agg(picked.alias("attributes"), F.count(F.lit(1)).alias("events"))
        .select(
            "product_id",
            "event_date",
            "events",
            *[F.col("attributes").getField(name).alias(name) for name in attributes],
        )
    )


def scd2(frame: DataFrame, attributes: tuple[str, ...]) -> DataFrame:
    history = Window.partitionBy("product_id").orderBy("event_date")
    signature = F.concat_ws(
        "|", *[F.coalesce(F.col(name).cast("string"), F.lit("~")) for name in attributes]
    )
    changed = F.when(F.lag(signature).over(history).isNull(), F.lit(1)).otherwise(
        (signature != F.lag(signature).over(history)).cast("int")
    )
    versioned = (
        frame.withColumn("signature", signature)
        .withColumn("changed", changed)
        .withColumn("version", F.sum("changed").over(history))
    )
    grouped = versioned.groupBy("product_id", "version").agg(
        F.min("event_date").alias("valid_from"),
        F.max("event_date").alias("last_seen"),
        F.sum("events").alias("events"),
        *[F.first(name).alias(name) for name in attributes],
    )
    forward = Window.partitionBy("product_id").orderBy("version")
    next_start = F.lead("valid_from").over(forward)
    return (
        grouped.withColumn("valid_to", F.date_sub(next_start, 1))
        .withColumn("is_current", next_start.isNull())
        .select(
            "product_id",
            "version",
            *attributes,
            "valid_from",
            "valid_to",
            "is_current",
            "last_seen",
            "events",
        )
    )


def build_dimension(spark: SparkSession, config: Config, run_id: str = "") -> dict:
    begin = started()
    attributes = config.dimension.attributes
    events = silver_frame(spark, config)
    rows_in = events.count()
    snapshots = daily_attributes(events, attributes)
    dimension = scd2(snapshots, attributes)
    write_table(
        dimension, config.paths.gold_table(DIMENSION_TABLE), files=config.gold.output_partitions
    )
    written = read_table(spark, config.paths.gold_table(DIMENSION_TABLE))
    rows_out = written.count()
    return record_run(
        spark,
        config,
        run_id or new_run_id(),
        "dimension",
        DIMENSION_TABLE,
        begin,
        rows_in=rows_in,
        rows_out=rows_out,
    )
