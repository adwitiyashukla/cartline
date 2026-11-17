from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from cartline.audit import new_run_id, record_run, started
from cartline.config import Config
from cartline.lake import read_optional, read_table, spark_path, write_table
from cartline.schema import SOURCE_COLUMNS, SOURCE_SCHEMA
from cartline.transform import prepare, split_valid

EVENTS_QUERY = "stream_events"


def stream_source(spark: SparkSession, config: Config) -> DataFrame:
    return (
        spark.readStream.schema(SOURCE_SCHEMA)
        .option("header", "true")
        .option("maxFilesPerTrigger", config.stream.max_files_per_trigger)
        .csv(spark_path(config.paths.landing))
    )


def deduplicated_stream(spark: SparkSession, config: Config) -> DataFrame:
    prepared = prepare(stream_source(spark, config)).withColumn("source_month", F.lit("stream"))
    return prepared.withWatermark(
        "event_time", f"{config.stream.watermark_minutes} minutes"
    ).dropDuplicatesWithinWatermark(list(config.silver.dedup_keys))


def _refresh_traffic(spark: SparkSession, config: Config, dates: list) -> None:
    if not dates:
        return
    events = read_table(spark, config.paths.stream_events).filter(F.col("event_date").isin(dates))
    traffic = events.groupBy("event_date", "event_hour").agg(
        F.count(F.lit(1)).alias("events"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("user_session").alias("sessions"),
        F.max("event_time").alias("latest_event"),
    )
    write_table(traffic, config.paths.stream_traffic, ("event_date",), files=1)


def batch_writer(spark: SparkSession, config: Config, run_id: str):
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        valid, rejected = split_valid(batch)
        valid = valid.withColumn("source_month", F.lit("stream"))
        valid.persist()
        rows = valid.count()
        if rows:
            write_table(
                valid,
                config.paths.stream_events,
                ("event_date",),
                mode="append",
                files=1,
                spread=("event_date",),
            )
            dates = [row["event_date"] for row in valid.select("event_date").distinct().collect()]
            _refresh_traffic(spark, config, dates)
        rejects = rejected.withColumn("source_month", F.lit("stream")).withColumn(
            "run_id", F.lit(f"{run_id}-{batch_id}")
        )
        rejected_rows = rejects.count()
        if rejected_rows:
            write_table(
                rejects, config.paths.bronze_quarantine, ("source_month",), mode="append", files=1
            )
        valid.unpersist()

    return write_batch


def start_stream(spark: SparkSession, config: Config, run_id: str) -> StreamingQuery:
    writer = (
        deduplicated_stream(spark, config)
        .writeStream.queryName(EVENTS_QUERY)
        .option("checkpointLocation", spark_path(config.paths.checkpoints / EVENTS_QUERY))
        .foreachBatch(batch_writer(spark, config, run_id))
    )
    if config.stream.trigger == "availableNow":
        return writer.trigger(availableNow=True).start()
    return writer.trigger(processingTime=f"{config.stream.processing_time_seconds} seconds").start()


def run_stream(spark: SparkSession, config: Config, timeout: int = 0, run_id: str = "") -> dict:
    identifier = run_id or new_run_id()
    begin = started()
    before = read_optional(spark, config.paths.stream_events)
    rows_before = 0 if before is None else before.count()
    query = start_stream(spark, config, identifier)
    if timeout > 0:
        query.awaitTermination(timeout)
        query.stop()
    else:
        query.awaitTermination()
    after = read_optional(spark, config.paths.stream_events)
    rows_after = 0 if after is None else after.count()
    partitions = 0 if after is None else after.select("event_date").distinct().count()
    return record_run(
        spark,
        config,
        identifier,
        "stream",
        EVENTS_QUERY,
        begin,
        rows_in=rows_after - rows_before,
        rows_out=rows_after - rows_before,
        partitions_written=partitions,
    )


def seed_landing(
    spark: SparkSession,
    config: Config,
    events: int = 20000,
    files: int = 4,
    late_share: float = 0.15,
    late_hours: int = 3,
) -> dict:
    source = (
        spark.read.schema(SOURCE_SCHEMA)
        .option("header", "true")
        .csv(spark_path(config.paths.source))
    )
    total = source.count()
    fraction = min(1.0, max(events / max(total, 1), 0.0))
    sampled = source.sample(fraction, config.generate.seed + 77).limit(events)
    stamp = F.try_to_timestamp(
        F.regexp_replace(F.col("event_time"), " UTC$", ""), F.lit("yyyy-MM-dd HH:mm:ss")
    )
    shifted = F.when(
        F.rand(config.generate.seed + 78) < F.lit(late_share),
        stamp - F.expr(f"INTERVAL {late_hours} HOURS"),
    ).otherwise(stamp)
    prepared = sampled.withColumn("arrival", F.col("event_time")).withColumn(
        "event_time",
        F.when(stamp.isNull(), F.col("event_time")).otherwise(
            F.concat(F.date_format(shifted, "yyyy-MM-dd HH:mm:ss"), F.lit(" UTC"))
        ),
    )
    (
        prepared.repartitionByRange(files, "arrival")
        .sortWithinPartitions("arrival")
        .select(*SOURCE_COLUMNS)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(spark_path(config.paths.landing))
    )
    return {"landing": str(config.paths.landing), "files": files, "rows": prepared.count()}
