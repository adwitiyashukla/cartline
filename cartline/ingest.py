from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cartline.audit import new_run_id, record_run, started
from cartline.config import Config
from cartline.generate import month_path
from cartline.lake import read_optional, spark_path
from cartline.schema import SOURCE_SCHEMA
from cartline.transform import prepare, split_valid


def read_source(spark: SparkSession, path: Path) -> DataFrame:
    return (
        spark.read.schema(SOURCE_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .csv(path.as_posix())
    )


def month_partition(root: Path, month: str) -> Path:
    return root / f"source_month={month}"


def ingest_month(spark: SparkSession, config: Config, month: str, run_id: str) -> dict:
    path = month_path(config, month)
    if not path.exists():
        raise FileNotFoundError(f"no source for {month} at {path}")
    begin = started()
    valid, rejected = split_valid(prepare(read_source(spark, path)))
    events_path = month_partition(config.paths.bronze_events, month)
    quarantine_path = month_partition(config.paths.bronze_quarantine, month)
    (
        valid.write.mode("overwrite")
        .option("maxRecordsPerFile", config.ingest.max_records_per_file)
        .parquet(spark_path(events_path))
    )
    (
        rejected.withColumn("run_id", F.lit(run_id))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(spark_path(quarantine_path))
    )
    written = spark.read.parquet(spark_path(events_path))
    rows_out = written.count()
    partitions = written.select("event_date").distinct().count()
    rows_rejected = spark.read.parquet(spark_path(quarantine_path)).count()
    return record_run(
        spark,
        config,
        run_id,
        "ingest",
        month,
        begin,
        rows_in=rows_out + rows_rejected,
        rows_out=rows_out,
        rows_rejected=rows_rejected,
        partitions_written=partitions,
    )


def ingest(
    spark: SparkSession, config: Config, months: tuple[str, ...] = (), run_id: str = ""
) -> list[dict]:
    selected = months or config.source.months
    unknown = [month for month in selected if month not in config.source.months]
    if unknown:
        raise ValueError(f"months not listed in config.source.months: {unknown}")
    identifier = run_id or new_run_id()
    return [ingest_month(spark, config, month, identifier) for month in selected]


def quarantine_rate(spark: SparkSession, config: Config) -> float:
    events = read_optional(spark, config.paths.bronze_events)
    rejected = read_optional(spark, config.paths.bronze_quarantine)
    if events is None:
        return 0.0
    good = events.count()
    bad = 0 if rejected is None else rejected.count()
    total = good + bad
    return 0.0 if total == 0 else bad / total
