from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession

from cartline.audit import new_run_id, record_run, started
from cartline.config import Config
from cartline.lake import read_optional, read_table, write_table
from cartline.schema import SILVER_PARTITIONS
from cartline.transform import deduplicate, filter_dates


def bronze_frame(spark: SparkSession, config: Config, dates: tuple[date, ...] = ()) -> DataFrame:
    return filter_dates(read_table(spark, config.paths.bronze_events), dates)


def silver_frame(spark: SparkSession, config: Config, dates: tuple[date, ...] = ()) -> DataFrame:
    return filter_dates(read_table(spark, config.paths.silver_events), dates)


def build_silver(
    spark: SparkSession, config: Config, dates: tuple[date, ...] = (), run_id: str = ""
) -> dict:
    begin = started()
    bronze = bronze_frame(spark, config, dates)
    rows_in = bronze.count()
    spread = bronze.repartition(config.silver.output_partitions, "event_date")
    deduplicated = deduplicate(spread, ("event_date", *config.silver.dedup_keys))
    write_table(deduplicated, config.paths.silver_events, SILVER_PARTITIONS)
    written = filter_dates(read_table(spark, config.paths.silver_events), dates)
    rows_out = written.count()
    partitions = written.select("event_date").distinct().count()
    return record_run(
        spark,
        config,
        run_id or new_run_id(),
        "silver",
        "all" if not dates else ",".join(str(day) for day in dates),
        begin,
        rows_in=rows_in,
        rows_out=rows_out,
        rows_deduplicated=rows_in - rows_out,
        partitions_written=partitions,
    )


def silver_dates(spark: SparkSession, config: Config) -> list[date]:
    frame = read_optional(spark, config.paths.silver_events)
    if frame is None:
        return []
    rows = frame.select("event_date").distinct().orderBy("event_date").collect()
    return [row["event_date"] for row in rows]
