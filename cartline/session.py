from __future__ import annotations

import os
import sys
from contextlib import contextmanager

from pyspark.sql import SparkSession

from cartline.config import Config


def _apply_submit_args(config: Config) -> None:
    if SparkSession.getActiveSession() is not None:
        return
    if os.environ.get("PYSPARK_SUBMIT_ARGS"):
        return
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {config.spark.driver_memory} pyspark-shell"
    )


def _apply_worker_python() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def build_session(config: Config, job: str = "") -> SparkSession:
    _apply_worker_python()
    _apply_submit_args(config)
    name = config.spark.app_name if not job else f"{config.spark.app_name}.{job}"
    builder = (
        SparkSession.builder.appName(name)
        .master(config.spark.master)
        .config("spark.python.unix.domain.socket.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        .config("spark.sql.files.maxPartitionBytes", config.spark.max_partition_bytes)
        .config("spark.sql.parquet.compression.codec", config.spark.parquet_compression)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", str(config.spark.adaptive).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(config.spark.adaptive_skew_join).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.warehouse.dir", config.paths.lake.as_posix())
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", config.spark.driver_memory)
    )
    if config.spark.local_dir:
        builder = builder.config("spark.local.dir", config.spark.local_dir)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(config.spark.log_level)
    return session


@contextmanager
def session_scope(config: Config, job: str = ""):
    session = build_session(config, job)
    try:
        yield session
    finally:
        session.stop()
