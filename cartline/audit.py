from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession

from cartline.config import Config
from cartline.lake import append_table, read_optional
from cartline.schema import AUDIT_SCHEMA


def new_run_id() -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}-{uuid.uuid4().hex[:6]}"


def record_run(
    spark: SparkSession,
    config: Config,
    run_id: str,
    job: str,
    target: str,
    started_at: datetime,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_rejected: int = 0,
    rows_deduplicated: int = 0,
    partitions_written: int = 0,
) -> dict:
    finished_at = datetime.now(UTC)
    seconds = (finished_at - started_at).total_seconds()
    row = (
        run_id,
        job,
        target,
        started_at.replace(tzinfo=None),
        finished_at.replace(tzinfo=None),
        seconds,
        int(rows_in),
        int(rows_out),
        int(rows_rejected),
        int(rows_deduplicated),
        int(partitions_written),
        config.fingerprint,
    )
    frame = spark.createDataFrame([row], AUDIT_SCHEMA)
    append_table(frame, config.paths.audit)
    return {
        "run_id": run_id,
        "job": job,
        "target": target,
        "seconds": round(seconds, 2),
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "rows_rejected": int(rows_rejected),
        "rows_deduplicated": int(rows_deduplicated),
        "partitions_written": int(partitions_written),
    }


def audit_frame(spark: SparkSession, config: Config) -> DataFrame | None:
    return read_optional(spark, config.paths.audit)


def started() -> datetime:
    return datetime.now(UTC)
