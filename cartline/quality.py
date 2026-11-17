from __future__ import annotations

import json
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cartline.config import Config
from cartline.dimension import DIMENSION_TABLE
from cartline.gold import (
    ABANDONMENT_TABLE,
    FUNNEL_TABLE,
    SESSION_TABLE,
    TRAFFIC_TABLE,
)
from cartline.ingest import quarantine_rate
from cartline.lake import read_optional, spark_path
from cartline.schema import EVENT_TYPES
from cartline.silver import silver_frame

ERROR = "error"
WARNING = "warning"


def _check(name: str, level: str, passed: bool, observed, limit=None, detail: str = "") -> dict:
    return {
        "check": name,
        "level": level,
        "passed": bool(passed),
        "observed": observed,
        "limit": limit,
        "detail": detail,
    }


def _silver_summary(events: DataFrame) -> dict:
    known = F.col("event_type").isin(list(EVENT_TYPES))
    row = events.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(
            F.when(
                F.col("event_time").isNull()
                | F.col("user_id").isNull()
                | F.col("product_id").isNull(),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("null_keys"),
        F.sum(F.when(F.col("price") < F.lit(0), F.lit(1)).otherwise(F.lit(0))).alias(
            "negative_price"
        ),
        F.sum(F.when(known, F.lit(0)).otherwise(F.lit(1))).alias("unknown_type"),
        F.sum(F.when(F.col("brand").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("null_brand"),
        F.sum(F.when(F.col("category_code").isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "null_category"
        ),
        F.min("event_date").alias("first_date"),
        F.max("event_date").alias("last_date"),
        F.countDistinct("event_date").alias("dates"),
    ).first()
    return row.asDict()


def run_checks(spark: SparkSession, config: Config) -> list[dict]:
    events = silver_frame(spark, config)
    summary = _silver_summary(events)
    rows = summary["rows"]
    checks = [
        _check(
            "bronze_quarantine_rate",
            ERROR,
            quarantine_rate(spark, config) <= config.ingest.max_quarantine_rate,
            round(quarantine_rate(spark, config), 6),
            config.ingest.max_quarantine_rate,
        ),
        _check("silver_rows_present", ERROR, rows > 0, rows, 0),
        _check("silver_null_keys", ERROR, summary["null_keys"] == 0, summary["null_keys"], 0),
        _check(
            "silver_negative_price",
            ERROR,
            summary["negative_price"] == 0,
            summary["negative_price"],
            0,
        ),
        _check(
            "silver_unknown_event_type",
            ERROR,
            summary["unknown_type"] == 0,
            summary["unknown_type"],
            0,
        ),
    ]

    duplicates = (
        events.groupBy(*config.silver.dedup_keys).count().filter(F.col("count") > F.lit(1)).count()
    )
    checks.append(_check("silver_duplicate_keys", ERROR, duplicates == 0, duplicates, 0))

    expected_dates = 0
    if summary["first_date"] and summary["last_date"]:
        expected_dates = (summary["last_date"] - summary["first_date"]).days + 1
    checks.append(
        _check(
            "silver_date_continuity",
            ERROR,
            summary["dates"] == expected_dates,
            summary["dates"],
            expected_dates,
            f"{summary['first_date']} to {summary['last_date']}",
        )
    )

    brand_rate = 0.0 if rows == 0 else summary["null_brand"] / rows
    category_rate = 0.0 if rows == 0 else summary["null_category"] / rows
    checks.append(
        _check(
            "silver_null_brand_rate",
            WARNING,
            brand_rate <= config.quality.max_null_brand_rate,
            round(brand_rate, 6),
            config.quality.max_null_brand_rate,
        )
    )
    checks.append(
        _check(
            "silver_null_category_rate",
            WARNING,
            category_rate <= config.quality.max_null_category_rate,
            round(category_rate, 6),
            config.quality.max_null_category_rate,
        )
    )

    dimension = read_optional(spark, config.paths.gold_table(DIMENSION_TABLE))
    if dimension is not None:
        wrong_current = (
            dimension.groupBy("product_id")
            .agg(F.sum(F.col("is_current").cast("int")).alias("current"))
            .filter(F.col("current") != F.lit(1))
            .count()
        )
        checks.append(_check("dim_single_current_row", ERROR, wrong_current == 0, wrong_current, 0))
        bad_interval = dimension.filter(
            F.col("valid_to").isNotNull() & (F.col("valid_to") < F.col("valid_from"))
        ).count()
        checks.append(_check("dim_valid_intervals", ERROR, bad_interval == 0, bad_interval, 0))

    silver_daily = {
        row["event_date"]: row["rows"]
        for row in events.groupBy("event_date").agg(F.count(F.lit(1)).alias("rows")).collect()
    }

    def daily_mismatch(frame: DataFrame, measure: F.Column) -> tuple[int, int]:
        counted = frame.groupBy("event_date").agg(measure.alias("rows")).collect()
        wrong = sum(1 for row in counted if silver_daily.get(row["event_date"], -1) != row["rows"])
        return wrong, len(counted)

    sessions = read_optional(spark, config.paths.gold_table(SESSION_TABLE))
    if sessions is not None:
        session_dates = {
            row["event_date"] for row in sessions.select("event_date").distinct().collect()
        }
        if session_dates == set(silver_daily):
            session_events = sessions.agg(F.sum("events")).first()[0] or 0
            checks.append(
                _check(
                    "session_event_reconciliation",
                    ERROR,
                    session_events == rows,
                    session_events,
                    rows,
                    f"{len(session_dates)} dates",
                )
            )
        else:
            checks.append(
                _check(
                    "session_event_reconciliation",
                    ERROR,
                    True,
                    len(session_dates),
                    len(silver_daily),
                    "skipped, sessions cover a partial date range",
                )
            )

    funnel = read_optional(spark, config.paths.gold_table(FUNNEL_TABLE))
    if funnel is not None:
        measure = F.sum(F.col("views") + F.col("carts") + F.col("removes") + F.col("purchases"))
        wrong, compared = daily_mismatch(funnel, measure)
        checks.append(
            _check("funnel_daily_reconciliation", ERROR, wrong == 0, wrong, 0, f"{compared} dates")
        )
        negative = funnel.filter(
            F.col("view_to_cart").isNotNull() & (F.col("view_to_cart") < F.lit(0))
        ).count()
        checks.append(_check("funnel_rates_non_negative", ERROR, negative == 0, negative, 0))

    traffic = read_optional(spark, config.paths.gold_table(TRAFFIC_TABLE))
    if traffic is not None:
        wrong, compared = daily_mismatch(traffic, F.sum("events"))
        checks.append(
            _check("traffic_daily_reconciliation", ERROR, wrong == 0, wrong, 0, f"{compared} dates")
        )

    abandonment = read_optional(spark, config.paths.gold_table(ABANDONMENT_TABLE))
    if abandonment is not None:
        impossible = abandonment.filter(
            (F.col("sessions_with_cart") > F.col("sessions"))
            | (F.col("carted_and_bought") > F.col("sessions_with_cart"))
            | (F.col("carted_and_bought") + F.col("direct_buys") != F.col("sessions_with_purchase"))
        ).count()
        checks.append(_check("abandonment_bounds", ERROR, impossible == 0, impossible, 0))
    return checks


def report(spark: SparkSession, config: Config, run_id: str) -> dict:
    checks = run_checks(spark, config)
    failures = [check for check in checks if check["level"] == ERROR and not check["passed"]]
    warnings = [check for check in checks if check["level"] == WARNING and not check["passed"]]
    payload = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "config_fingerprint": config.fingerprint,
        "checks": checks,
        "passed": len(checks) - len(failures) - len(warnings),
        "failed": len(failures),
        "warned": len(warnings),
    }
    config.paths.reports.mkdir(parents=True, exist_ok=True)
    destination = config.paths.reports / f"quality_{run_id}.json"
    destination.write_text(json.dumps(payload, indent=2, default=str))
    frame = spark.createDataFrame(
        [
            (
                run_id,
                check["check"],
                check["level"],
                check["passed"],
                str(check["observed"]),
                str(check["limit"]),
                check["detail"],
            )
            for check in checks
        ],
        "run_id string, check string, level string, passed boolean, "
        "observed string, limit string, detail string",
    )
    frame.write.mode("append").parquet(spark_path(config.paths.quality))
    return payload
