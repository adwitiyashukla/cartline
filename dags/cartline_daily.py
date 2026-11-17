from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

PROJECT_ROOT = Path(os.environ.get("CARTLINE_HOME", Path(__file__).resolve().parents[1]))
CONFIG = os.environ.get("CARTLINE_CONFIG", str(PROJECT_ROOT / "config.toml"))
PYTHON = os.environ.get("CARTLINE_PYTHON", "python")
BRONZE = PROJECT_ROOT / "data" / "lake" / "bronze" / "events"

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def bronze_partition_ready(ds: str) -> bool:
    month = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y-%b")
    return any((BRONZE / f"source_month={month}").glob("*.parquet"))


with DAG(
    dag_id="cartline_daily",
    description="rebuild silver and gold for one event date",
    schedule="@daily",
    start_date=datetime(2019, 10, 1, tzinfo=UTC),
    end_date=datetime(2019, 10, 31, tzinfo=UTC),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cartline", "silver", "gold"],
) as dag:
    wait_for_bronze = PythonSensor(
        task_id="wait_for_bronze",
        python_callable=bronze_partition_ready,
        op_kwargs={"ds": "{{ ds }}"},
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m cartline.cli --config {CONFIG} "
            "silver --date {{ ds }}"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m cartline.cli --config {CONFIG} "
            "gold --date {{ ds }}"
        ),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m cartline.cli --config {CONFIG} quality",
    )

    wait_for_bronze >> build_silver >> build_gold >> quality_gate
