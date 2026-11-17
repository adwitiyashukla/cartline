from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, Param

PROJECT_ROOT = Path(os.environ.get("CARTLINE_HOME", Path(__file__).resolve().parents[1]))
CONFIG = os.environ.get("CARTLINE_CONFIG", str(PROJECT_ROOT / "config.toml"))
PYTHON = os.environ.get("CARTLINE_PYTHON", "python")
SOURCE_GLOB = str(PROJECT_ROOT / "data" / "source" / "*{{ params.month }}*")

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

with DAG(
    dag_id="cartline_ingest",
    description="land one source month in bronze and check the quarantine rate",
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    params={"month": Param("2019-Oct", type="string")},
    tags=["cartline", "bronze"],
) as dag:
    wait_for_source = FileSensor(
        task_id="wait_for_source",
        filepath=SOURCE_GLOB,
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )

    load_month = BashOperator(
        task_id="load_month",
        bash_command=(
            f"cd {PROJECT_ROOT} && {PYTHON} -m cartline.cli --config {CONFIG} "
            "ingest --month {{ params.month }}"
        ),
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {PROJECT_ROOT} && {PYTHON} -m cartline.cli --config {CONFIG} quality",
    )

    wait_for_source >> load_month >> quality_gate
