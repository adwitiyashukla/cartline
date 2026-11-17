from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag

DAG_FOLDER = Path(__file__).resolve().parents[1] / "dags"
EXPECTED = {
    "cartline_ingest": ["wait_for_source", "load_month", "quality_gate"],
    "cartline_daily": ["wait_for_bronze", "build_silver", "build_gold", "quality_gate"],
}


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAG_FOLDER))


def test_dags_import_without_errors(bag):
    assert bag.import_errors == {}
    assert set(bag.dags) == set(EXPECTED)


def test_every_dag_has_the_expected_tasks(bag):
    for dag_id, tasks in EXPECTED.items():
        assert sorted(task.task_id for task in bag.dags[dag_id].tasks) == sorted(tasks)


def test_tasks_retry_and_run_one_at_a_time(bag):
    for dag_id in EXPECTED:
        dag = bag.dags[dag_id]
        assert dag.max_active_runs == 1
        for task in dag.tasks:
            assert task.retries == 2


def test_daily_dag_backfills_the_data_range(bag):
    dag = bag.dags["cartline_daily"]
    assert dag.catchup is True
    assert dag.start_date.year == 2019
    assert dag.end_date.month == 10
