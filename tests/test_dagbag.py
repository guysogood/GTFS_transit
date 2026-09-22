from pathlib import Path

import pytest

pytest.importorskip("airflow.models")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_daily_transit_batch_structure(dagbag):
    dag = dagbag.dags.get("daily_transit_batch")
    assert dag is not None
    assert dag.catchup is False
    assert [t.task_id for t in dag.topological_sort()] == [
        "wait_for_day_complete",
        "validate_landing_data",
        "bronze_to_silver",
        "silver_to_gold",
        "load_to_bigquery",
        "post_load_quality_check",
    ]


def test_every_task_is_date_parameterized(dagbag):
    dag = dagbag.dags.get("daily_transit_batch")
    for task in dag.tasks:
        text = getattr(task, "bash_command", None) or task.filepath
        assert "{{ ds }}" in text, task.task_id


def test_failure_callback_and_sla_configured(dagbag):
    dag = dagbag.dags.get("daily_transit_batch")
    assert dag.sla_miss_callback is not None
    for task in dag.tasks:
        assert task.on_failure_callback is not None
        assert task.sla is not None
