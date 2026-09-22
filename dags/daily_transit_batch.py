import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

log = logging.getLogger(__name__)

PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/project")
LANDING = f"{PROJECT_DIR}/landing"
RUN = f"cd {PROJECT_DIR} && python -m"
DATE = "{{ ds }}"


def notify_failure(context: dict) -> None:
    ti = context["task_instance"]
    log.error("Task failed: dag=%s task=%s date=%s", ti.dag_id, ti.task_id, context["ds"])


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    log.error("SLA missed for dag=%s tasks=%s", dag.dag_id, task_list)


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "sla": timedelta(hours=3),
}

with DAG(
    dag_id="daily_transit_batch",
    description="MBTA GTFS-RT landing -> silver -> gold -> BigQuery, one date partition per run",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=notify_sla_miss,
    tags=["gtfs", "batch"],
) as dag:
    wait_for_day_complete = FileSensor(
        task_id="wait_for_day_complete",
        filepath=f"{LANDING}/{DATE}/_SUCCESS",
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 60 * 60,
    )

    validate_landing_data = BashOperator(
        task_id="validate_landing_data",
        bash_command=f"{RUN} batch.validate_landing --date {DATE}",
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{RUN} batch.bronze_to_silver --date {DATE}",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{RUN} batch.silver_to_gold --date {DATE}",
    )

    load_to_bigquery = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"{RUN} batch.load_to_bigquery --date {DATE}",
    )

    post_load_quality_check = BashOperator(
        task_id="post_load_quality_check",
        bash_command=(
            f"{RUN} batch.quality_check --date {DATE} "
            "--table $GCP_PROJECT.$BQ_DATASET.route_daily_metrics"
        ),
    )

    (
        wait_for_day_complete
        >> validate_landing_data
        >> bronze_to_silver
        >> silver_to_gold
        >> load_to_bigquery
        >> post_load_quality_check
    )
