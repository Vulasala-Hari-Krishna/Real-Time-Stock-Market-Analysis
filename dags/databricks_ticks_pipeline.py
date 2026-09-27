"""Databricks ticks pipeline DAG (R8).

Triggers the deployed ``landed_ticks`` Databricks job, then publishes its
gold output (``daily_quote_summary``) to Snowflake. This is the first of
three new DAGs that give local Airflow the triggering role the target
architecture always assigned it (see ``docs/hybrid-migration.md``):
GitHub Actions may only deploy/update/teardown these jobs, never run them
(see the standing ``feedback_github_actions_infra_only`` project memory).

Unlike the historical/fundamentals jobs (always a fresh fetch), ``landed_ticks``
only has new work when new files have landed under ``landing/ticks/`` since
its last run - this DAG checks that first and skips the rest of the run
otherwise, closing a gap flagged since R1 ("a manually started no-op job
still incurs startup cost until an external input gate is built").

Ships paused (``AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`` is already
``true`` for this project) - unpausing is a separate, deliberate action.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable

from databricks_pipeline_common import export_and_load, trigger_databricks_job
from src.config.settings import get_settings

WATERMARK_VARIABLE = "ticks_landing_watermark"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="databricks_ticks_pipeline",
    description="Trigger landed_ticks on Databricks, then publish to Snowflake",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_ticks_pipeline() -> None:
    """Orchestrate the ticks job trigger and its Snowflake publication."""

    @task.short_circuit()
    def check_for_new_landing_files() -> bool:
        """Skip the rest of this run when nothing new landed since last time.

        Compares the newest object's LastModified under ``landing/ticks/``
        against a durable Airflow Variable watermark - Variables are the
        idiomatic Airflow mechanism for small persistent runtime state like
        this, distinct from the static deployment config in Settings.
        """
        settings = get_settings()
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_default_region,
        )
        response = s3.list_objects_v2(
            Bucket=settings.s3_bucket_name, Prefix="landing/ticks/"
        )
        objects = response.get("Contents", [])
        if not objects:
            return False

        latest = max(obj["LastModified"] for obj in objects).isoformat()
        watermark = Variable.get(WATERMARK_VARIABLE, default_var=None)
        if watermark is not None and latest <= watermark:
            return False

        Variable.set(WATERMARK_VARIABLE, latest)
        return True

    gate = check_for_new_landing_files()
    trigger = trigger_databricks_job(
        task_id="trigger_landed_ticks",
        job_short_name="landed-ticks",
        timeout_seconds=1800,
    )
    publish = export_and_load.override(task_id="publish_daily_quote_summary")(
        "daily_quote_summary", "{{ run_id }}"
    )

    gate >> trigger >> publish


databricks_ticks_pipeline()
