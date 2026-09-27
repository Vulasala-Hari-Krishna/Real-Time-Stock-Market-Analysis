"""Databricks fundamentals pipeline DAG (R8).

Triggers the deployed ``landed_fundamentals`` Databricks job, then publishes
its gold output (``fundamentals``) to Snowflake. Scheduled weekly, matching
the legacy ``fundamental_data_refresh.py``'s cadence - fundamentals change
slowly, unlike ticks/historical prices.

**Known blocker**: ``landed_fundamentals`` fetches via yfinance, which is
currently rate-limited/blocked from both GitHub Actions' and Databricks'
networks (see ``docs/implementation-handover.md``'s 2026-09-26 entries) - an
actual trigger of this DAG will fail at that step until that issue is
separately resolved. This DAG is built and deployed regardless per explicit
instruction to complete the infra first and revisit running/testing later;
it stays paused until then.

Ships paused (see ``databricks_ticks_pipeline.py``'s docstring for why).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag

from databricks_pipeline_common import export_and_load, trigger_databricks_job

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="databricks_fundamentals_pipeline",
    description="Trigger landed_fundamentals on Databricks, then publish to Snowflake",
    schedule="0 6 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_fundamentals_pipeline() -> None:
    """Orchestrate the fundamentals job trigger and its Snowflake publication."""

    trigger = trigger_databricks_job(
        task_id="trigger_landed_fundamentals",
        job_short_name="landed-fundamentals",
        timeout_seconds=2700,
    )
    publish = export_and_load.override(task_id="publish_fundamentals")(
        "fundamentals", "{{ run_id }}"
    )

    trigger >> publish


databricks_fundamentals_pipeline()
