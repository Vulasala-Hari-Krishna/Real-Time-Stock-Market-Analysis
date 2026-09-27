"""Databricks indicators pipeline DAG (R8).

Triggers the deployed ``landed_indicators`` Databricks job (daily
summaries/signals, sector performance, correlations - computed from
``landed_historical``'s already-published Databricks gold table directly,
not from the Snowflake copy), then publishes all three gold outputs to
Snowflake.

Split out from what was originally a single combined
``databricks_historical_and_indicators_pipeline`` DAG (2026-09-27) so this
job's schedule isn't tied to ``landed_historical``'s - see
``databricks_historical_pipeline.py``'s docstring for why they needed
different cadences. This one is scheduled daily, weekdays, matching the
legacy ``daily_batch_aggregation.py``'s cadence exactly (its direct analog).
It does not depend on ``databricks_historical_pipeline`` having just run -
only on ``landed_historical`` having published *some* version of
``historical_ohlcv`` previously, which it reads at whatever version is
currently available.

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
    dag_id="databricks_indicators_pipeline",
    description="Trigger landed_indicators on Databricks, then publish to Snowflake",
    schedule="0 7 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_indicators_pipeline() -> None:
    """Orchestrate the indicators job trigger and its Snowflake publications."""

    trigger = trigger_databricks_job(
        task_id="trigger_landed_indicators",
        job_short_name="landed-indicators",
        timeout_seconds=1800,
    )
    publish_daily_summaries = export_and_load.override(
        task_id="publish_daily_summaries"
    )("daily_summaries", "{{ run_id }}")
    publish_sector_performance = export_and_load.override(
        task_id="publish_sector_performance"
    )("sector_performance", "{{ run_id }}")
    publish_correlations = export_and_load.override(task_id="publish_correlations")(
        "correlations", "{{ run_id }}"
    )

    trigger >> [
        publish_daily_summaries,
        publish_sector_performance,
        publish_correlations,
    ]


databricks_indicators_pipeline()
