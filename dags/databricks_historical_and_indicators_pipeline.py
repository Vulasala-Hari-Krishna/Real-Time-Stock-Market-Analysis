"""Databricks historical OHLCV + indicators pipeline DAG (R8).

Triggers ``landed_historical`` (multi-year OHLCV backfill), publishes its
gold output to Snowflake, then triggers ``landed_indicators`` (daily
summaries/sector performance/correlations, computed from
``landed_historical``'s already-published Databricks gold table directly -
not from the Snowflake copy) and publishes its three gold outputs.

Both jobs always do a fresh fetch/recompute (no landed-file input gate like
``databricks_ticks_pipeline``'s), so this DAG runs on a plain daily schedule
rather than checking for new input first.

**Known blocker**: ``landed_historical`` fetches via yfinance, which is
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
    dag_id="databricks_historical_and_indicators_pipeline",
    description=(
        "Trigger landed_historical + landed_indicators on Databricks, "
        "then publish both to Snowflake"
    ),
    schedule="0 5 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_historical_and_indicators_pipeline() -> None:
    """Orchestrate both jobs' triggers and their Snowflake publications."""

    trigger_historical = trigger_databricks_job(
        task_id="trigger_landed_historical",
        job_short_name="landed-historical",
        timeout_seconds=2700,
    )
    publish_historical = export_and_load.override(task_id="publish_historical_ohlcv")(
        "historical_ohlcv", "{{ run_id }}"
    )

    trigger_indicators = trigger_databricks_job(
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

    # indicators reads landed_historical's Databricks gold table directly,
    # not the Snowflake copy - it only needs the job to have completed, not
    # publish_historical_ohlcv.
    trigger_historical >> [publish_historical, trigger_indicators]
    trigger_indicators >> [
        publish_daily_summaries,
        publish_sector_performance,
        publish_correlations,
    ]


databricks_historical_and_indicators_pipeline()
