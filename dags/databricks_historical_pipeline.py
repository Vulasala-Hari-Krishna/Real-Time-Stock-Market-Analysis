"""Databricks historical OHLCV pipeline DAG (R8).

Triggers the deployed ``landed_historical`` Databricks job (multi-year OHLCV
backfill), then publishes its gold output (``historical_ohlcv``) to
Snowflake.

Split out from what was originally a single combined
``databricks_historical_and_indicators_pipeline`` DAG (2026-09-27): that DAG
ran daily, matching ``landed_indicators``'s natural cadence but not this
job's. ``landed_historical`` always does a full 5-year, 10-symbol rebuild (a
deliberate "always rebuild" simplification, not incremental like the legacy
``daily_tick_rollup.py``) - closer in nature to the legacy
``initial_historical_backfill.py``'s one-time full fetch than to a cheap
daily append, so running it daily was needlessly heavy. Scheduled weekly
instead - frequent enough to pick up retroactive split/dividend corrections,
far cheaper than daily.

**Known blocker**: ``landed_historical`` fetches via yfinance, and briefly
also failed on a missing ``pydantic-settings`` dependency in
``historical_env`` (both root-caused and fixed 2026-09-27 - see
``docs/implementation-handover.md``) - an actual trigger of this DAG only
exercises the fix once the bundle has been redeployed with it.

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
    dag_id="databricks_historical_pipeline",
    description="Trigger landed_historical on Databricks, then publish to Snowflake",
    schedule="0 5 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_historical_pipeline() -> None:
    """Orchestrate the historical job trigger and its Snowflake publication."""

    trigger = trigger_databricks_job(
        task_id="trigger_landed_historical",
        job_short_name="landed-historical",
        timeout_seconds=2700,
    )
    publish = export_and_load.override(task_id="publish_historical_ohlcv")(
        "historical_ohlcv", "{{ run_id }}"
    )

    trigger >> publish


databricks_historical_pipeline()
