"""Databricks historical OHLCV pipeline DAG (R8).

Triggers the deployed ``landed_historical`` Databricks job (multi-year OHLCV
backfill), then publishes its gold output (``historical_ohlcv``) to
Snowflake.

**Manual trigger only, no schedule** (changed 2026-09-27): now that
``databricks_ticks_rollup_pipeline`` exists to cheaply sync already-captured
data into ``historical_ohlcv`` on a daily/weekdays schedule at zero extra
fetch cost, this job's own always-full 5-year yfinance re-fetch has no
reason to run on any recurring cadence of its own - exactly mirroring legacy,
where ``initial_historical_backfill.py`` (this job's hybrid equivalent) is
also manual/one-time-or-rare-deliberate-rebackfill, with ``daily_tick_rollup.py``
(``databricks_ticks_rollup_pipeline``'s hybrid equivalent) owning the daily
sync instead. Trigger by hand only for the initial backfill or a deliberate
full rebuild (e.g. after a retroactive split/dividend correction).

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
    schedule=None,
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
