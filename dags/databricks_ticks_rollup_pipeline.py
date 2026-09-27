"""Databricks ticks-rollup pipeline DAG (R8).

Triggers the deployed ``landed_ticks_rollup`` Databricks job - a cheap,
zero-external-API-call sync of ``landed_ticks``' already-published
``daily_quote_summary`` into ``historical_ohlcv`` - then publishes that gold
output to Snowflake.

Added 2026-09-27 to close a real inefficiency: without this job,
``historical_ohlcv`` was kept "up to date" only by ``landed_historical``'s
full 5-year yfinance re-fetch, exactly the pattern legacy avoids. Legacy
instead splits historical into two feeders of one target - one-time/rare
``initial_historical_backfill.py`` plus a cheap daily
``daily_tick_rollup.py`` that reuses data the streaming layer already
captured, at zero extra fetch cost. This DAG is that second feeder's hybrid
equivalent, and its schedule matches ``daily_tick_rollup.py``'s exactly:
weekdays at 06:00 UTC, ahead of ``databricks_indicators_pipeline``'s 07:00 so
indicators can read a same-day-fresh ``historical_ohlcv``.

See ``databricks_historical_pipeline.py``'s docstring: with this job in
place, that one now matches ``initial_historical_backfill.py``'s manual-only
role instead of running on its own weekly schedule.

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
    dag_id="databricks_ticks_rollup_pipeline",
    description="Trigger landed_ticks_rollup on Databricks, then publish to Snowflake",
    schedule="0 6 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["databricks", "snowflake", "r8"],
)
def databricks_ticks_rollup_pipeline() -> None:
    """Orchestrate the ticks-rollup job trigger and its Snowflake publication."""

    trigger = trigger_databricks_job(
        task_id="trigger_landed_ticks_rollup",
        job_short_name="landed-ticks-rollup",
        timeout_seconds=900,
    )
    publish = export_and_load.override(task_id="publish_historical_ohlcv")(
        "historical_ohlcv", "{{ run_id }}"
    )

    trigger >> publish


databricks_ticks_rollup_pipeline()
