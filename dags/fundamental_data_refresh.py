"""Weekly fundamental data refresh DAG.

Fetches the latest company fundamentals (P/E, market cap, dividend
yield, 52-week range, etc.) from yfinance and writes to the gold
layer.  Runs weekly on Sundays.

The Spark job is submitted to the standalone Spark cluster via
``spark-submit`` — Airflow only orchestrates.

Schedule: weekly at 06:00 UTC on Sundays.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from spark_submit_config import spark_submit_cmd

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="fundamental_data_refresh",
    description="Weekly refresh of company fundamentals from yfinance",
    schedule="0 6 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "fundamentals"],
)
def fundamental_data_refresh() -> None:
    """Orchestrate weekly fundamental data refresh."""

    BashOperator(
        task_id="refresh_fundamentals",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/fundamental_enrichment.py"
        ),
    )


fundamental_data_refresh()
