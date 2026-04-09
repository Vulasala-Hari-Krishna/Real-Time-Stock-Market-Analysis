"""Daily tick-to-OHLCV rollup DAG.

Reads real-time tick data from ``silver/stock_ticks`` (written by Spark
Streaming), aggregates into daily OHLCV bars, deduplicates against the
existing ``silver/historical`` data, and appends new bars.  This bridges
the speed layer (Kafka → Spark Streaming) and the batch layer (daily
aggregation → gold).

The Spark job is submitted to the standalone Spark cluster via
``spark-submit`` — Airflow only orchestrates.

Schedule: daily at 06:00 UTC (runs before daily_batch_aggregation at 07:00).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from spark_submit_config import spark_submit_cmd

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="daily_tick_rollup",
    description="Roll up real-time ticks into daily OHLCV bars for the batch layer",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "rollup", "lambda-architecture"],
)
def daily_tick_rollup() -> None:
    """Orchestrate daily tick rollup and validation."""

    rollup = BashOperator(
        task_id="rollup_ticks",
        bash_command=spark_submit_cmd("/opt/airflow/src/batch/tick_rollup.py"),
    )

    @task()
    def validate_rollup() -> None:
        """Log the rollup result.

        On weekends or holidays the tick stream may produce zero bars,
        which is expected.  This task logs the outcome without failing.
        """
        logger.info(
            "Tick rollup spark-submit completed successfully. "
            "Zero bars on weekends/holidays is expected."
        )

    rollup >> validate_rollup()


daily_tick_rollup()
