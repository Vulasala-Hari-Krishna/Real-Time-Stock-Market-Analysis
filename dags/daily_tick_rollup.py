"""Daily tick-to-OHLCV rollup DAG.

Reads real-time tick data from ``silver/stock_ticks`` (written by Spark
Streaming), aggregates into daily OHLCV bars, deduplicates against the
existing ``silver/historical`` data, and appends new bars.  This bridges
the speed layer (Kafka → Spark Streaming) and the batch layer (daily
aggregation → gold).

Schedule: daily at 06:00 UTC (runs before daily_batch_aggregation at 07:00).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

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

    @task()
    def rollup_ticks() -> int:
        """Aggregate ticks to daily bars and append to silver/historical.

        Returns:
            Number of new daily bars written.
        """
        from src.batch.tick_rollup import create_spark_session, run_tick_rollup

        spark = create_spark_session()
        try:
            rows = run_tick_rollup(spark)
            logger.info("Tick rollup wrote %d new daily bars", rows)
            return rows
        finally:
            spark.stop()

    @task()
    def validate_rollup(rows_written: int) -> None:
        """Log the rollup result.

        On weekends or holidays the tick stream may produce zero bars,
        which is expected.  This task logs the outcome without failing.

        Args:
            rows_written: Number of bars written by the rollup task.
        """
        if rows_written == 0:
            logger.warning(
                "No new bars written — expected on weekends/holidays "
                "or if the streaming pipeline was paused."
            )
        else:
            logger.info("Validated: %d new daily bars written", rows_written)

    rows = rollup_ticks()
    validate_rollup(rows)


daily_tick_rollup()
