"""Daily historical backfill DAG.

Fetches 5-year OHLCV history for every watchlist symbol via yfinance,
writes bronze JSON and silver Parquet to S3.  Validates that all
symbols were backfilled successfully.

Schedule: daily at 06:00 UTC.
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
    dag_id="daily_historical_backfill",
    description="Backfill historical OHLCV data from yfinance to bronze/silver S3 layers",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "backfill"],
)
def daily_historical_backfill() -> None:
    """Orchestrate historical data backfill and validation."""

    @task()
    def fetch_historical_data() -> dict[str, bool]:
        """Download OHLCV history and write to bronze/silver layers.

        Returns:
            Dict mapping each symbol to its success status.
        """
        from src.batch.historical_backfill import run_backfill

        results = run_backfill()
        succeeded = sum(1 for v in results.values() if v)
        logger.info(
            "Backfill complete: %d/%d succeeded", succeeded, len(results)
        )
        return results

    @task()
    def validate_data(results: dict[str, bool]) -> None:
        """Validate that all watchlist symbols were backfilled.

        Args:
            results: Symbol → success mapping from the backfill task.

        Raises:
            ValueError: If any symbol failed to backfill.
        """
        from src.config.watchlist import SYMBOLS

        failed = [sym for sym in SYMBOLS if not results.get(sym, False)]
        if failed:
            raise ValueError(
                f"Backfill failed for {len(failed)} symbols: {failed}"
            )
        logger.info("All %d symbols validated successfully", len(SYMBOLS))

    backfill_results = fetch_historical_data()
    validate_data(backfill_results)


daily_historical_backfill()
