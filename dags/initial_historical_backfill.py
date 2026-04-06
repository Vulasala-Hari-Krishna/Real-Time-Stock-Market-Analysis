"""One-time historical backfill DAG (manual trigger only).

Downloads 5 years of daily OHLCV data for every watchlist symbol via
yfinance, writes bronze JSON and silver Parquet to S3.  Validates that
all symbols were backfilled successfully.

This DAG has **no schedule** — it is meant to run once when the
pipeline is first deployed (or when a full data refresh is needed).
Trigger it manually from the Airflow UI.

Schedule: None (manual trigger via Airflow UI).
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
    dag_id="initial_historical_backfill",
    description="One-time 5-year OHLCV backfill from yfinance to bronze/silver S3 layers",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "backfill", "one-time"],
)
def initial_historical_backfill() -> None:
    """Orchestrate full historical data backfill and validation."""

    @task()
    def fetch_historical_data() -> dict[str, bool]:
        """Download 5-year OHLCV history and write to bronze/silver layers.

        Returns:
            Dict mapping each symbol to its success status.
        """
        from src.batch.historical_backfill import run_backfill

        results = run_backfill()
        succeeded = sum(1 for v in results.values() if v)
        logger.info(
            "Full backfill complete: %d/%d succeeded", succeeded, len(results)
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


initial_historical_backfill()
