"""Weekly fundamental data refresh DAG.

Fetches the latest company fundamentals (P/E, market cap, dividend
yield, 52-week range, etc.) from yfinance and writes to the gold
layer.  Runs weekly on Sundays.

Schedule: weekly at 06:00 UTC on Sundays.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

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

    @task()
    def refresh_fundamentals() -> None:
        """Fetch fundamentals and write to gold layer.

        Downloads the latest fundamentals for all watchlist symbols
        via yfinance, joins with silver-layer price data, and writes
        both standalone fundamentals and enriched prices to the gold
        layer as Parquet.
        """
        from src.batch.fundamental_enrichment import (
            create_spark_session,
            run_fundamental_enrichment,
            write_enriched_gold,
            write_fundamentals_gold,
        )
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session()
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"
            gold_path = f"s3a://{bucket}/gold"

            price_df = spark.read.parquet(silver_path)
            outputs = run_fundamental_enrichment(spark, price_df)
            write_fundamentals_gold(outputs["fundamentals"], gold_path)
            write_enriched_gold(outputs["enriched"], gold_path)
            logger.info(
                "Fundamental refresh complete for all watchlist symbols"
            )
        finally:
            spark.stop()

    refresh_fundamentals()


fundamental_data_refresh()
