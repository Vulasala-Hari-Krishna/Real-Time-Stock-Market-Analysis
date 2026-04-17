"""One-time historical backfill DAG (manual trigger only).

Downloads 5 years of daily OHLCV data for every watchlist symbol via
yfinance, writes bronze JSON and silver Parquet to S3.  Validates that
all symbols were backfilled successfully.

The Spark job is submitted to the standalone Spark cluster via
``spark-submit`` — Airflow only orchestrates.

This DAG has **no schedule** — it is meant to run once when the
pipeline is first deployed (or when a full data refresh is needed).
Trigger it manually from the Airflow UI.

Schedule: None (manual trigger via Airflow UI).
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
    """Orchestrate full historical data backfill, gold-layer seed, and validation."""

    run_backfill = BashOperator(
        task_id="fetch_historical_data",
        bash_command=spark_submit_cmd("/opt/airflow/src/batch/historical_backfill.py"),
    )

    # After backfill, run aggregation in 'full' mode to seed gold Delta tables
    # with the complete 5-year indicator history.
    seed_gold = BashOperator(
        task_id="seed_gold_layer",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/daily_aggregation.py",
            extra_args="--mode full",
        ),
    )

    seed_fundamentals = BashOperator(
        task_id="seed_fundamentals",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/fundamental_enrichment.py",
            extra_args="--mode full",
        ),
    )

    @task()
    def validate_data() -> None:
        """Validate that all watchlist symbols were backfilled to S3."""
        import boto3

        from src.config.settings import get_settings
        from src.config.watchlist import SYMBOLS

        settings = get_settings()
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_default_region,
        )

        missing = []
        for symbol in SYMBOLS:
            response = s3.list_objects_v2(
                Bucket=settings.s3_bucket_name,
                Prefix=f"silver/historical/symbol={symbol}/",
                MaxKeys=1,
            )
            if response.get("KeyCount", 0) == 0:
                missing.append(symbol)

        if missing:
            raise ValueError(f"Backfill failed for {len(missing)} symbols: {missing}")
        logger.info("All %d symbols validated in silver layer", len(SYMBOLS))

    run_backfill >> validate_data() >> seed_gold >> seed_fundamentals


initial_historical_backfill()
