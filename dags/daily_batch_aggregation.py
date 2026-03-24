"""Daily batch aggregation DAG.

Reads silver-layer data, computes daily indicators / signals / sector
rollups, then enriches with fundamental data and writes everything to
the gold layer.  Runs after the historical backfill DAG completes.

Schedule: daily at 07:00 UTC.
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
    dag_id="daily_batch_aggregation",
    description="Compute daily indicators, enrichment, and gold-layer outputs",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "aggregation"],
)
def daily_batch_aggregation() -> None:
    """Orchestrate daily aggregation, enrichment, and validation."""

    @task()
    def run_aggregation() -> None:
        """Run daily aggregation pipeline (indicators + signals + sector rollups).

        Reads from the silver layer, computes all technical indicators,
        and writes gold-layer Parquet outputs to S3.
        """
        from src.batch.daily_aggregation import (
            create_spark_session,
            run_daily_aggregation,
            write_gold_outputs,
        )
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session()
        try:
            bucket = settings.s3_bucket_name
            silver_path = f"s3a://{bucket}/silver/historical"
            gold_path = f"s3a://{bucket}/gold"

            outputs = run_daily_aggregation(spark, silver_path)
            write_gold_outputs(outputs, gold_path)
            logger.info("Daily aggregation complete")
        finally:
            spark.stop()

    @task()
    def run_enrichment() -> None:
        """Run fundamental enrichment pipeline.

        Fetches latest fundamentals from yfinance, joins with silver
        price data, and writes enriched output to the gold layer.
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
            logger.info("Fundamental enrichment complete")
        finally:
            spark.stop()

    @task()
    def validate_gold_layer() -> None:
        """Validate that gold-layer tables are populated.

        Reads each gold output and checks that at least one row exists.

        Raises:
            ValueError: If any gold table is empty.
        """
        from src.batch.daily_aggregation import create_spark_session
        from src.config.settings import get_settings

        settings = get_settings()
        spark = create_spark_session(app_name="GoldValidation")
        try:
            bucket = settings.s3_bucket_name
            gold_path = f"s3a://{bucket}/gold"

            tables = [
                "daily_summaries",
                "sector_performance",
                "correlations",
                "fundamentals",
                "enriched_prices",
            ]
            empty_tables: list[str] = []
            for table in tables:
                try:
                    df = spark.read.parquet(f"{gold_path}/{table}")
                    count = df.count()
                    if count == 0:
                        empty_tables.append(table)
                    logger.info("Gold/%s: %d rows", table, count)
                except Exception:
                    logger.exception("Failed to read gold/%s", table)
                    empty_tables.append(table)

            if empty_tables:
                raise ValueError(
                    f"Gold layer validation failed — empty tables: {empty_tables}"
                )
            logger.info("All gold tables validated")
        finally:
            spark.stop()

    agg = run_aggregation()
    enrich = run_enrichment()
    validate = validate_gold_layer()

    agg >> enrich >> validate


daily_batch_aggregation()
