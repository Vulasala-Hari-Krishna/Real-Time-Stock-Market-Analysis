"""Daily batch aggregation DAG.

Reads silver-layer data, computes daily indicators / signals / sector
rollups, then enriches with fundamental data and writes everything to
the gold layer.  Runs after the historical backfill DAG completes.

Spark jobs are submitted to the standalone Spark cluster via
``spark-submit`` — Airflow only orchestrates.

Schedule: weekdays at 07:00 UTC.
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
    dag_id="daily_batch_aggregation",
    description="Compute daily indicators, enrichment, and gold-layer outputs",
    schedule="0 7 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["batch", "aggregation"],
)
def daily_batch_aggregation() -> None:
    """Orchestrate daily aggregation, enrichment, and validation."""

    run_aggregation = BashOperator(
        task_id="run_aggregation",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/daily_aggregation.py",
            extra_args="--mode daily",
        ),
    )

    run_enrichment = BashOperator(
        task_id="run_enrichment",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/fundamental_enrichment.py",
            extra_args="--mode weekly",
        ),
    )

    @task()
    def validate_gold_layer() -> None:
        """Validate that gold-layer tables exist in S3.

        Uses boto3 to check each gold output has at least one object.

        Raises:
            ValueError: If any gold table is empty.
        """
        import boto3

        from src.config.settings import get_settings

        settings = get_settings()
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_default_region,
        )

        tables = [
            "daily_summaries",
            "sector_performance",
            "correlations",
            "fundamentals",
            "enriched_prices",
        ]
        empty_tables: list[str] = []
        for table in tables:
            response = s3.list_objects_v2(
                Bucket=settings.s3_bucket_name,
                Prefix=f"gold/{table}/",
                MaxKeys=1,
            )
            count = response.get("KeyCount", 0)
            if count == 0:
                empty_tables.append(table)
            logger.info("Gold/%s: %s", table, "OK" if count else "EMPTY")

        if empty_tables:
            raise ValueError(
                f"Gold layer validation failed — empty tables: {empty_tables}"
            )
        logger.info("All gold tables validated")

    run_aggregation >> run_enrichment >> validate_gold_layer()


daily_batch_aggregation()
