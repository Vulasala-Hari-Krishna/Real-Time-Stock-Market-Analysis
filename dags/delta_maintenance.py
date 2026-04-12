"""Monthly Delta Lake maintenance DAG.

Runs OPTIMIZE (compaction) and VACUUM (stale file cleanup) on all
gold-layer Delta tables.  Scheduled every Sunday at 04:00 UTC, but
only executes on the **last Sunday of the month** — a
``ShortCircuitOperator`` skips the run on other Sundays.

The Spark job is submitted to the standalone Spark cluster via
``spark-submit`` — Airflow only orchestrates.
"""

from __future__ import annotations

import calendar
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

from spark_submit_config import spark_submit_cmd

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _is_last_sunday(**context: object) -> bool:
    """Return True only when the execution date is the last Sunday of its month."""
    logical_date: datetime = context["logical_date"]  # type: ignore[assignment]
    year, month = logical_date.year, logical_date.month
    # calendar.monthrange returns (weekday-of-first-day, number-of-days)
    last_day = calendar.monthrange(year, month)[1]
    # Walk back from the last day to find the last Sunday (weekday 6)
    for day in range(last_day, last_day - 7, -1):
        if calendar.weekday(year, month, day) == 6:  # Sunday
            is_last = logical_date.day == day
            logger.info(
                "Last Sunday of %d-%02d is day %d; execution day is %d → %s",
                year,
                month,
                day,
                logical_date.day,
                "RUN" if is_last else "SKIP",
            )
            return is_last
    return False  # pragma: no cover


@dag(
    dag_id="delta_maintenance",
    description="Monthly OPTIMIZE + VACUUM on gold-layer Delta tables (last Sunday)",
    schedule="0 4 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["maintenance", "delta-lake", "gold"],
)
def delta_maintenance() -> None:
    """Run Delta Lake maintenance and log results."""

    check_last_sunday = ShortCircuitOperator(
        task_id="check_last_sunday",
        python_callable=_is_last_sunday,
    )

    maintain = BashOperator(
        task_id="optimize_and_vacuum",
        bash_command=spark_submit_cmd(
            "/opt/airflow/src/batch/delta_maintenance.py",
        ),
    )

    @task()
    def log_completion() -> None:
        """Log that maintenance finished."""
        logger.info(
            "Delta maintenance spark-submit completed. "
            "Check Spark logs for per-table details."
        )

    check_last_sunday >> maintain >> log_completion()


delta_maintenance()
