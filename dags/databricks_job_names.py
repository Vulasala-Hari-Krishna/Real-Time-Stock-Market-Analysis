"""Pure Databricks bundle job-name resolution - deliberately free of any
Airflow import.

Split out of ``databricks_pipeline_common.py`` so it can be unit-tested by
the main ``pytest`` suite without requiring ``apache-airflow`` in the dev
environment (that package only lives inside the Airflow Docker image - see
``docker/airflow/requirements.txt`` - a personal project's fast local test
loop shouldn't need Airflow installed just to check a string format).
"""

from src.config.settings import get_settings


def job_name(short_name: str) -> str:
    """Build a deployed Databricks bundle job's full name.

    Args:
        short_name: The job's bundle-relative name, e.g. ``"landed-ticks"``.

    Returns:
        The exact name ``databricks.yml`` deploys it under, e.g.
        ``"stock-market-hybrid-dev-landed-ticks"``.
    """
    settings = get_settings()
    return f"stock-market-hybrid-{settings.databricks_bundle_target}-{short_name}"
