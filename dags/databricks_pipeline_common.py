"""Shared building blocks for the R8 Databricks-triggering DAGs.

Mirrors ``spark_submit_config.py``'s role for the legacy Spark DAGs: a single
place that knows how to talk to the platform (there, ``spark-submit``; here,
the Databricks Jobs API and the already-tested export/load modules), so each
DAG only orchestrates task order.

Every trigger uses an ``idempotency_token`` derived from the Airflow run ID
and the job's own short name. If the same Airflow task attempt is retried
(a transient network/API error while submitting or polling, not a real job
failure), Databricks recognizes the token and reconnects to the existing run
instead of starting a duplicate one - this is what lets task-level retries be
safe here, unlike retrying the underlying business logic itself (each
Databricks job's own ``max_retries: 0`` in ``databricks.yml`` is deliberate,
for the same reason).

The token is passed via the ``json`` constructor argument, not the operator's
own ``idempotency_token=`` keyword - confirmed live 2026-09-27 that this
matters: ``DatabricksRunNowOperator``'s ``template_fields`` only lists
``("json", "databricks_conn_id")``, so a Jinja expression given to the named
``idempotency_token`` kwarg is merged into the request at execute() time
*after* Airflow's templating pass already ran, and is sent to Databricks as
the literal, unrendered string ``"{{ run_id }}-<job>"`` - identical on every
run, forever. Every trigger of a given job was silently reusing the same
token as a result, so every retry (and every later, genuinely new DAG run)
just replayed one very first run's cached result instead of executing
anything. Routing the token through ``json={"idempotency_token": ...}``
puts it inside the one field Airflow does render before execute().

No ``deferrable=True``: that mode needs an ``airflow triggerer`` service,
which this Docker Compose stack does not run today. Without it, each trigger
task blocks a worker slot for the job's full runtime - the same trade-off the
legacy DAGs already make via ``BashOperator``/``spark-submit``, not a new one
introduced here.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.decorators import task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from databricks_job_names import job_name
from src.config.settings import get_settings
from src.export.gold_snapshot import GoldExportConfig, run_export
from src.load.snowflake_snapshot import SnowflakeLoadConfig, run_load

logger = logging.getLogger(__name__)

DATABRICKS_CONN_ID = "databricks_default"


def trigger_databricks_job(
    task_id: str,
    job_short_name: str,
    timeout_seconds: int,
    retries: int = 2,
) -> DatabricksRunNowOperator:
    """Build an operator that triggers one deployed job and waits for it.

    Args:
        task_id: Airflow task id for this trigger.
        job_short_name: The job's bundle-relative name (see ``job_name``).
        timeout_seconds: Matches that job's own ``databricks.yml`` timeout,
            plus polling overhead. ``DatabricksRunNowOperator`` itself has no
            such parameter (only ``polling_period_seconds``, the interval
            between polls) - the actual wall-clock cap is Airflow's own
            ``BaseOperator.execution_timeout``, verified directly against the
            provider's real ``__init__`` signature (6.7.0), not assumed.
        retries: Airflow-level retries for the trigger/poll call itself, safe
            because of ``idempotency_token`` (see module docstring) - not a
            retry of the underlying job logic.
    """
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=job_name(job_short_name),
        # See module docstring: idempotency_token must go through `json`
        # (a templated field) to actually get its Jinja expression rendered -
        # the named idempotency_token= kwarg is not templated at all.
        json={"idempotency_token": f"{{{{ run_id }}}}-{job_short_name}"},
        execution_timeout=timedelta(seconds=timeout_seconds),
        retries=retries,
    )


@task
def export_and_load(dataset: str, producer_run_id: str) -> dict[str, object]:
    """Export one dataset's gold snapshot, then load it into Snowflake.

    Reuses ``run_export``/``run_load`` unchanged - both already enforce the
    manifest/reconciliation guarantees the R8 roadmap item describes
    (checksum re-derivation before a manifest is trusted; schema-drift and
    stale-version rejection; an honest 'failed' ledger row on error) - so this
    is thin orchestration, not a reimplementation. Raising is the
    reconciliation signal: Airflow marks the task (and downstream tasks)
    failed, exactly as it would for any other task exception.

    Args:
        dataset: A name registered in both DATASET_BUSINESS_KEYS
            (gold_snapshot.py) and DATASET_COLUMNS (snowflake_snapshot.py).
        producer_run_id: Non-secret identifier for this run - the Airflow
            ``run_id``, mirroring how the GitHub Actions equivalents pass
            their own ``github.run_id``.

    Returns:
        The load outcome as a plain dict, for XCom visibility in the UI.
    """
    settings = get_settings()

    manifest = run_export(
        GoldExportConfig(
            catalog=settings.databricks_catalog,
            schema_prefix=settings.databricks_schema_prefix,
            bucket=settings.s3_bucket_name,
            dataset=dataset,
        ),
        producer_run_id=producer_run_id,
    )

    result = run_load(
        SnowflakeLoadConfig(
            account=settings.snowflake_account,
            user=os.environ["SNOWFLAKE_USER"],
            role=settings.snowflake_loader_role,
            warehouse=settings.snowflake_warehouse,
            database=settings.snowflake_database,
            staging_schema=settings.snowflake_staging_schema,
            serving_schema=settings.snowflake_serving_schema,
            stage_name=settings.snowflake_stage_name,
            bucket=settings.s3_bucket_name,
            dataset=dataset,
        ),
        private_key_pem=os.environ["SNOWFLAKE_PRIVATE_KEY"],
        batch_id=manifest.batch_id,
        private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        or None,
    )
    logger.info(
        "export_and_load(%s): batch=%s status=%s rows_loaded=%d",
        dataset,
        result.batch_id,
        result.status,
        result.rows_loaded,
    )
    return {
        "dataset": dataset,
        "batch_id": result.batch_id,
        "status": result.status,
        "rows_loaded": result.rows_loaded,
    }
