---
applyTo: "dags/**/*.py"
description: "Use when changing local Airflow DAGs, Databricks job orchestration, Snowflake loads, retries, or hybrid pipeline schedules."
---

# Airflow DAG Instructions

- Follow the migration boundaries in [the repository instructions](../copilot-instructions.md). Airflow and its metadata PostgreSQL stay in local Docker; do not add MWAA or a cloud scheduler.
- Use `@dag`/`@task` for Python orchestration and supported provider operators/hooks for platform calls. Pin providers compatible with the installed Airflow version; do not assume the latest provider supports Airflow 2.8.
- Every DAG needs `dag_id`, `description`, `schedule`, a timezone-aware fixed `start_date`, `catchup=False`, `max_active_runs=1`, meaningful task IDs, and tags. New hybrid/cloud DAGs start with `schedule=None` and paused; do not silently disable existing local schedules.
- Set bounded `retries`, `retry_delay`, and execution/poll timeouts. Prevent overlapping billable runs and retry storms.
- Keep business transformations in reusable modules, Databricks tasks, or versioned Snowflake SQL. Never run Spark computation in the Airflow scheduler or at DAG import time.
- Preserve current `spark-submit` DAGs until the corresponding remote job is verified. Trigger an existing Databricks job for migrated work instead of submitting to the local Spark master.
- The target parent workflow checks for new input, starts or reconnects to a Databricks run, validates the completed export manifest, loads Snowflake staging, publishes validated tables/marts, and reconciles results.
- Store durable logical batch and remote run IDs; use supported idempotency tokens when submitting jobs. A retry or scheduler restart must monitor the same run rather than launch duplicate cloud work.
- Use XCom only for small identifiers/metadata, not DataFrames, credentials, or full datasets. Read platform settings through configuration and credentials through secured Airflow Connections or supported secret stores.
- Wait for actual remote completion and manifest readiness, not elapsed time. A bucket prefix containing an object is not a sufficient data quality check.
- Validate keys, row counts, schema, source versions, and freshness at data publication boundaries; do not proceed to serving when validation fails.
- Airflow owns cross-platform scheduling; Databricks Jobs own their internal task graph. Future Snowflake Tasks own only explicitly assigned warehouse-local work, not a duplicate full schedule.
- Include explicit cloud timeout/cancel/suspend handling and a documented manual recovery path. Airflow or laptop shutdown does not automatically cancel remote jobs; do not suspend a shared warehouse while other authorized work is using it.