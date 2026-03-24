---
applyTo: "dags/**/*.py"
---

# Airflow DAG Instructions

- Every DAG must have `dag_id`, `description`, `schedule_interval`, `start_date`, `catchup=False`
- Use `default_args` with `owner`, `retries`, `retry_delay`
- Use `@dag` and `@task` decorators (TaskFlow API) for clean DAG definitions
- Never put business logic directly in DAG files — import from `src/batch/` modules
- Use Airflow Variables or environment variables for configuration, never hardcode
- Set `max_active_runs=1` to prevent parallel execution of the same DAG
- Include a data quality check task in every data pipeline DAG
- Use meaningful task IDs that describe what the task does
- DAGs must be idempotent — safe to re-run without side effects
- Add `tags` to every DAG for filtering in the Airflow UI (e.g., `["batch", "backfill"]`)