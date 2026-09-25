---
applyTo: "**/*.py"
description: "Use when writing Python application code, configuration, platform adapters, notebooks, or tests for the hybrid analytics pipeline."
---

# Python Code Instructions

- Keep local code Python 3.11-compatible, with type hints and Google-style public docstrings including relevant Args, Returns, and Raises. Check the selected Databricks runtime separately; do not assume local and cloud interpreters match.
- Preserve Black (88), Ruff, mypy, and existing import/naming conventions. Prefer simple functions and existing helpers over speculative platform abstractions.
- Use Pydantic v2 for configuration and boundary validation. Local application settings belong in `src/config/settings.py`; add only settings required for the current implementation step.
- Databricks job parameters/bundle variables and Airflow Connections may supply platform configuration through explicit adapters. Do not require local `.env` files or unrelated API credentials inside remote transformation jobs.
- Never hardcode deployment bucket names, workspace URLs, account identifiers, keys, or connection strings. Redact secrets from logs, exception context, SQL, and manifests.
- Keep domain transformations independent of cloud orchestration and transport. Package SDK/connector dependencies only for the components needing them; importing a module must not start network calls or billable work.
- Use `pathlib.Path` for local paths; handle S3 URIs and platform table identifiers with appropriate APIs rather than filesystem path manipulation. Use structured parsers for JSON/YAML/manifests.
- Use `logging`, not `print()` in production code. Scripts and exploratory notebooks may display results. Include non-secret batch/run/dataset identifiers for troubleshooting.
- Handle exceptions explicitly, use bounded timeouts and selective retries, and clean up connections with context managers. Do not retry non-idempotent cloud submissions blindly or suppress data-quality failures.
- Persist source event timestamps separately from ingestion/processing times. Use timezone-aware UTC for instants and an explicit exchange calendar/timezone when deriving trading dates.
- Keep API fetching/rate-limit handling local and preserve producer stop controls. Do not manufacture live-data claims or assume sampled quote volumes are per-trade increments.
- For the planned dashboard split, local live data and Snowflake historical results need explicit freshness/source indicators and caching. Do not silently present generated demo data as successful cloud output; keep demo fallback explicit.
- Preserve current APIs and local execution until a requested migration step replaces them with tested behavior. Apply the Spark, Databricks, Snowflake, Airflow, and test instructions alongside these language rules where relevant.