---
applyTo: "tests/**/*.py"
description: "Use when adding unit, contract, or opt-in integration tests for local ingestion, Databricks, Snowflake, or Airflow migration steps."
---

# Testing Instructions

- Follow TDD for behavior changes: establish a failing focused check before implementation where practical. Preserve regression coverage for the existing local workflow throughout migration.
- Use pytest/pytest-cov and the existing >=80% unit coverage gate; maintain at least 80% coverage for new/changed Python modules. Do not lower or bypass coverage to accommodate cloud SDK imports.
- Unit tests must be fast, hermetic, and cloud-free. Mock API calls, Kafka/S3 clients, platform SDKs, connectors, orchestration polling, time, and cost-triggering operations. Use `tmp_path` for isolated artifact/manifest tests when filesystem behavior is the subject.
- Reuse `tests/conftest.py`, neighboring helpers, `unittest.mock`/pytest fixtures, and parametrization. Use `test_{module_name}.py`; avoid duplicating fixtures or creating a new test framework.
- Keep reusable Spark transformation tests independent of Databricks credentials. Follow existing Spark fixtures; do not assert business correctness only through mocked method-call chains when a small DataFrame test is feasible.
- Test source identity retention, malformed payload quarantine, duplicate input, late events, interrupted S3 writes, and write-success/checkpoint-failure replay when changing ingestion.
- Test manifest schema/files/row counts, incomplete export rejection, source-version consistency, duplicate business keys, empty snapshots, corrections, deletions, and repeated batch IDs when adding the snapshot handoff.
- Test staging failure leaves published data intact, publication/audit state commits together, and retries do not duplicate rows. Pure mocks do not prove Snowflake SQL or transaction semantics; label the remaining platform integration check.
- Test Airflow DAG import without network calls, manual cloud schedules, dependency/quality gates, no-input skips, bounded retries/timeouts, and reconnection to existing Databricks run IDs.
- When CDC is explicitly implemented, test the snapshot/change boundary, version ordering, duplicate/reordered delivery, tombstones, expired retention, and reconciliation after recovery. Do not treat Snowpipe filename deduplication as row-level exactly-once proof.
- Put external integration tests in `tests/integration/`. Docker and cloud tests must be explicitly selected and gated; skip unavailable environments clearly rather than making the default suite provision infrastructure.
- Cloud integration tests need explicit authorization, isolated test objects, bounded execution, non-secret logs, and cleanup/cancel/suspend safeguards. Never point destructive tests at shared or production data.
- Run the focused slice first, then relevant repository gates. Distinguish mocked/local validation from real Databricks/Snowflake execution in results. Instruction-only changes need frontmatter/link/static checks, not application coverage runs.