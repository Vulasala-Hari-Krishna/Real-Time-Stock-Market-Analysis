# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Start here

This repo's migration from a fully local Kafka/Spark/Airflow pipeline to a
hybrid Databricks/Snowflake architecture is **complete and the legacy local
pipeline has been retired** (2026-09-28, branch `feature/databricks_snowflake_impl`).
Before making changes:

1. Read [docs/implementation-handover.md](docs/implementation-handover.md) — the
   model-independent progress ledger (status per workstream, what's actually
   verified vs. just written, current gate, next task, blockers). Do not trust
   prior chat history over this file.
2. Read [.github/copilot-instructions.md](.github/copilot-instructions.md) — the
   canonical repository instructions (target architecture, ownership boundaries,
   cost/security defaults, implementation order). It also links scoped
   per-domain instructions in [.github/instructions/](.github/instructions/)
   (python, spark-jobs, databricks, snowflake, airflow-dags, docker,
   cloudformation, tests, cursor rules) — follow the ones matching the files
   you touch.
3. Update the handover ledger before ending a session: status table, checks
   actually run, blockers, and a concrete next task.

Key facts from the ledger you should not re-derive from scratch:
- Do not switch branches or commit unless the user asks.
- **The architecture is now Kafka (local) → Databricks → Snowflake → Airflow
  (local) → Streamlit — nothing else.** The local Spark cluster, its batch
  jobs (`tick_rollup.py`, `daily_aggregation.py`'s standalone execution,
  `fundamental_enrichment.py`, `delta_maintenance.py`), the legacy Airflow
  DAGs that `spark-submit`ted them, and the AWS Glue/Athena querying stack
  have all been removed. A handful of legacy-named modules survive as
  **library code** genuinely reused by Databricks jobs (see below) — do not
  assume every legacy-named file is gone, and do not assume every
  legacy-named file is still a runnable script.
- Every Databricks job, Airflow DAG, and Snowflake dataset has been deployed
  and live-verified with real data at least once. `maintain_delta_tables`
  ships with a paused native schedule; every other Databricks job is
  triggered by an Airflow DAG. See the handover ledger's change log for the
  full verification history.
- Never provision paid/recurring cloud resources, start cloud jobs, or run
  destructive cleanup without explicit user authorization. Full teardown is a
  requirement of this project but must be explicitly confirmed each time, not
  inferred.

## Commands

```bash
# Setup
make setup              # pip install -r requirements.txt -r requirements-dev.txt

# Local stack (Docker Compose) - Kafka, raw-landing consumer, Airflow, Streamlit
make start               # start all local services
make stop                # stop all local services
make demo                # MAX_ITERATIONS=5 docker compose up (quick demo)
make logs                # tail logs from all services

# Tests
make test                                                              # pytest tests/unit, coverage >=80% gate
pytest tests/unit -v --cov=src --cov-report=term-missing --cov-fail-under=80
pytest tests/unit/test_some_module.py::test_case -v                    # single test
pytest tests/integration -v -m integration                            # opt-in, gated

# Lint / format / typecheck
make lint                # ruff check src/ tests/ dags/ dashboards/ ; mypy src/ dags/ --ignore-missing-imports
make format              # black + ruff --fix, same scope

# AWS CloudFormation (S3 data lake + IAM, stacks 01/03)
make validate-cfn        # cfn-lint cloudformation/*.yaml
make deploy              # deploy stacks 01/03 — do not run without authorization
make teardown            # destroy stacks 01/03 — do not run without authorization
make deploy-hybrid       # deploy hybrid-access stacks 05/06 — do not run without authorization
make teardown-hybrid     # destroy stacks 05/06 (run before make teardown) — do not run without authorization

# Databricks bundle
python -m pip wheel --no-deps --wheel-dir databricks/dist ./databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev     # do not run without authorization

# Terraform (databricks/terraform/{credential,workspace}, snowflake/terraform/{bootstrap,workspace})
terraform init -backend=false -input=false -lockfile=readonly
terraform fmt -check
terraform validate
terraform test            # uses mock providers — not a real apply
```

Always run the focused test/lint slice for the files you touched before the
full gate. Docs-only changes need link/frontmatter checks, not a full test run.

## Architecture

```
Alpha Vantage -> Kafka producer -> Kafka broker
                                       |
                                       v
                          raw_landing.py (plain Python, continuous,
                          gzip NDJSON envelopes, SQLite spool)
                                       |
                                       v
                          S3 landing/ticks/  <-- read directly by the
                                       |         dashboard's Live Data page
                                       v
              Databricks Auto Loader (AvailableNow, triggered by Airflow every 15 min)
                                       |
                      bronze -> silver -> gold  (Delta Lake, Unity Catalog)
                       landed_ticks -> daily_quote_summary
                       landed_ticks_rollup (daily) + landed_historical (manual) -> historical_ohlcv
                       landed_indicators (daily) -> daily_summaries/sector_performance/correlations
                       landed_fundamentals (weekly) -> fundamentals
                                       |
                                       v
              Immutable S3 snapshot export + manifest (src/export/gold_snapshot.py)
                                       |
                                       v
       Snowflake: COPY INTO staging -> atomic DELETE+INSERT swap into SERVING.*
       (src/load/snowflake_snapshot.py)
                                       |
                                       v
           Streamlit (dashboards/snowflake_loader.py + landing_reader.py)
                       <- SERVING.* (Snowflake) + S3 landing/ticks/ (live)

Local Airflow (dags/databricks_*.py) triggers every Databricks job above via
the Jobs API, then runs the export/load step - it does no computation itself.
```

- `src/producers/stock_producer.py` — polls Alpha Vantage every 60s, publishes to Kafka. Respects `RUN_PIPELINE` / `MAX_ITERATIONS` kill switches — preserve these. No longer backs up to S3 itself (that was a legacy-only, unread side effect; removed).
- `src/consumers/raw_landing.py` — the sole bridge from Kafka into the hybrid pipeline (writes `landing/ticks/`), continuous, no Docker Compose profile gate. At-least-once delivery — downstream (Databricks) dedupes on `(source_id, topic, partition, offset)`.
- `src/batch/databricks_*.py` / `landed_*.py` — Databricks job runners and pure Spark transforms, deployed via the Asset Bundle in `databricks/`.
- `src/batch/daily_aggregation.py` and `src/batch/historical_backfill.py` — **legacy-named but not legacy**: kept as library code because `databricks_indicators.py` imports the former's transform functions directly, and `landed_historical.py` imports the latter's `download_history` directly. Their own legacy standalone-script/CLI entry points are unreachable now (the Spark cluster and Airflow DAGs that ran them are gone) — treat them as importable modules, not runnable jobs.
- `src/common/schemas.py`, `src/common/s3_utils.py` — shared Pydantic models and S3 helpers, reused by both the kept producer/consumer and Databricks/Snowflake code.
- `src/config/settings.py` — single source of Pydantic env-var config, including `AWS_DEFAULT_REGION`; don't hardcode region/bucket elsewhere.
- `dags/databricks_pipeline_common.py` — shared trigger/export/load helper every Airflow DAG uses; keeps the scheduler lightweight (it calls the Databricks Jobs API and `src/export/`+`src/load/`, never runs Spark itself).
- Gold/serving tables are deduplicated on business keys documented in [README.md](README.md#business-keys) per table.
- `dashboards/snowflake_loader.py` — generic, dataset-registry-driven Snowflake loader (`daily_quote_summary`, `daily_summaries`, `sector_performance`, `correlations`, `fundamentals`), each with a local-cache fallback (`dashboards/local_cache.py`) and a synthetic demo fallback — never present demo/cached data as real without an explicit `LoadStatus` banner (`dashboards/load_status.py`).
- `dashboards/landing_reader.py` — the Live Data page's source: decodes S3 `landing/ticks/` envelopes directly, no Databricks/Snowflake round trip.

### Cross-cutting rules worth remembering

- Business keys reuse `(symbol, date)`-style composites (see [README.md#business-keys](README.md#business-keys)); indicator warm-up windows and historical corrections require recomputing affected ranges, not just the latest date.
- Bronze/raw data must retain source identity (Kafka topic/partition/offset, ingestion vs. event time) for audit/replay; never drop it before canonical validation.
- Delta tables are read through Delta APIs/transaction log, never as raw Parquet directories.
- No credentials/bucket names/workspace URLs in code, logs, SQL, or manifests — configuration flows through `src/config/settings.py`, job parameters, or Airflow Connections, never hardcoded.
- Coverage gate is `--cov-fail-under=80`; unit tests must stay hermetic and cloud-free (mock Kafka/S3/Databricks/Snowflake clients); real integration tests live under `tests/integration/` and are opt-in/gated.
- GitHub Actions only ever deploys/updates/tears down infrastructure — it never triggers a business-data job run. The one documented exception is `maintain_delta_tables --drop-tables` during teardown, since that job is infrastructure housekeeping, not business logic.
