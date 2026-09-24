# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Start here

This repo is mid-migration from a fully local pipeline to a hybrid Databricks/Snowflake
architecture, on branch `feature/databricks_snowflake_impl`. Before making changes:

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
- The "legacy" architecture (README body below the migration-status banner) is
  the currently working local system — Kafka/Spark local pipeline writing to S3,
  local Airflow, Streamlit reading S3. Keep it operational; migration steps
  replace pieces of it incrementally, not all at once.
- `databricks/` contains an implemented-but-not-deployed vertical slice
  (Auto Loader bronze -> silver validation/quarantine/dedup -> gold daily
  sampled-quote summary) plus staged Terraform for platform IaC. Nothing has
  been deployed to a real Databricks workspace; Snowflake integration does not
  exist yet.
- Never provision paid/recurring cloud resources, start cloud jobs, or run
  destructive cleanup without explicit user authorization. Full teardown is a
  requirement of this project but must be explicitly confirmed each time, not
  inferred.

## Commands

```bash
# Setup
make setup              # pip install -r requirements.txt -r requirements-dev.txt

# Local stack (Docker Compose)
make start               # start all services (Kafka, Spark, Airflow, Streamlit)
make stop                # stop all services
make demo                # MAX_ITERATIONS=5 docker compose up (quick demo)
make logs                # tail logs from all services

# Tests
make test                                                              # pytest tests/unit, coverage >=80% gate
pytest tests/unit -v --cov=src --cov-report=term-missing --cov-fail-under=80
pytest tests/unit/test_some_module.py::test_case -v                    # single test
pytest tests/integration -v                                            # requires Docker services running

# Lint / format / typecheck
make lint                # ruff check src/ tests/ ; mypy src/ --ignore-missing-imports
make format              # black src/ tests/ ; ruff check --fix src/ tests/

# AWS CloudFormation
make validate-cfn        # cfn-lint cloudformation/*.yaml
make deploy              # deploy all CFN stacks — do not run without authorization
make teardown            # destroy all CFN stacks — do not run without authorization

# Databricks (staged, not yet CLI-validated in this environment)
python -m pip wheel --no-deps --wheel-dir databricks/dist ./databricks

# Terraform (per root: databricks/terraform/credential, databricks/terraform/workspace)
terraform init -backend=false -input=false -lockfile=readonly
terraform fmt -check
terraform validate
terraform test            # uses mock providers — not a real apply
```

Always run the focused test/lint slice for the files you touched before the
full gate. Docs-only changes need link/frontmatter checks, not a full test run.

## Architecture

### Local baseline (Lambda architecture, currently working)

```
Alpha Vantage -> Kafka producer -> Kafka -> Spark Structured Streaming -> S3 silver/stock_ticks (Parquet)
                        |-> raw backup -> S3 bronze/
Airflow DAGs -> spark-submit -> Spark cluster:
    tick_rollup (ticks -> daily OHLCV) -> S3 silver/historical
    daily_aggregation (indicators/signals/sectors/correlations) -> S3 gold/ (Delta Lake, MERGE)
    fundamental_enrichment (P/E, market cap via yfinance) -> S3 gold/ (Delta MERGE)
    delta_maintenance (OPTIMIZE + VACUUM, monthly)
Streamlit (deltalake reader, no Spark) <- S3 silver (live) + gold (historical)
```

- `src/producers/stock_producer.py` — polls Alpha Vantage every 60s, publishes to Kafka, backs up raw to S3 bronze. Respects `RUN_PIPELINE` / `MAX_ITERATIONS` kill switches — preserve these.
- `src/consumers/spark_streaming.py` — Spark Structured Streaming consumer, 30s micro-batches, validation/dedup/anomaly detection, writes silver Parquet.
- `src/batch/` — PySpark batch jobs run via Airflow `spark-submit` (`tick_rollup.py`, `daily_aggregation.py`, `fundamental_enrichment.py`, `delta_maintenance.py`, `historical_backfill.py`). Each has a daily/incremental mode (partition-pruned, MERGE into gold) and a full mode (one-time seed, overwrite).
- `src/common/` — shared indicator functions, S3 helpers, Pydantic schemas — reused by both local batch jobs and Databricks transforms.
- `src/config/settings.py` — single source of Pydantic env-var config, including `AWS_DEFAULT_REGION`; don't hardcode region/bucket elsewhere.
- `dags/spark_submit_config.py` — shared builder for the `spark-submit` command every DAG uses; keeps Airflow scheduler lightweight (BashOperator only, no heavy compute in the scheduler).
- Gold layer is Delta Lake; MERGE keys are documented in [README.md](README.md#merge-keys) per table (`daily_summaries`: symbol+date, `sector_performance`: sector+date, `correlations`: symbol_a+symbol_b+date, `fundamentals`: symbol, `enriched_prices`: symbol+date).
- Dashboard (`dashboards/`) reads gold Delta tables with the lightweight `deltalake` package (no Spark), falling back to Parquet then generated demo data if S3 is unreachable — never present demo data as real without saying so.

### Hybrid target (in progress — see handover ledger for real status)

```
Local producer -> local Kafka -> local raw consumer -> S3 landing/
S3 landing -> triggered Databricks Auto Loader -> bronze -> silver -> gold Delta (Unity Catalog)
Delta gold -> immutable S3 snapshot exports + completed manifest -> Snowflake -> SQL marts
Local Airflow -> Databricks job API -> publication validation -> Snowflake SQL
Local Streamlit -> local live cache / Snowflake historical analytics
```

- `databricks/` — wheel-task bundle (`databricks.yml`, `pyproject.toml`) plus `src/batch/databricks_ticks.py` / `landed_ticks.py` (runner + transforms) implementing the current Databricks slice: Auto Loader (`AvailableNow`) reads landed envelopes -> raw bronze Delta -> validate/quarantine/dedup -> silver `quote_samples` -> gold `daily_quote_summary`. This is sampled quotes, not exchange OHLCV — do not conflate with the legacy `daily_summaries` product.
- `databricks/terraform/` — two independent Terraform roots (`credential/`, `workspace/`) for platform IaC (fail-closed IAM role activated via generated external ID, then workspace/Unity Catalog objects). Ownership split: CloudFormation owns AWS; Terraform owns Databricks (and later Snowflake) platform objects; bundles own Databricks job code; versioned SQL will own Snowflake models. Give every object one owner — don't let CloudFormation and Terraform manage the same resource.
- `src/consumers/raw_landing.py` — opt-in raw consumer (disabled by default; enabled via `hybrid-raw` Compose profile) that preserves original Kafka bytes/offsets to S3 `landing/` with a durable SQLite spool, committing only after successful upload. At-least-once delivery — downstream (Databricks) must dedupe on `(source_id, topic, partition, offset)`.
- Snowflake integration, gold snapshot export/manifest, hybrid Airflow coordination, and local live-data cache are **not implemented yet** — see the roadmap table (R1–R11) in the handover ledger before assuming any of this exists.

### Cross-cutting rules worth remembering

- Business/upsert keys reuse `(symbol, date)`-style composites; indicator warm-up windows and historical corrections require recomputing affected ranges, not just merging the latest date.
- Bronze/raw data must retain source identity (Kafka topic/partition/offset, ingestion vs. event time) for audit/replay; never drop it before canonical validation.
- Delta tables are read through Delta APIs/transaction log, never as raw Parquet directories.
- No credentials/bucket names/workspace URLs in code, logs, SQL, or manifests — configuration flows through `src/config/settings.py`, job parameters, or Airflow Connections, never hardcoded.
- Coverage gate is `--cov-fail-under=80`; unit tests must stay hermetic and cloud-free (mock Kafka/S3/Databricks/Snowflake clients); real integration tests live under `tests/integration/` and are opt-in/gated.
