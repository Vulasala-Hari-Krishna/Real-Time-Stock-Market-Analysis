# Stock Market Analytics: Copilot Instructions

## Status and Scope

- This is an existing personal portfolio project, not a new scaffold. Implement the hybrid migration incrementally on `feature/databricks_snowflake_impl`; do not switch branches or commit unless requested.
- The architecture below is the agreed target, not a claim that its integrations already exist. Inspect the touched code and tests before each step. Keep the existing local workflow operational until its replacement is implemented and verified.
- The current baseline is documented in [README.md](../README.md): local Kafka and Spark streaming write silver Parquet to S3; local Spark batch jobs write gold Delta tables; local Airflow submits jobs; Streamlit reads S3. Databricks notebooks are exploratory, not deployed pipelines.
- Do not interpret the old local-only/free-services design as a prohibition on Databricks or Snowflake. Equally, do not provision paid resources or activate recurring cloud workloads without explicit authorization.

## Agreed Hybrid Architecture

```text
Local API producer -> local Kafka -> local consumer -> S3 raw landing
                                              |-> local live-data cache (planned)
Local historical/fundamental fetchers -----------> S3 raw landing
S3 landing -> triggered Databricks -> Delta bronze -> silver -> gold
Delta gold -> immutable S3 exports + completed manifest -> Snowflake -> SQL marts
Local Streamlit -> local live-data cache / Snowflake historical analytics
Local Airflow -> Databricks job API -> publication validation -> Snowflake SQL
```

| Boundary | Ownership |
|----------|-----------|
| Local Docker | Kafka, producer, consumer, Airflow/PostgreSQL, Streamlit. Retain local Spark for existing workloads during migration. |
| Local source fetchers | API requests, credentials, rate limits, historical and fundamental extraction. Verify provider quotas; polling frequency does not guarantee live market data. |
| AWS S3 | Durable landing data, lakehouse storage, checkpoints, and publishing artifacts in separate prefixes. No managed Kafka or Airflow. |
| Databricks | Canonical validation, quarantine, deduplication, OHLCV rollups, indicators, and reusable Delta data products governed by Unity Catalog. |
| Snowflake | Native replicated tables, dimensional models, screening/reporting SQL, RBAC, and historical dashboard serving. Do not recalculate indicators already owned by Databricks. |
| Orchestration | Local Airflow owns cross-platform sequencing. Databricks Jobs own internal dependencies. Start manual-only with cloud schedules disabled. |

- S3 is the local/cloud boundary. Use outbound authenticated connections; do not expose local Kafka or Airflow to the internet or make Databricks depend on Docker hostnames.
- Preserve source messages and Kafka topic/partition/offset before canonical cleansing. Consumer-landed events are the target stream source; keep producer API backups separate to prevent double ingestion.
- A laptop outage stops local capture and scheduling. Persist local state and resume from durable checkpoints; do not claim recovery of market events never captured.
- Describe live/local and batch/cloud freshness separately. The default cloud path is on demand, later optionally daily, not continuous real-time analytics.

## Data Contracts and Delivery

- Target bronze, silver, and gold are Delta tables, not ordinary Parquet directories. Keep raw landing files separate from bronze Delta storage. Existing silver Parquet remains supported during migration.
- Start the Snowflake handoff with complete snapshots of selected small gold datasets. Export through Delta APIs to immutable per-run paths; write a completed manifest last with dataset/schema identity, source table versions, file list, and row counts.
- Load only completed exports into staging using `COPY INTO`; validate before transactional publication and record successful batch IDs for replay protection. Never scan Delta's physical Parquet files as a substitute for reading its transaction log.
- Reuse business keys such as `(symbol, date)`. Keep historical corrections, deletions, indicator warm-up periods, and snapshot consistency explicit. Existing latest-date-only merges are not sufficient for arbitrary historical corrections.
- Later CDC is a separate, opt-in phase: Delta Change Data Feed -> immutable change exports -> Snowpipe -> Snowflake Streams/Tasks -> ordered `MERGE`. Define bootstrap boundaries, durable progress, out-of-order/replayed events, tombstones, and expired-retention recovery before enabling it.

## Cost and Security Defaults

- Minimize billed activity, not feature count. Use triggered Auto Loader (`AvailableNow`) and terminating Databricks jobs; skip cloud work when there is no new input. Do not start compute per quote or uploaded file.
- Start with one Snowflake X-Small warehouse shared by load/transform/query roles, short auto-suspend, timeouts, and resource monitors. Cache dashboard results; do not poll Snowflake continuously.
- Snowpipe, recurring Tasks, Dynamic Table refreshes, declarative pipelines, and other autonomous compute must be explicitly enabled and stoppable. Warehouse resource monitors do not cap serverless spending; budgets/alerts are not guaranteed hard spending limits.
- Prefer compatible AWS regions. Check workspace edition, runtime, connectivity, IAM, and prices before provisioning. Free Edition/trial availability is not a guarantee that the full architecture is supported or permanently free.
- Avoid MSK, MWAA, EMR, always-on EC2, and unnecessary networking services. Glue/Athena are optional legacy paths, not requirements of the new serving layer.
- Preserve producer controls (`RUN_PIPELINE`, `MAX_ITERATIONS`). Local Docker shutdown and AWS CloudFormation teardown do not stop Databricks jobs or Snowflake resources; cloud pause/cancel/suspend procedures must be implemented explicitly.
- No credentials in code, bundles, SQL, logs, or manifests. Use least-privilege IAM/Unity Catalog storage credentials, Snowflake storage integrations, supported service authentication, and local/CI secret stores. Do not use administrator identities for routine jobs.
- Do not expire active Delta files or checkpoints using S3 lifecycle rules. Separate raw/export retention from table-aware maintenance; account for noncurrent object versions and recovery needs. Existing S3 policies must be reviewed before reusing prefixes.

## Incremental Implementation Order

1. Foundation: document contracts, identify required accounts/capabilities, and prepare minimal storage/identity configuration. Preserve the local baseline.
2. Databricks vertical slice: ingest one landed dataset, register governed tables, run transformations on demand, and reconcile outputs.
3. Snowflake vertical slice: publish/load snapshots, create an analytical model, and connect one historical dashboard view.
4. Operations: local Airflow coordination, idempotent retries, quality gates, deployment automation, cost controls, and independent cloud shutdown.
5. Advanced demonstrations: CDC, Snowpipe, Streams/Tasks, declarative quality expectations, and recovery exercises. Keep them off by default; do not duplicate pipelines just to list technologies.

Complete only the requested step. Do not deploy resources, rewrite all jobs, or enable later phases as incidental follow-up work.

## Repository Conventions and Checks

- Reuse `src/`, `dags/`, `dashboards/`, tests, and existing utilities. Keep reusable transformations in Python modules. Planned `databricks/` deployment assets and `snowflake/` SQL assets should be added only when their implementation step needs them.
- AWS infrastructure remains CloudFormation-managed. Use versioned Databricks deployment bundles and versioned Snowflake SQL for their respective platform objects; do not require CloudFormation to manage all three platforms.
- Follow the applicable [scoped instructions](instructions/): Python, Spark, Airflow, Docker, CloudFormation, tests, Databricks, and Snowflake. Language and domain instructions apply together; keep them consistent.
- Preserve Python 3.11-compatible local code, type hints, Google-style public docstrings, Black (88), Ruff, mypy, Pydantic settings, structured logging, and environment-based configuration. Check managed-runtime compatibility separately.
- Test behavior changes first where practical. Keep default tests hermetic and cloud-free; use mocks for external services and opt-in integration tests for deployed platforms. Preserve the existing >=80% unit coverage gate.
- Run focused checks first. For relevant code changes use the existing `make lint` and `make test` gates (or their direct tool equivalents); `make validate-cfn` for AWS templates. Docs-only changes need instruction/frontmatter/link checks, not billable integration tests.
- Update documentation for changed behavior and clearly distinguish implemented features, planned features, and measured results. Do not claim cloud validation when only local/static checks ran.