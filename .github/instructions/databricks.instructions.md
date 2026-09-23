---
applyTo: "databricks/**,notebooks/**/*.py"
description: "Use when implementing Databricks bundles, Lakeflow Jobs, Unity Catalog, Auto Loader, Delta tables, or S3 exports for the hybrid migration."
---

# Databricks Instructions

## Scope and Compute

- Follow [the repository migration plan](../copilot-instructions.md). `databricks/` is a planned deployment location; existing `notebooks/` are exploratory. Do not claim either is deployed without evidence.
- Databricks owns cloud lakehouse transformations, not the local Kafka connection or external market API polling. Read S3 landing inputs; no Docker hostnames or inbound laptop access.
- Start with one on-demand vertical slice and supported terminating job compute. Use Auto Loader with `AvailableNow` for file ingestion, durable checkpoints, and persisted schema state.
- Skip runs with no new input. Set finite timeouts, bounded retries, and concurrency limits. Keep schedules and file-arrival triggers disabled until requested; no compute startup per quote or file.
- Verify workspace edition, AWS region, storage access, runtime, and feature support before provisioning. Free Edition/trial limits are not a reliable substitute for this check. Benchmark serverless versus small classic job compute rather than assuming either is cheapest.

## Storage and Transformations

- Use Unity Catalog catalog/schema/table identifiers, least-privilege grants, storage credentials, and external locations. For the initial AWS S3 design, register explicitly located Delta tables under a dedicated lakehouse prefix; do not mix their paths with raw landing, export, checkpoint, or managed-storage roots.
- Keep bronze raw/replayable with source metadata; silver validates types, quarantines rejected rows, deduplicates, and computes canonical bars; gold computes reusable indicators and summaries. Reuse tested modules from `src/` rather than copying business logic into notebooks.
- Do not drop invalid messages locally before the canonical bronze capture. Keep Kafka identity and event/ingestion timestamps for audit and replay; define separate business keys for market records.
- Use runtime-provided Spark/Delta and supported Unity Catalog access; do not install conflicting standalone JARs, copy local Spark catalog overrides, or pass static S3 keys into jobs.
- Do not rely on row order or arbitrary `dropDuplicates` for competing corrections. Define deterministic precedence, event-time rules, source priority, and indicator warm-up/recompute windows.
- Use quality checks and quarantine first; add Lakeflow declarative pipelines/expectations as an explicit later step, not a second parallel implementation of the same transformation.
- Pin table features to capabilities required by approved readers. Do not enable protocol upgrades automatically while the existing lightweight Delta dashboard reader still accesses those tables.

## Publishing and Operations

- Start with complete snapshots of selected small gold tables. Read through Delta APIs at recorded versions, write immutable per-run Parquet exports, then publish the manifest only after all writes and checks succeed.
- Record dataset identity, schema version, logical batch/run ID, source table/version map, file list, and expected row counts. Define a consistent batch boundary across tables; independent Delta commits do not provide an automatic cross-table transaction.
- Snowflake must read the export contract, never Delta's physical data or change-file directories. Keep canonical table storage independent of the external publishing format.
- Enable Change Data Feed only for the later CDC phase using the selected runtime's supported APIs. Document snapshot bootstrap, version checkpoints, update/delete handling, and retention/rebootstrap behavior. CDF is not permanent history.
- Keep checkpoints outside lifecycle expiry; schedule Delta maintenance only when justified and safe for recovery. Inspect existing S3 policies before selecting table paths.
- Package reusable code and version deployment bundles with environment-specific non-secret variables and service identities. Local Airflow triggers jobs and tracks run IDs; Databricks controls internal task dependencies.
- Validate bundle/configuration syntax locally where possible. Commands requiring workspace authentication must be identified as such; deployment and billable run commands require authorization. Record run status, input/output counts, rejected records, source versions, and measured cost without exposing secrets.