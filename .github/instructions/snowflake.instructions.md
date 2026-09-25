---
applyTo: "snowflake/**"
description: "Use when implementing Snowflake SQL, storage integrations, snapshot loads, marts, warehouse controls, or later Snowpipe and Streams/Tasks CDC."
---

# Snowflake Instructions

## Scope and Cost

- Follow [the repository architecture](../copilot-instructions.md). `snowflake/` is the planned home for versioned SQL assets, not evidence that Snowflake objects already exist.
- Own SQL analytical models and historical serving, not raw Kafka ingestion or duplicate calculation of Databricks technical indicators. Start with native Snowflake tables fed by explicit S3 exports.
- Prefer the same supported AWS region as S3/Databricks. Use one X-Small standard warehouse initially, shared across separate load/transform/read roles, with short auto-suspend, query timeouts, and a resource monitor.
- Account for the warehouse start/resume minimum billing period; group load/transform/validation work and cache dashboard queries. Do not create separate always-running warehouses for a small personal workload.
- Default to Airflow-triggered `COPY INTO`, not autonomous ingestion. Snowpipe, recurring Tasks, Dynamic Tables, clustering services, and other ongoing compute remain disabled/uncreated until their explicit implementation phase.
- Resource monitors do not cap Snowpipe/serverless charges. Add budgets/usage visibility and explicit pipe/task/refresh pause controls when those features are added; do not promise a hard account-wide cost cap.

## Snapshot Load Contract

- Use a least-privilege storage integration and external stage restricted to `publish/`. Do not embed AWS credentials in SQL; complete the supported IAM trust/external-ID configuration explicitly.
- Load immutable, completed manifest-listed export files into isolated staging with an explicit schema and format. Never point `COPY INTO` or Snowpipe at raw Delta table directories.
- Track logical batch IDs, source table versions, file identity, row counts, and schema versions in audit/control tables. Reject incomplete exports, unexpected schema changes, and stale batches before publishing.
- Validate unique business keys, expected counts, required fields, and freshness. Snowflake standard-table primary/unique key declarations generally do not enforce uniqueness; use explicit checks and deterministic deduplication rules.
- For the first small complete-snapshot path, prepare objects/staging before publication, then use transactional DML to replace the selected replicated datasets and commit the success audit together. Do not put `CREATE OR REPLACE`, `TRUNCATE`, or other implicitly committing DDL inside the intended atomic publication transaction.
- Define empty-snapshot behavior and cross-table consistency. A validated empty dataset is not the same as a failed export; complete replacement must propagate removed rows as well as corrections.
- Make repeated batches safe, serialize publication, and reject older source versions. COPY load history alone is not a durable business-level replay ledger.
- Model reusable facts/dimensions and business-facing views without unnecessary materialization. Use role-separated loader, transformer, and dashboard access, not ACCOUNTADMIN for application sessions.
- Use supported service authentication, connection cleanup, parameterized values, and validated/quoted identifiers. Keep credentials outside scripts and repository assets.

## Later Features and Verification

- Add CDC only after snapshot correctness is established: Delta CDF exports -> landing table -> Snowflake Stream/Task -> deterministic `MERGE`. A Snowflake Stream tracks its Snowflake source; it does not read Delta CDF directly.
- Snowpipe can load files out of order. Deduplicate by source identity and commit/version ordering, ignore update preimages appropriately, and retain tombstone/version state so late inserts cannot resurrect deleted records.
- Keep a bootstrap/recovery protocol, retained exports, and progress ledger; do not rely on indefinite CDF, COPY-history, or Stream retention. Assign only one ingestion mechanism to a given file set to avoid duplicate loading.
- Use Dynamic Tables only for suitable derived SQL marts with an explicit refresh/cost policy. Do not duplicate the same transformation with both Dynamic Tables and Streams/Tasks.
- Demonstrate Time Travel and zero-copy cloning on isolated test objects with supported retention settings; clones can incur subsequent storage costs. Do not enable higher editions/features without checking availability and cost.
- Version repeatable setup/migration SQL; avoid destructive object recreation during routine deployment. Keep local/static validation separate from authorized live tests of SQL semantics, transactions, grants, and recovery.