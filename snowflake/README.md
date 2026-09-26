# Snowflake Snapshot Loader (R4)

## Status

Implemented, not yet run against the live account. Platform IaC ([terraform/](terraform/))
is live and DONE (see [terraform/README.md](terraform/README.md)); this
covers the next piece - actually loading a published gold snapshot batch
into Snowflake, per the Snapshot Load Contract in
[.github/instructions/snowflake.instructions.md](../.github/instructions/snowflake.instructions.md).
Full unit suite (mocked Snowflake connector + mocked S3) passes; a real
`workspace` Terraform apply (to create the new `SERVING` schema) and a real
run of [load-snowflake-snapshot.yaml](../.github/workflows/load-snowflake-snapshot.yaml)
are both still outstanding - see the handover ledger's change log for the
concrete next action.

## Architecture

```text
S3 publish/batches/<batch_id>/manifest.json + daily_quote_summary/part-*.parquet
    -> src/load/snowflake_snapshot.py, authenticated as the least-privilege
       loader role (never ACCOUNTADMIN):
         1. find the latest batch with a *completed* manifest
         2. re-validate expected columns/business-keys, re-derive checksums
            (reuses src/export/gold_snapshot.verify_published_batch)
         3. consult PUBLISH_STAGING.BATCH_LEDGER: skip if already completed,
            reject if the source version is not newer than what's loaded
         4. COPY INTO PUBLISH_STAGING.DAILY_QUOTE_SUMMARY_STAGING from the
            exact manifest-listed files, casting every column explicitly
         5. validate staged row count + business-key uniqueness in Snowflake
         6. one transaction: DELETE + INSERT into
            SERVING.DAILY_QUOTE_SUMMARY, then mark the ledger row completed
```

`snowflake/sql/` holds the versioned, idempotent (`CREATE TABLE IF NOT
EXISTS`) DDL for all three tables - the loader executes these files
directly at the start of every run rather than duplicating the schema
definition in Python. Never `CREATE OR REPLACE` any of them: that would
implicitly commit and could silently empty a table the loader is supposed
to only append/replace transactionally.

Two schemas, one purpose split: `PUBLISH_STAGING` (disposable staging +
the durable `BATCH_LEDGER` control table) and `SERVING` (the one table
dashboards/marts should actually query). The loader role has `CREATE
TABLE` on both (see [terraform/workspace/main.tf](terraform/workspace/main.tf))
so it owns, and can freely DML, everything it creates - no separate
per-table grants needed.

## Required Decisions

- Column set/order for `daily_quote_summary` is hand-fixed in
  `DATASET_COLUMNS` (src/load/snowflake_snapshot.py), not derived from
  whatever a manifest happens to declare - a manifest with different
  columns/business-keys is rejected as schema drift, not silently loaded.
- Authentication is key-pair (JWT), the same `SNOWFLAKE_USER`/
  `SNOWFLAKE_PRIVATE_KEY` secrets the Terraform provider already uses - no
  new secret needed.
- `BATCH_LEDGER` (not COPY load history) is the durable idempotency/replay
  ledger, per the contract - COPY history alone is not treated as a
  business-level record.

## Offline Validation

```bash
python -m pytest tests/unit/test_snowflake_snapshot.py -v
python -m ruff check src/load/ tests/unit/test_snowflake_snapshot.py
python -m black --check src/load/ tests/unit/test_snowflake_snapshot.py
python -m mypy src/load/ --ignore-missing-imports
```

No live Snowflake account is exercised by these - they mock the DB-API
connection/cursor and the S3 client. The first real test is a live run of
`load-snowflake-snapshot.yaml` against a real published batch.
