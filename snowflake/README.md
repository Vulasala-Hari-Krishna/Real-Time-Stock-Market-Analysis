# Snowflake Snapshot Loader (R4) and Historical Dashboard View (R5)

Both the exporter (`src/export/gold_snapshot.py`) and this loader
(`src/load/snowflake_snapshot.py`) were built generic across datasets, not
hand-tied to `daily_quote_summary` - R6's `fundamentals` dataset (see
[docs/hybrid-migration.md](../docs/hybrid-migration.md#fundamentals-snapshot-contract-r6))
needed only a registry entry in each plus its own
`snowflake/sql/004_staging_fundamentals.sql` /
`005_serving_fundamentals.sql`, not a redesign.

## Status

**R4 is DONE and fully verified live** (loader ran successfully, row-level
content confirmed correct in `SERVING.DAILY_QUOTE_SUMMARY`/`BATCH_LEDGER`,
and a replay of the same batch was confirmed as an idempotent no-op). See
the handover ledger's change log for the full story, including one real bug
found and fixed live (a `bronze_version` lineage column missing from the
loader's schema registry).

**R5 (the Streamlit historical view) is implemented, not yet run against
the live account.** It reads `SERVING.DAILY_QUOTE_SUMMARY` read-only, as a
new `reader` account role separate from the R4 loader role - per the
Snapshot Load Contract's role-separation requirement. That role, and its
`SELECT`-only grants, were added to `workspace/main.tf` alongside R5's code
but have **not yet been applied live** - the next concrete action is a
`workspace` Terraform apply, then opening the dashboard's "Snowflake
History" page against the real account.

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

## Offline Validation (R4)

```bash
python -m pytest tests/unit/test_snowflake_snapshot.py -v
python -m ruff check src/load/ tests/unit/test_snowflake_snapshot.py
python -m black --check src/load/ tests/unit/test_snowflake_snapshot.py
python -m mypy src/load/ --ignore-missing-imports
```

No live Snowflake account is exercised by these - they mock the DB-API
connection/cursor and the S3 client.

## R5: Historical Dashboard View

`dashboards/snowflake_loader.py` queries `SERVING.DAILY_QUOTE_SUMMARY`
using the same key-pair (JWT) credentials as R4, but a **different,
read-only role** (`reader`, `SELECT`-only - see `workspace/main.tf`), so the
dashboard can never write/create objects even if compromised. Every load
returns an explicit `LoadStatus(source, ok, message, as_of, batch_id)`
alongside the data; `dashboards/pages/snowflake_history.py` renders that
status as a visible banner (`st.success` for real Snowflake data,
`st.error` with a `DEMO DATA` label on any failure) - per the R5 completion
criterion that a failed cloud connection must never masquerade as demo
success. On any connection/query error it falls back to deterministic demo
data (same local-dev convenience as the rest of `dashboards/`), always
paired with `ok=False`.

Required env vars for the dashboard container to reach the real account
(absent locally, which is fine - it then shows the demo banner):
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`; optional
`SNOWFLAKE_READER_ROLE`/`SNOWFLAKE_WAREHOUSE`/`SNOWFLAKE_DATABASE`/
`SNOWFLAKE_SERVING_SCHEMA` override the defaults matching this root's
Terraform output values.

### Offline Validation (R5)

```bash
python -m pytest tests/unit/test_snowflake_loader.py -v
python -m ruff check dashboards/
```

Headlessly smoke-tested via Streamlit's `AppTest` (no live Snowflake
account): with no `SNOWFLAKE_PRIVATE_KEY` set, the page renders with no
exceptions and shows the `DEMO DATA` banner - confirming the failure path
is safe, not just the happy path. The live-Snowflake path is unverified
until the `reader` role is applied and the dashboard is pointed at real
credentials.
