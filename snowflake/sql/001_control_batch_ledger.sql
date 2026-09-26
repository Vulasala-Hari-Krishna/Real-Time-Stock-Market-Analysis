-- Batch ledger: the durable, business-level replay/idempotency record for the
-- Snowflake snapshot loader (src/load/snowflake_snapshot.py). COPY load
-- history alone is not a durable ledger per the Snapshot Load Contract
-- (.github/instructions/snowflake.instructions.md) - this table is the
-- source of truth for "already loaded" / "stale" / "failed" decisions.
-- Idempotent: safe to re-run every load (CREATE TABLE IF NOT EXISTS), never
-- CREATE OR REPLACE - this table must never be silently recreated empty.
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.BATCH_LEDGER (
    dataset            VARCHAR NOT NULL,
    batch_id           VARCHAR NOT NULL,
    source_table       VARCHAR NOT NULL,
    source_version     NUMBER NOT NULL,
    manifest_row_count NUMBER NOT NULL,
    status             VARCHAR NOT NULL, -- 'loading' | 'completed' | 'failed'
    loaded_row_count   NUMBER,
    started_at         TIMESTAMP_NTZ NOT NULL,
    completed_at       TIMESTAMP_NTZ,
    error_message      VARCHAR,
    CONSTRAINT batch_ledger_pk PRIMARY KEY (dataset, batch_id)
);
