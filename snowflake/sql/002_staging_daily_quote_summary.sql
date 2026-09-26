-- Isolated staging table for the daily_quote_summary snapshot dataset.
-- Loaded fresh from the exact manifest-listed files on every run (see
-- src/load/snowflake_snapshot.py::copy_into_staging); never read directly by
-- dashboards/marts - only the SERVING table (003) is the published output.
-- Column set/order mirrors the gold table's actual output columns:
-- src/batch/landed_ticks.py::summarize_quotes plus the bronze_version
-- lineage column databricks_ticks.py::rebuild_outputs appends afterward
-- (confirmed live 2026-09-26 - not part of summarize_quotes itself) and
-- the exporter's DATASET_BUSINESS_KEYS in src/export/gold_snapshot.py;
-- update all of these together if the gold schema changes.
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.DAILY_QUOTE_SUMMARY_STAGING (
    provider               VARCHAR NOT NULL,
    symbol                 VARCHAR NOT NULL,
    capture_date_utc       DATE NOT NULL,
    first_observed_price   DOUBLE,
    highest_observed_price DOUBLE,
    lowest_observed_price  DOUBLE,
    last_observed_price    DOUBLE,
    last_reported_volume   NUMBER(38, 0),
    quote_count            NUMBER(38, 0) NOT NULL,
    first_quote_at         TIMESTAMP_NTZ,
    last_quote_at          TIMESTAMP_NTZ,
    observed_change_pct    DOUBLE,
    bronze_version         NUMBER(38, 0)
);
