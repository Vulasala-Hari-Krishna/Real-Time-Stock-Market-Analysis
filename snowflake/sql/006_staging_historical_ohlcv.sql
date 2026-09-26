-- Isolated staging table for the historical_ohlcv snapshot dataset (R6).
-- Loaded fresh from the exact manifest-listed files on every run (see
-- src/load/snowflake_snapshot.py::copy_into_staging); never read directly by
-- dashboards/marts - only the SERVING table (007) is the published output.
-- Business key is (symbol, date) - a time series, one row per trading day.
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.HISTORICAL_OHLCV_STAGING (
    symbol          VARCHAR NOT NULL,
    date            DATE NOT NULL,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          NUMBER(38, 0),
    source          VARCHAR,
    bronze_version  NUMBER(38, 0)
);
