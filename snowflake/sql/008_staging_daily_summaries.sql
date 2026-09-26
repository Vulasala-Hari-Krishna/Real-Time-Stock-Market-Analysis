-- Isolated staging table for the daily_summaries snapshot dataset (R6).
-- Column set mirrors src/batch/daily_aggregation.py::compute_daily_summaries'
-- output (reused directly by databricks_indicators.py, not re-implemented).
-- Business key is (symbol, date), matching the legacy daily_summaries
-- product's documented MERGE key (see README.md#merge-keys).
CREATE TABLE IF NOT EXISTS {database}.{staging_schema}.DAILY_SUMMARIES_STAGING (
    symbol              VARCHAR NOT NULL,
    date                DATE NOT NULL,
    open                DOUBLE,
    high                DOUBLE,
    low                 DOUBLE,
    close               DOUBLE,
    volume              NUMBER(38, 0),
    daily_return_pct    DOUBLE,
    sma_20              DOUBLE,
    sma_50              DOUBLE,
    sma_200             DOUBLE,
    ema_12              DOUBLE,
    ema_26              DOUBLE,
    rsi_14              DOUBLE,
    macd_line           DOUBLE,
    macd_signal         DOUBLE,
    macd_histogram      DOUBLE,
    volume_vs_avg       DOUBLE,
    sector              VARCHAR,
    signals             VARCHAR,
    source_version      NUMBER(38, 0)
);
