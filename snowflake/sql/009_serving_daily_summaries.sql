-- Published, queryable output of the daily_summaries snapshot loader (R6).
-- Replaced transactionally (DELETE + INSERT, never TRUNCATE/CREATE OR
-- REPLACE inside it - see src/load/snowflake_snapshot.py::publish_serving)
-- so a failed load leaves the previous snapshot intact. Business key is
-- (symbol, date), matching the legacy product's documented MERGE key.
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.DAILY_SUMMARIES (
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
    source_version      NUMBER(38, 0),
    loaded_batch_id     VARCHAR NOT NULL,
    loaded_at           TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT daily_summaries_pk PRIMARY KEY (symbol, date)
);
