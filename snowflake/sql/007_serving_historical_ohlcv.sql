-- Published, queryable output of the historical_ohlcv snapshot loader (R6).
-- Replaced transactionally (DELETE + INSERT in one Snowflake transaction,
-- never TRUNCATE/CREATE OR REPLACE inside it - see
-- src/load/snowflake_snapshot.py::publish_serving) so a failed load leaves
-- the previous snapshot intact. Business key is (symbol, date).
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.HISTORICAL_OHLCV (
    symbol          VARCHAR NOT NULL,
    date            DATE NOT NULL,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          NUMBER(38, 0),
    source          VARCHAR,
    bronze_version  NUMBER(38, 0),
    loaded_batch_id VARCHAR NOT NULL,
    loaded_at       TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT historical_ohlcv_pk PRIMARY KEY (symbol, date)
);
