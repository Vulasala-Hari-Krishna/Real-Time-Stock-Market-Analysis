-- Published, queryable output of the fundamentals snapshot loader (R6).
-- Replaced transactionally (DELETE + INSERT in one Snowflake transaction,
-- never TRUNCATE/CREATE OR REPLACE inside it - see
-- src/load/snowflake_snapshot.py::publish_serving) so a failed load leaves
-- the previous snapshot intact. Business key is (symbol) only: complete
-- replacement means a symbol dropped from a later snapshot is genuinely
-- gone, not left stale, per the Snapshot Load Contract's "complete
-- replacement handles removed rows as well as corrections."
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.FUNDAMENTALS (
    symbol                 VARCHAR NOT NULL,
    retrieved_at           TIMESTAMP_NTZ NOT NULL,
    market_cap             DOUBLE,
    pe_ratio               DOUBLE,
    forward_pe             DOUBLE,
    dividend_yield         DOUBLE,
    eps                    DOUBLE,
    beta                   DOUBLE,
    fifty_two_week_high    DOUBLE,
    fifty_two_week_low     DOUBLE,
    sector                 VARCHAR,
    industry               VARCHAR,
    bronze_version         NUMBER(38, 0),
    loaded_batch_id        VARCHAR NOT NULL,
    loaded_at              TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT fundamentals_pk PRIMARY KEY (symbol)
);
