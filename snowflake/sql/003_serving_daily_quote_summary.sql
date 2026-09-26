-- Published, queryable output of the daily_quote_summary snapshot loader.
-- Replaced transactionally (DELETE + INSERT in one Snowflake transaction,
-- never TRUNCATE/CREATE OR REPLACE inside it - see
-- src/load/snowflake_snapshot.py::publish_serving) so a failed load leaves
-- the previous snapshot intact, per the Snapshot Load Contract. Business key
-- matches the exporter's DATASET_BUSINESS_KEYS: (provider, symbol,
-- capture_date_utc).
CREATE TABLE IF NOT EXISTS {database}.{serving_schema}.DAILY_QUOTE_SUMMARY (
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
    loaded_batch_id        VARCHAR NOT NULL,
    loaded_at              TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT daily_quote_summary_pk PRIMARY KEY (provider, symbol, capture_date_utc)
);
