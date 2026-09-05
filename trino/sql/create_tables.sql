CREATE SCHEMA IF NOT EXISTS iceberg.stocks;

-- Bronze Layer: Raw stock transaction events ingested directly from Kafka
CREATE TABLE IF NOT EXISTS iceberg.stocks.transactions (
    transaction_id VARCHAR,
    ts TIMESTAMP,
    stock_symbol VARCHAR,
    price DOUBLE,
    quantity INT,
    order_type VARCHAR,
    exchange VARCHAR,
    ts_year INT,
    ts_month INT,
    ts_day INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ts_year', 'ts_month', 'ts_day']
);

-- Silver Layer: Cleaned and validated stock transaction records
CREATE TABLE IF NOT EXISTS iceberg.stocks.transactions_cleaned (
    transaction_id VARCHAR,
    ts TIMESTAMP,
    stock_symbol VARCHAR,
    price DOUBLE,
    quantity INT,
    order_type VARCHAR,
    exchange VARCHAR,
    ts_year INT,
    ts_month INT,
    ts_day INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ts_year', 'ts_month', 'ts_day']
);
