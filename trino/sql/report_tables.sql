CREATE SCHEMA IF NOT EXISTS iceberg.stocks_reporting;

-- Gold Layer: Daily Market Summary
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_market_summary (
    report_date DATE,
    total_volume BIGINT,
    total_value DOUBLE,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

-- Insert statement for Daily Market Summary (from Silver layer)
INSERT INTO iceberg.stocks_reporting.daily_market_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions_cleaned
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE);

-- Gold Layer: Daily Stock Summary
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_stock_summary (
    report_date DATE,
    stock_symbol VARCHAR,
    exchange VARCHAR,
    total_volume BIGINT,
    total_value DOUBLE,
    transaction_count BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

-- Insert statement for Daily Stock Summary (from Silver layer)
INSERT INTO iceberg.stocks_reporting.daily_stock_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    stock_symbol,
    exchange,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    COUNT(transaction_id) AS transaction_count,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions_cleaned
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), stock_symbol, exchange;

-- Gold Layer: Daily Order Type Summary
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_order_type_summary (
    report_date DATE,
    order_type VARCHAR,
    order_count BIGINT,
    total_volume BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

-- Insert statement for Daily Order Type Summary (from Silver layer)
INSERT INTO iceberg.stocks_reporting.daily_order_type_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    order_type,
    COUNT(transaction_id) AS order_count,
    SUM(quantity) AS total_volume,
    ts_year AS year,
    ts_month AS month
FROM
    iceberg.stocks.transactions_cleaned
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), order_type;

-- Gold Layer: Daily Exchange Summary
CREATE TABLE IF NOT EXISTS iceberg.stocks_reporting.daily_exchange_summary (
    report_date DATE,
    exchange VARCHAR,
    total_volume BIGINT,
    total_value DOUBLE,
    transaction_count BIGINT,
    year INT,
    month INT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month']
);

-- Insert statement for Daily Exchange Summary (from Silver layer)
INSERT INTO iceberg.stocks_reporting.daily_exchange_summary
SELECT
    CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE) AS report_date,
    exchange,
    SUM(quantity) AS total_volume,
    SUM(CAST(price AS DOUBLE) * quantity) AS total_value,
    COUNT(transaction_id) AS transaction_count,
    year,
    month
FROM
    iceberg.stocks.transactions_cleaned
WHERE
    ts_year = year(CURRENT_DATE) AND
    ts_month = month(CURRENT_DATE) AND
    ts_day = day(CURRENT_DATE)
GROUP BY
    ts_year, ts_month, CAST(format_datetime(ts, 'yyyy-MM-dd') AS DATE), exchange;
