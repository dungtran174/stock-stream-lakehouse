# End-to-End Architecture & Data Pipeline Flow

The Stock Stream Lakehouse implements a modern, cloud-native **Medallion Architecture (Bronze -> Silver -> Gold)** combining real-time stream ingestion and hourly batch ETL.

---

## 1. High-Level Architecture Flow

```text
[ Data Source ]
  Mock Stock Transactions (fake_data generator)
        │
        ▼
[ Ingestion API ]
  Flask REST API (GET /api/get_data)
        │
        ▼
[ Message Broker ]
  Apache Kafka (KRaft mode, topic: stock_transactions_YYYY_M_D)
        │
        ▼
[ Stream Processing Engine ]
  Apache Spark Streaming (PySpark on Spark Master + Worker)
        │
        ▼
[ Data Lakehouse Storage (Bronze Layer) ]
  Apache Iceberg REST Catalog + MinIO (S3-compatible)
  Table: iceberg.stocks.transactions (Raw Parquet, Partitioned by Year/Month/Day)
        │
        ▼
[ Batch Cleaning & Quality (Silver Layer) ]
  Spark Batch Job (clean_data.py): Null removal, deduplication, schema validation
  Table: iceberg.stocks.transactions_cleaned
        │
        ▼
[ Distributed SQL Query Engine & Mart (Gold Layer) ]
  Trino Coordinator + Catalog iceberg
  Schema: iceberg.stocks_reporting
  Tables: daily_market_summary, daily_stock_summary, daily_order_type_summary, daily_exchange_summary
        │
        ▼
[ Visualization & BI ]
  Apache Superset (Connected to Trino via sqlalchemy-trino)
```

---

## 2. Medallion Lakehouse Layers

### A. Bronze Layer (Raw Ingestion)
- **Table:** `iceberg.stocks.transactions`
- **Format:** Apache Iceberg with Parquet data files.
- **Partitioning Strategy:** `ARRAY['ts_year', 'ts_month', 'ts_day']`.
- **Purpose:** Append-only raw streaming ingestion preserving original event structure for auditing and replayability.

### B. Silver Layer (Cleaned & Conformed)
- **Table:** `iceberg.stocks.transactions_cleaned`
- **Format:** Apache Iceberg with Parquet.
- **Partitioning Strategy:** `ARRAY['ts_year', 'ts_month', 'ts_day']`.
- **Transformations:**
  - Filtering out records where `price` or `quantity` are null or NaN.
  - Removing corrupted strings (`order_type != '"NaN"'`, `exchange != '"NaN"'`).
  - Deduplicating transactions based on `Window.partitionBy("transaction_id").orderBy(col("ts").desc())`.

### C. Gold Layer (Business Aggregates)
- **Schema:** `iceberg.stocks_reporting`
- **Tables:**
  - `daily_market_summary`: Total trading volume and monetary value per date.
  - `daily_stock_summary`: Performance metrics aggregated by individual stock tickers and exchanges.
  - `daily_order_type_summary`: Buy vs. Sell order volume and frequency.
  - `daily_exchange_summary`: Trade volume distribution across stock exchanges.

---

## 3. Technology Stack Justification

| Technology | Category | Key Benefit in this Architecture |
| :--- | :--- | :--- |
| **Apache Kafka (KRaft)** | Streaming Broker | Decoupled from ZooKeeper, high-throughput log ingestion buffer. |
| **Apache Spark** | Processing Engine | Unified engine supporting both Structured Streaming (Kafka -> Iceberg) and Batch ETL (Bronze -> Silver). |
| **Apache Iceberg** | Lakehouse Table Format | ACID transactions, hidden partitioning, schema evolution, and time travel on top of object storage. |
| **MinIO** | Object Storage | S3-compatible, performant local object storage mimicking AWS S3. |
| **Trino** | Distributed SQL Engine | High-speed ANSI SQL query coordinator for federated queries on Iceberg tables. |
| **Apache Airflow** | Orchestrator | Enterprise pipeline scheduling, dependency management, and automated retries. |
| **Apache Superset** | BI Visualization | Fast, interactive dashboard creation connecting directly to Trino via SQLAlchemy. |
| **Docker & Compose** | Containerization | 100% reproducible local environment with isolated networks and volumes. |
