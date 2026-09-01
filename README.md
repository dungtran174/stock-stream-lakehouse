# LakeStream 🌊

A modern Data Lakehouse and Streaming architecture with Kafka, Spark Streaming, MinIO, Iceberg, Trino, Superset, Airflow, and Docker!

## Description

### Objective
The LakeStream project simulates a real-time data ingestion and processing pipeline. It consumes mock streaming data (e.g. user activity events) via a Flask API, ingests it into Kafka, and processes it using PySpark Streaming. The processed data is stored in a Data Lakehouse architecture using Apache Iceberg on MinIO (S3-compatible storage). We then use Trino as a distributed SQL query engine to serve data to Apache Superset for real-time dashboarding and analytics.

### Tools & Technologies
- Infrastructure & Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Message Broker - [**Kafka**](https://kafka.apache.org)
- Stream Processing - [**Spark Streaming**](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- Data Lakehouse Storage - [**Apache Iceberg**](https://iceberg.apache.org), [**MinIO**](https://min.io)
- Distributed Query Engine - [**Trino**](https://trino.io)
- Data Visualization - [**Apache Superset**](https://superset.apache.org)
- Orchestration - [**Airflow**](https://airflow.apache.org)
- Language - [**Python**](https://www.python.org)

## Setup

### Pre-requisites
- Docker and Docker Compose installed on your local machine.
- Python 3.9+ (if running scripts locally outside of containers).

### Architecture
*(Architecture diagram will be added here)*

## How to run
We use `Makefile` to simplify command executions. 

```bash
# Start all infrastructure services
make infra-up

# Stop all infrastructure services
make infra-down
```
