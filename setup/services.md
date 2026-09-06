# Services, Ports, and Credentials Reference

This document outlines all services, port mappings, internal network endpoints, and default access credentials across the Stock Stream Lakehouse architecture.

## 1. Services Overview Table

| Service | Container Name | Host Port | Internal Port | Web UI URL | Default Credentials | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Airflow Webserver** | `webserver` | `8282` | `8080` | [http://localhost:8282](http://localhost:8282) | `admin` / `admin` | Workflow orchestration UI & DAG trigger |
| **Apache Superset** | `superset` | `8088` | `8088` | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` | BI Dashboard & Data Visualization |
| **Trino Query Engine**| `trino` | `8383` | `8080` | [http://localhost:8383](http://localhost:8383) | `admin` (no password) | Distributed SQL Query Engine |
| **MinIO Console** | `minio` | `9001` | `9001` | [http://localhost:9001](http://localhost:9001) | `admin` / `password` | S3 Object Storage Web Console |
| **MinIO S3 API** | `minio` | `9000` | `9000` | - | `admin` / `password` | S3 API endpoint for Iceberg storage |
| **Iceberg REST** | `iceberg-rest` | `8181` | `8181` | - | None | Apache Iceberg REST Catalog |
| **Spark Master UI** | `spark-iceberg` | `8081` | `8080` | [http://localhost:8081](http://localhost:8081) | None | Spark Master Web UI |
| **Spark Master RPC** | `spark-iceberg` | `7077` | `7077` | - | None | Spark cluster master communication |
| **Spark Worker UI** | `spark-worker-1`| `8082` | `8081` | [http://localhost:8082](http://localhost:8082) | None | Spark Worker Web UI |
| **Kafka Broker** | `broker` | `9092` | `29092` | - | None | Apache Kafka broker (KRaft mode) |
| **Kafka UI** | `kafka-ui` | `8080` | `8080` | [http://localhost:8080](http://localhost:8080) | None | Topic inspector and message monitor |
| **Flask API** | `flask-api` | `5000` | `5000` | [http://localhost:5000](http://localhost:5000) | None | Stock transaction event producer API |
| **Airflow Postgres** | `postgres` | - | `5432` | - | `airflow` / `airflow` | Airflow metadata backend database |

---

## 2. Docker Network Architecture

All containers communicate with each other over a single user-defined bridge network:
- **Network Name:** `lakehouse-network`
- **Driver:** `bridge`

When services connect internally inside Docker, use their **service/container name**:
- Kafka internal address: `broker:29092`
- Flask internal address: `http://flask-api:5000`
- MinIO internal address: `http://minio:9000`
- Iceberg REST catalog: `http://iceberg-rest:8181`
- Spark Master: `spark://spark-iceberg:7077`
- Trino Coordinator: `http://trino:8080`
- Airflow Postgres: `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`

---

## 3. Quick Healthcheck Commands

```bash
# Verify all container statuses
docker-compose ps

# Check Kafka broker connectivity
nc -zv localhost 9092

# Check MinIO S3 endpoint
curl -I http://localhost:9000/minio/health/live

# Check Trino coordinator
curl -I http://localhost:8383/v1/info
```
