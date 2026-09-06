# Troubleshooting & Debugging Guide

This guide compiles common production and local development issues encountered across Docker, Kafka, Spark, Trino, Airflow, and Superset, along with verified solutions.

---

## 1. Port Conflicts

### Symptom:
`docker-compose up` fails with `Bind for 0.0.0.0:<PORT> failed: port is already allocated`.

### Common Collisions:
- **Port 8080:** Used by Kafka UI host mapping. If another process (or local Tomcat/Jenkins) is running on 8080, modify `ports:` in `docker-compose.yml` for `kafka-ui` (e.g., `8085:8080`).
- **Port 5000:** Used by Flask API. On macOS, AirPlay Receiver frequently listens on port 5000 (disable AirPlay Receiver in System Settings).
- **Port 9092:** Used by external Kafka broker if a local Kafka service is already running on the host.

### Diagnostics:
```bash
# Identify which process is holding a port (e.g., 8080)
sudo lsof -i :8080
# or
sudo netstat -tulpn | grep 8080
```

---

## 2. Docker Out of Memory (OOM)

### Symptom:
Containers abruptly exit with `Exited (137)`.

### Root Cause:
Running Spark Master + Worker, Trino Coordinator, Airflow Webserver + Scheduler, Kafka, MinIO, and Superset concurrently requires approximately 8GB to 12GB of RAM.

### Solutions:
1. Increase Docker Desktop or daemon resource limits to at least 8GB RAM and 4 CPU cores.
2. Review memory caps defined under `mem_limit` in `docker-compose.yml`:
   - `broker`: 512MB
   - `kafka-ui`: 256MB
   - `spark-iceberg`: 2GB
   - `spark-worker-1`: 3GB
3. Inspect memory consumption across containers:
   ```bash
   docker stats
   ```

---

## 3. Kafka Ingestion: "Leader Not Available"

### Symptom:
Flask API or Spark Streaming logs `UNKNOWN_TOPIC_OR_PARTITION` or `Leader Not Available`.

### Solutions:
1. In KRaft mode, Kafka creates topics dynamically upon receiving the first message if `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is true.
2. If auto-creation is delayed, create the daily topic explicitly via Kafka UI ([http://localhost:8080](http://localhost:8080)) or via CLI:
   ```bash
   docker exec -it broker kafka-topics --bootstrap-server broker:29092 --create --topic stock_transactions_2025_6_10 --partitions 1 --replication-factor 1
   ```

---

## 4. Airflow Spark Connection Fails

### Symptom:
`SparkSubmitOperator` tasks fail with `Failed to connect to spark-iceberg:7077`.

### Solutions:
1. Verify Spark Master is healthy and running:
   ```bash
   docker exec -it webserver nc -zv spark-iceberg 7077
   ```
2. In the Airflow Web UI (**Admin** -> **Connections**):
   - Ensure Connection ID is exactly `spark_conn`.
   - Ensure Host is set to `spark-iceberg` and Port to `7077`.
   - Ensure Connection Type is `Spark`.

---

## 5. Trino Connection Fails: "Could not resolve host"

### Symptom:
Superset or Airflow reports `Could not resolve host: trino`.

### Solutions:
1. Verify that all containers belong to the same Docker bridge network (`lakehouse-network`):
   ```bash
   docker network inspect lakehouse-network
   ```
2. Verify Trino coordinator responsiveness:
   ```bash
   curl -I http://localhost:8383/v1/info
   ```
3. Test Trino query execution directly via CLI inside the network:
   ```bash
   docker exec -it trino trino --catalog iceberg --schema stocks --execute "SELECT 1;"
   ```

---

## 6. Environment Reset & Clean Slate

If the environment becomes corrupted, perform a clean reset:

```bash
# Stop all services and remove orphan containers
make infra-down

# Clean up dangling volumes and caches
make clean

# Bring services back up cleanly
make infra-up
```
