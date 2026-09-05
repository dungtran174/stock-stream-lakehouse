#!/usr/bin/env bash
set -e

echo "==> Running Airflow database migrations..."
airflow db upgrade

echo "==> Creating default Admin user if not exists..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@lakehouse.com \
  --password admin || true

echo "==> Starting Airflow Webserver..."
exec airflow webserver
