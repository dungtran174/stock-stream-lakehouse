#!/usr/bin/env bash
set -e

COMPOSE_CMD="docker compose"
if ! docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

echo "==> Initializing Apache Superset admin user..."
$COMPOSE_CMD exec -T superset superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@lakehouse.com \
    --password admin

echo "==> Running database migrations..."
$COMPOSE_CMD exec -T superset superset db upgrade

echo "==> Initializing default roles and permissions..."
$COMPOSE_CMD exec -T superset superset init

echo "==> Superset bootstrap completed successfully! Web UI: http://localhost:8088"
