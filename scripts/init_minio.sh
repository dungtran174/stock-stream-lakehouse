#!/bin/sh

echo "Connecting to MinIO..."
until (/usr/bin/mc config host add minio http://minio:9000 admin password); do
  echo '...waiting for minio to start...'
  sleep 1
done

echo "Creating 'warehouse' bucket for Iceberg..."
/usr/bin/mc mb minio/warehouse || echo "Bucket already exists"
/usr/bin/mc policy set public minio/warehouse

echo "MinIO initialization completed successfully."

# Keep container alive
tail -f /dev/null
