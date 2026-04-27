#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Starting PostgreSQL and Adminer..."
docker compose -f docker/compose.yaml up -d

echo "Waiting for PostgreSQL healthcheck..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' predsoc_postgres 2>/dev/null || echo starting)" = "healthy" ]; do
  echo "Postgres not healthy yet..."
  sleep 3
done

echo ""
echo "Environment ready."
echo "Adminer: http://localhost:8080"
