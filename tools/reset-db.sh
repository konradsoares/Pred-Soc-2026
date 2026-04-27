#!/usr/bin/env bash
set -euo pipefail

echo "Stopping containers and removing database volume..."
docker compose -f docker/compose.yaml down -v

echo "Starting fresh environment..."
docker compose -f docker/compose.yaml up -d

echo "Done. Fresh database created."
