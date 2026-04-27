#!/usr/bin/env bash
set -euo pipefail

docker exec -it predsoc_postgres psql -U predsoc_user -d predsoc