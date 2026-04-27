#!/usr/bin/env bash
set -euo pipefail

WINDOW=${1:-morning}
TARGET_DATE=${2:-$(date +%F)}

export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
nvm use 20 >/dev/null

cd /home/konrad/projects/Pred-Soc-2026

echo "Running PredSoc for $TARGET_DATE / $WINDOW"

node src/jobs/ingest-today.js "$TARGET_DATE"
node src/jobs/enrich-compare-stats.js "$TARGET_DATE"
node src/jobs/build-ai-tips.js "$WINDOW"
node src/jobs/send-tips-email.js "$WINDOW" "$TARGET_DATE"
