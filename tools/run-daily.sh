#!/usr/bin/env bash
set -euo pipefail

WINDOW=${1:-morning}
TARGET_DATE=${2:-$(date +%F)}
YESTERDAY=$(date -d "$TARGET_DATE -1 day" +%F)

export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
nvm use 20 >/dev/null

cd /home/konrad/projects/Pred-Soc-2026

echo "Running results update for $YESTERDAY and $TARGET_DATE"
node src/jobs/update-results.js "$YESTERDAY"
node src/jobs/update-results.js "$TARGET_DATE"
node src/jobs/settle-tips-results.js "$YESTERDAY"
node src/jobs/settle-tips-results.js "$TARGET_DATE"
node src/jobs/send-results-email.js "$YESTERDAY" "$WINDOW" || true
node src/jobs/send-results-email.js "$TARGET_DATE" "$WINDOW" || true

echo "Running PredSoc tips for $TARGET_DATE / $WINDOW"
node src/jobs/ingest-today.js "$TARGET_DATE"
node src/jobs/enrich-compare-stats.js "$TARGET_DATE"
node src/jobs/build-ai-tips.js "$WINDOW"
node src/jobs/send-tips-email.js "$WINDOW" "$TARGET_DATE"
