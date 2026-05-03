#!/usr/bin/env bash
set -euo pipefail

TARGET_DATE=${1:-$(date +%F)}
YESTERDAY=$(date -d "$TARGET_DATE -1 day" +%F)

export NVM_DIR="$HOME/.nvm"
. "$NVM_DIR/nvm.sh"
nvm use 20 >/dev/null

cd /home/konrad/projects/Pred-Soc-2026

echo "Updating Betfair results for $YESTERDAY and $TARGET_DATE"
node src/jobs/update-results-from-betfair.js "$YESTERDAY" || true
#node src/jobs/update-results-from-betfair.js "$TARGET_DATE" || true

echo "Settling shadow markets for $YESTERDAY and $TARGET_DATE"
node src/jobs/settle-shadow-markets-from-betfair.js "$YESTERDAY" || true
#node src/jobs/settle-shadow-markets-from-betfair.js "$TARGET_DATE" || true

node src/jobs/send-results-email.js "$YESTERDAY" daily || true
#node src/jobs/send-results-email.js "$TARGET_DATE" daily || true

echo "Running Betfair daily tips for $TARGET_DATE"
node src/jobs/ingest-betfair-today.js "$TARGET_DATE"
node src/jobs/sync-betfair-to-fixtures.js "$TARGET_DATE"
node src/jobs/match-betfair-to-statarea.js "$TARGET_DATE"
node src/jobs/backfill-betfair-scraped-predictions.js "$TARGET_DATE"
node src/jobs/enrich-compare-stats.js "$TARGET_DATE"
node src/jobs/build-ai-tips.js "$TARGET_DATE"
node src/jobs/map-tips-to-betfair-markets.js "$TARGET_DATE"

echo "Creating paper bets for $TARGET_DATE"
node src/jobs/create-paper-bets.js "$TARGET_DATE"

node src/jobs/send-tips-email.js daily "$TARGET_DATE"

echo "PredSoc daily flow completed for $TARGET_DATE"
