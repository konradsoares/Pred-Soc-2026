#!/usr/bin/env bash
set -euo pipefail

export NVM_DIR="$HOME/.nvm"

if [ -s "$NVM_DIR/nvm.sh" ]; then
  # shellcheck source=/dev/null
  . "$NVM_DIR/nvm.sh"
else
  echo "NVM not found at $NVM_DIR/nvm.sh"
  exit 1
fi

nvm use 20 >/dev/null

cd /home/konrad/projects/Pred-Soc-2026

node src/jobs/ingest-today.js
node src/jobs/enrich-compare-stats.js
node src/jobs/build-ai-tips.js
node src/jobs/send-tips-email.js
