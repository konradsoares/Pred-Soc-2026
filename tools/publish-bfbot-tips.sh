#!/usr/bin/env bash
set -euo pipefail

WINDOW="${1:-daily}"
TARGET_DATE="${2:-$(date +%F)}"

PROJECT_DIR="/home/konrad/projects/Pred-Soc-2026"
PAGES_DIR="$PROJECT_DIR/docs/bfbot"

cd "$PROJECT_DIR"

node src/jobs/export-bfbot-tips.js "$WINDOW" "$TARGET_DATE"

mkdir -p "$PAGES_DIR"

cp "$PROJECT_DIR/output/bfbot/bfbot-latest.csv" "$PAGES_DIR/tips.csv"

git add docs/bfbot/tips.csv
git commit -m "Update BF Bot tips" || true
git push origin main
