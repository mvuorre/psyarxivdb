#!/bin/bash
# Daily OSF Preprints update script
# This script is designed to be run as a cron job
# e.q. harvest yesterday's preprints and update database at 00:10
# 10 0 * * * cd /path/harvest-osf-preprints && ./tools/daily_osf_update.sh

# Set up environment
PROJECT_DIR=$(dirname "$(dirname "$(readlink -f "$0")")")
cd "$PROJECT_DIR" || exit 1
LOG_FILE="$PROJECT_DIR/logs/osf_update_$(date +"%Y%m%d").log"
mkdir -p "$(dirname "$LOG_FILE")"

echo "=== OSF Update: $(date) ===" | tee -a "$LOG_FILE"

# Run the pipeline
echo "Harvesting preprints..." | tee -a "$LOG_FILE"
.venv/bin/python -m scripts.harvest 2>&1 | tee -a "$LOG_FILE"
sleep 2

echo "Ingesting preprints..." | tee -a "$LOG_FILE"
.venv/bin/python -m scripts.ingest 2>&1 | tee -a "$LOG_FILE"
sleep 2

echo "Optimizing database..." | tee -a "$LOG_FILE"
.venv/bin/python -m scripts.optimize 2>&1 | tee -a "$LOG_FILE"

echo "Restarting service..." | tee -a "$LOG_FILE"
pm2 restart osfdata -s

echo "=== Update complete: $(date) ===" | tee -a "$LOG_FILE" 