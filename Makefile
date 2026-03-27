.PHONY: vacuum analyze harvest ingest help daily-update status fix-gaps fix-version-flags refresh-dashboard-summaries pull-db datasette-local

ifneq (,$(wildcard .env))
include .env
endif

# Database path from osf/config.py
DB_PATH = data/preprints.db
DEV_DB_HOST ?=
DEV_DB_REMOTE_PATH ?=
DATASETTE_HOST ?= 127.0.0.1
DATASETTE_PORT ?= 8001

help:
	@echo "Available commands:"
	@echo "  make harvest        - Harvest PsyArXiv preprints from OSF API"
	@echo "  make ingest         - Process raw data into normalized tables"
	@echo "  make analyze        - Run ANALYZE on the database to improve query performance"
	@echo "  make vacuum         - Compact the database with VACUUM"
	@echo "  make status         - Show database status"
	@echo "  make fix-gaps       - Detect and fill gaps in harvested data"
	@echo "  make fix-version-flags - Fix is_latest_version flags for all preprints"
	@echo "  make refresh-dashboard-summaries - Rebuild the dashboard summary tables"
	@echo "  make pull-db        - Copy the current database from the VPS"
	@echo "  make datasette-local - Run Datasette on 127.0.0.1:8001"
	@echo "  make daily-update   - Run the complete daily update process"


vacuum:
	@echo "Running VACUUM (this may take a while)..."
	sqlite3 $(DB_PATH) "PRAGMA auto_vacuum = FULL; VACUUM;"
	@echo "VACUUM complete"

analyze:
	@echo "Running ANALYZE..."
	sqlite3 $(DB_PATH) "ANALYZE;"
	@echo "ANALYZE complete"

harvest:
	@echo "Harvesting PsyArXiv preprints from OSF API..."
	.venv/bin/python -m scripts.harvest
	@echo "Harvesting complete"

ingest:
	@echo "Processing raw data into normalized tables..."
	.venv/bin/python -m scripts.ingest
	@echo "Processing complete"

status:
	@echo "Checking database status..."
	.venv/bin/python tools/show_status.py

fix-gaps:
	@echo "Detecting and filling gaps in harvested data..."
	.venv/bin/python tools/fix_gaps.py

fix-version-flags:
	@echo "Fixing is_latest_version flags for all preprints..."
	.venv/bin/python tools/fix_version_flags.py

refresh-dashboard-summaries:
	@echo "Refreshing dashboard summary tables..."
	.venv/bin/python tools/refresh_dashboard_summaries.py

pull-db:
	@test -n "$(DEV_DB_HOST)" || (echo "Set DEV_DB_HOST, e.g. user@your-vps"; exit 1)
	@test -n "$(DEV_DB_REMOTE_PATH)" || (echo "Set DEV_DB_REMOTE_PATH, e.g. /srv/psyarxivdb/data/preprints.db"; exit 1)
	@mkdir -p $(dir $(DB_PATH))
	@echo "Copying $(DEV_DB_REMOTE_PATH) from $(DEV_DB_HOST) to $(DB_PATH)..."
	rsync -avz $(DEV_DB_HOST):$(DEV_DB_REMOTE_PATH) $(DB_PATH)
	@echo "Local database refreshed: $(DB_PATH)"

datasette-local:
	@.venv/bin/python -m datasette --version >/dev/null 2>&1 || (echo "Datasette is not installed in .venv. Run: uv sync --frozen"; exit 1)
	@test -f $(DB_PATH) || (echo "Database file not found: $(DB_PATH). Run: make pull-db"; exit 1)
	@echo "Starting local Datasette on http://$(DATASETTE_HOST):$(DATASETTE_PORT) with production-like query limits..."
	.venv/bin/python -m datasette serve $(DB_PATH) --metadata datasette/metadata.yml --setting truncate_cells_html 160 --setting sql_time_limit_ms 5000 --setting max_csv_mb 50 --setting default_page_size 20 --setting max_returned_rows 5000 --setting suggest_facets off --host $(DATASETTE_HOST) --port $(DATASETTE_PORT)

daily-update:
	@echo "=== PsyArXiv Update: $$(date) ==="
	@$(MAKE) harvest
	@sleep 2
	@$(MAKE) ingest
	@sleep 2
	@$(MAKE) fix-version-flags
	@sleep 2
	@$(MAKE) refresh-dashboard-summaries
	@sleep 2
	@$(MAKE) analyze
	@sleep 2
	@$(MAKE) vacuum
	@echo "=== Update complete: $$(date) ===" 

dump:
	@echo "Exporting PsyArXiv data to CSV..."
	sqlite3 $(DB_PATH) ".headers on" ".mode csv" "SELECT * FROM preprints;" | gzip > data/preprints.csv.gz
	@echo "Export complete: data/preprints.csv.gz"
