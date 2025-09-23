.PHONY: setup-fts vacuum analyze harvest ingest help daily-update status fix-gaps

# Database path from osf/config.py
DB_PATH = data/preprints.db

help:
	@echo "Available commands:"
	@echo "  make harvest        - Harvest PsyArXiv preprints from OSF API"
	@echo "  make ingest         - Process raw data into normalized tables"
	@echo "  make setup-fts      - Set up full-text search index with triggers"
	@echo "  make analyze        - Run ANALYZE on the database to improve query performance"
	@echo "  make vacuum         - Compact the database with VACUUM"
	@echo "  make status         - Show database status"
	@echo "  make fix-gaps       - Detect and fill gaps in harvested data"
	@echo "  make daily-update   - Run the complete daily update process"

# The --create-triggers flag creates SQLite triggers that automatically keep 
# the FTS index in sync when data in the preprints table is inserted, 
# updated, or deleted. Once set up, no manual update is needed.
setup-fts:
	@echo "Setting up full-text search on preprints table..."
	.venv/bin/sqlite-utils enable-fts $(DB_PATH) preprints title description --create-triggers
	@echo "FTS setup complete"

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

daily-update:
	@echo "=== PsyArXiv Update: $$(date) ==="
	@$(MAKE) harvest
	@sleep 2
	@$(MAKE) ingest
	@sleep 2
	@$(MAKE) analyze
	@sleep 2
	@$(MAKE) vacuum
	@echo "=== Update complete: $$(date) ===" 

dump:
	@echo "Exporting PsyArXiv data to CSV..."
	sqlite3 $(DB_PATH) ".headers on" ".mode csv" "SELECT * FROM preprints;" | gzip > data/preprints.csv.gz
	@echo "Export complete: data/preprints.csv.gz"
