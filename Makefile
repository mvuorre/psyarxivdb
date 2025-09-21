.PHONY: setup-fts optimize vacuum analyze harvest ingest update restart help daily-update

# Database path from osf/config.py
DB_PATH = data/preprints.db

help:
	@echo "Available commands:"
	@echo "  make setup-fts      - Set up full-text search index with triggers"
	@echo "  make analyze        - Run ANALYZE on the database to improve query performance"
	@echo "  make vacuum         - Compact the database with VACUUM"
	@echo "  make harvest        - Harvest new preprints from OSF API"
	@echo "  make ingest         - Ingest harvested preprints into the database"
	@echo "  make restart        - Restart the Datasette service"
	@echo "  make daily-update   - Run the complete daily update process (harvest, ingest, vacuum, analyze, restart)"

# The --create-triggers flag creates SQLite triggers that automatically keep 
# the FTS index in sync when data in the preprints_ui table is inserted, 
# updated, or deleted. Once set up, no manual update is needed.
setup-fts:
	@echo "Setting up full-text search with automatic triggers..."
	sqlite-utils enable-fts $(DB_PATH) preprints_ui title description contributors_list --create-triggers
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
	@echo "Harvesting preprints from OSF API..."
	.venv/bin/python -m scripts.harvest
	@echo "Harvesting complete"

ingest:
	@echo "Ingesting harvested preprints..."
	.venv/bin/python -m scripts.ingest
	@echo "Ingestion complete"

restart:
	@echo "Restarting Datasette service..."
	pm2 restart osfdata -s
	@echo "Service restarted"

daily-update:
	@echo "=== OSF Update: $$(date) ==="
	@$(MAKE) harvest
	@sleep 2
	@$(MAKE) ingest
	@sleep 2
	@$(MAKE) analyze
	@sleep 2
	@$(MAKE) vacuum
	@sleep 2
	@$(MAKE) restart
	@echo "=== Update complete: $$(date) ===" 

dump:
	sqlite3 $(DB_PATH) ".headers on" ".mode csv" "SELECT * FROM preprints_ui;" | gzip > data/preprints_ui.csv.gz

dump-psyarxiv:
	sqlite3 $(DB_PATH) "SELECT * FROM preprints WHERE provider='psyarxiv';" -header -csv | gzip > data/psyarxiv.csv.gz
