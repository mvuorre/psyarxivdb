# PsyArXiv Datasette

A [Datasette](https://datasette.io/) instance serving [PsyArXiv](https://psyarxiv.com/) preprint data from the [Open Science Framework](https://osf.io/) (OSF) API. Updated daily.

Browse PsyArXiv preprints at <https://osfdata.vuorre.com>.

[Comments](https://github.com/mvuorre/osfdatasette/issues) welcome. Thanks to OSF, PsyArXiv, and all researchers sharing their work.

## Development

### Prerequisites

- Python 3.8+
- [UV](https://github.com/astral-sh/uv)

### Setup

```bash
# Clone the repository
git clone https://github.com/mvuorre/osfdatasette
cd osfdata

# Create and activate virtual environment (with uv)
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Harvest and process PsyArXiv data
make harvest    # Fetch raw data from OSF API
make ingest     # Process into normalized tables
make setup-fts  # Enable full-text search

# Check status
make status
```

### Architecture

1. **Harvest**: `make harvest` → Fetch PsyArXiv data from OSF API → Store in `raw_data` table
2. **Ingest**: `make ingest` → Process raw data → Store in `preprints` table with JSON columns

### Project Structure

- `data/preprints.db` - Single SQLite database with all data
- `osf/` - Core library (harvester, ingestor, database)
- `scripts/` - CLI tools (`harvest.py`, `ingest.py`)  
- `tools/` - Admin utilities (`show_status.py`, `reset_db.py`)
- `datasette/` - Metadata configuration for web interface

### Commands

```bash
make help         # Show all available commands
```

## Use

### First Time Setup
```bash
make harvest      # Fetch PsyArXiv data
make ingest       # Process into tables  
make setup-fts    # Enable search
```

### Daily Updates
```bash
make daily-update # Full pipeline + restart
```

### Manual Operations
```bash
make harvest --limit 100  # Fetch specific amount
make status               # Check database state
make reset                # Reset tables (keeps raw data)
make dump                 # Export to CSV
```

## Deploy & Schedule

**Datasette**: 
```bash
.venv/bin/datasette data/preprints.db --metadata datasette/metadata.yml --host 0.0.0.0 --port 8001
```

**Cron** (daily at 6 AM):
```bash
0 6 * * * cd /path/to/osfdata && make daily-update
```
