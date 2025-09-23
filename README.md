# PsyArXivDB

A [Datasette](https://datasette.io/) instance serving [PsyArXiv](https://psyarxiv.com/) preprint data from the [Open Science Framework](https://osf.io/) (OSF) API. Updated daily.

Browse PsyArXiv preprints at <https://psyarxivdb.vuorre.com>.

[Comments](https://github.com/mvuorre/psyarxivdb/issues) welcome. Thanks to OSF, PsyArXiv, and all researchers sharing their work.

## Development

Before getting started, get a copy of a current database file from Matti to speed up the setup & avoid unnecessarily stressing the OSF API.

### Prerequisites

- Computer etc.
- Python 3.9+
- [UV](https://github.com/astral-sh/uv)

### Setup and use

```bash
# Clone the repository
git clone https://github.com/mvuorre/psyarxivdb
cd psyarxivdb

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

# Dump processed preprints table as compressed csv
make dump

# Run daily update of database
make daily-update

# Show all available commands
make help
```

### Project Structure

- `data/preprints.db` - SQLite database with all data
- `osf/` - Core library (harvester, ingestor, database)
- `scripts/` - CLI tools (`harvest.py`, `ingest.py`)  
- `tools/` - Admin utilities (`show_status.py`, `fix_gaps.py`)
- `datasette/` - Metadata configuration for web interface
