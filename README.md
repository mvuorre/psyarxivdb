# PsyArXivDB

A [Datasette](https://datasette.io/) instance serving [PsyArXiv](https://psyarxiv.com/) preprint data from the [Open Science Framework](https://osf.io/) (OSF) API. Updated daily.

Browse PsyArXiv preprints at <https://psyarxivdb.vuorre.com>.

[Comments](https://github.com/mvuorre/psyarxivdb/issues) welcome. Thanks to OSF, PsyArXiv, and all researchers sharing their work.

## Development

Before getting started, get a copy of a current database file from Matti to speed up the setup & avoid unnecessarily stressing the OSF API.

### Prerequisites

- Computer etc.
- Python 3.9-3.13
- [UV](https://github.com/astral-sh/uv)

### Setup and use

```bash
# Clone the repository
git clone https://github.com/mvuorre/psyarxivdb
cd psyarxivdb

# Create .venv with the repo-pinned Python 3.13 and install dependencies
uv sync --frozen

# Optional: activate the environment in your shell
source .venv/bin/activate

# Harvest and process PsyArXiv data
make harvest    # Fetch raw data from OSF API
make ingest     # Process into normalized tables

# Check status
make status

# Dump processed preprints table as compressed csv
make dump

# Run daily update of database
make daily-update

# Show all available commands
make help
```

To rebuild the small precomputed tables used by the dashboard without doing a full ingest, run:

```bash
make refresh-dashboard-summaries
```

### Local copy workflow

The simplest local development flow is:

1. Pull a fresh copy of the database from the VPS.
2. Run Datasette locally against that copy.
3. Point any local apps at `http://127.0.0.1:8001`.

Create a local `.env` file with the VPS connection details. `.env` is gitignored.

```bash
cp .env.example .env
```

Then edit `.env` so it contains:

```bash
DEV_DB_HOST=user@your-vps
DEV_DB_REMOTE_PATH=/path/to/preprints.db
```

Then pull the latest copy:

```bash
make pull-db
```

This copies `DEV_DB_REMOTE_PATH` from the VPS to `data/preprints.db` using `rsync`.

Keep it simple: run this when the VPS database is not being updated.

If you pulled an older copy of the database or you changed the dashboard summary-table logic, refresh those tables once:

```bash
make refresh-dashboard-summaries
```

Then serve the local copy with Datasette:

```bash
make datasette-local
```

This serves `data/preprints.db` on `http://127.0.0.1:8001` with the Datasette metadata in `datasette/metadata.yml` and production-like query limits, including `sql_time_limit_ms=5000`.

If `make datasette-local` says Datasette is not installed, run:

```bash
uv sync --frozen
```

### Project Structure

- `data/preprints.db` - SQLite database with all data
- `osf/` - Core library (harvester, ingestor, database)
- `scripts/` - CLI tools (`harvest.py`, `ingest.py`)
- `tools/` - Admin utilities (`show_status.py`, `fix_gaps.py`, `refresh_dashboard_summaries.py`)
- `datasette/` - Metadata configuration for web interface
