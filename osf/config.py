"""
Configuration settings for the PsyArXiv harvester.
"""
from pathlib import Path

# Data storage paths
DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "preprints.db"
DATASETTE_DIR = Path("datasette")
LOGS_DIR = Path("logs")

# API configuration
OSF_API_URL = "https://api.osf.io/v2/preprints/"

# Default URL parameters - PsyArXiv only
DEFAULT_PARAMS = {
    "sort": "date_modified",
    "filter[provider]": "psyarxiv",  # Only fetch PsyArXiv preprints
    "filter[date_modified][gt]": "2010-01-01",  # Default start date
    "embed": ["contributors", "license"],
    "fields[licenses]": "name",
    "page[size]": 50
}

# Request configuration
REQUEST_TIMEOUT = 30  # seconds
REQUEST_DELAY = 3  # Seconds between requests