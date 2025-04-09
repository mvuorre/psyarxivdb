"""
Configuration settings for the OSF Preprints harvester.
"""
from pathlib import Path

# Data storage paths
DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "preprints.db"
TRACKER_DB_PATH = DATA_DIR / "tracker.db"
DATASETTE_DIR = Path("datasette")
LOGS_DIR = Path("logs")

# API configuration
OSF_API_URL = "https://api.osf.io/v2/preprints/"

# Default URL parameters
DEFAULT_PARAMS = {
    "sort": "date_created",
    "filter[date_created][gt]": "2010-01-01",  # Default start date
    "embed": ["contributors", "license"],
    "fields[licenses]": "name",
    "page[size]": 50
}

# Request configuration
REQUEST_TIMEOUT = 30  # seconds
REQUEST_DELAY = 1  # Seconds between requests

# JSON Storage options
STORE_JSON_AS = "path"  # Can be "path" or "payload" to determine if we store the file path or the actual JSON 