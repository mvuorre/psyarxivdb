"""
PsyArXiv Processing

A toolkit for harvesting, processing, and querying PsyArXiv preprints data.
Simple pipeline: API → raw_data → normalized tables → Datasette.
"""

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Package version
__version__ = "0.1.0"

# Ensure all modules available for import
from osf import config
from osf import database
from osf import harvester
from osf import ingestor 