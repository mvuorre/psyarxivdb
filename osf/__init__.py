"""
OSF Preprints Processing

A toolkit for harvesting, processing, and querying OSF preprints data.
Implements separate concerns for harvesting and ingesting with independent tracking.
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
from osf import entities
from osf import harvester
from osf import ingestor
from osf import tracker
from osf import optimizer 