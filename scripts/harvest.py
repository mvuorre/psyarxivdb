#!/usr/bin/env python3
"""
PsyArXiv Harvester CLI - Fetch and store PsyArXiv preprint data from OSF API

Usage: python scripts/harvest.py [--limit NUM]
"""

import argparse
import logging
import sys
from osf.harvester import harvest_preprints
from osf import database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('harvest_cli')

def main():
    parser = argparse.ArgumentParser(description='Harvest PsyArXiv preprints from the OSF API')
    parser.add_argument('--limit', type=int, help='Limit the number of preprints to harvest')
    args = parser.parse_args()
    
    try:
        # Log the starting count
        db = database.get_db()
        initial_count = db["raw_data"].count if "raw_data" in db.table_names() else 0
        logger.info(f"Starting with {initial_count} raw preprints")
        
        # Run the harvester
        saved_count = harvest_preprints(limit=args.limit)
        
        # Log the result
        final_count = db["raw_data"].count if "raw_data" in db.table_names() else 0
        logger.info(f"Finished with {final_count} raw preprints (added {final_count - initial_count})")
        return 0
        
    except Exception as e:
        logger.error(f"Harvesting failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 