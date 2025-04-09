#!/usr/bin/env python3
"""
OSF Preprints Ingestion CLI - Import harvested preprint data into SQLite

Usage: python scripts/ingest.py [--limit NUM]
"""

import argparse
import logging
import sys
import traceback
from osf.ingestor import process_all_new_preprints
from osf import database
from osf import tracker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('ingest_cli')

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Ingest harvested preprints into SQLite')
    parser.add_argument('--limit', type=int, help='Limit the number of preprints to ingest')
    args = parser.parse_args()
    
    try:
        # Setup and check status
        database.init_db()
        tracker.init_tracker_db()
        
        initial_count = database.get_preprint_count()
        pending_count = tracker.get_pending_ingestion_count()
        
        logger.info(f"Database contains {initial_count} preprints, {pending_count} pending ingestion")
        
        # Skip if nothing to do
        if pending_count == 0:
            logger.info("No preprints to ingest, exiting")
            return 0
        
        # Process preprints
        processed, success, errors = process_all_new_preprints(limit=args.limit)
        
        # Report results
        final_count = database.get_preprint_count()
        logger.info(f"Ingestion complete: {success} added, {errors} errors")
        logger.info(f"Database now contains {final_count} preprints")
        
        return 0
        
    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        logger.debug(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 