#!/usr/bin/env python3
"""
PsyArXiv Ingestion CLI - Process raw data into normalized tables

Usage: python scripts/ingest.py [--limit NUM] [--force]
"""

import argparse
import logging
import sys
import traceback
from osf.ingestor import process_all_new_preprints, get_unprocessed_preprints
from osf import database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('ingest_cli')

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Process raw PsyArXiv data into normalized tables')
    parser.add_argument('--limit', type=int, help='Limit the number of preprints to process')
    parser.add_argument('--force', action='store_true', help='Force reprocessing of all preprints (clears preprints table)')
    args = parser.parse_args()
    
    try:
        # Setup and check status
        db = database.init_db()
        
        # Handle force flag - clear preprints table to reprocess everything
        if args.force:
            if "preprints" in db.table_names():
                logger.info("Force flag specified - clearing preprints table for reprocessing")
                db["preprints"].drop()
                db = database.init_db()  # Recreate the table
        
        initial_count = db["preprints"].count if "preprints" in db.table_names() else 0
        unprocessed = get_unprocessed_preprints(db, limit=1)  # Just check count
        pending_count = len(get_unprocessed_preprints(db))
        
        logger.info(f"Database contains {initial_count} processed preprints, {pending_count} unprocessed")
        
        # Skip if nothing to do (unless force was used)
        if pending_count == 0 and not args.force:
            logger.info("No preprints to process, exiting")
            return 0
        
        # Process preprints
        processed, success, errors = process_all_new_preprints(limit=args.limit)
        
        # Report results
        final_count = db["preprints"].count if "preprints" in db.table_names() else 0
        logger.info(f"Processing complete: {success} added, {errors} errors")
        logger.info(f"Database now contains {final_count} processed preprints")
        
        return 0
        
    except Exception as e:
        logger.error(f"Processing error: {e}")
        logger.debug(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 