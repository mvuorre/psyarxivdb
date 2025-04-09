#!/usr/bin/env python3
"""
OSF Preprints UI Table Builder

A utility to create and maintain the denormalized preprints_ui table for efficient UI access.

Usage:
    python scripts/build_ui_table.py [options]

Options:
    --full-rebuild    Rebuild the entire UI table from scratch
    --batch-size N    Process N preprints at a time (default: 500)
"""

import argparse
import logging
import sys
import time

from osf import optimizer_ui
from osf import database

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('build_ui_table_cli')

def parse_args():
    """Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Build and maintain the OSF Preprints UI table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split('\n\n')[2]  # Extract usage examples from docstring
    )
    parser.add_argument('--full-rebuild', action='store_true',
                        help='Rebuild the entire UI table from scratch')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Number of preprints to process in each batch')
    return parser.parse_args()

def main():
    """Main entry point for the UI table builder CLI."""
    args = parse_args()
    
    try:
        start_time = time.time()
        
        # Get initial database size
        initial_size = database.get_database_size()
        if initial_size == 0:
            logger.error("Database does not exist. Run the harvester and ingestor first.")
            return 1
            
        logger.info(f"Database size before UI table build: {initial_size:,} bytes ({initial_size / (1024*1024):.2f} MB)")
        
        # Get preprint count
        db = database.get_db()
        preprint_count = db["preprints"].count if "preprints" in db.table_names() else 0
        ui_count = db["preprints_ui"].count if "preprints_ui" in db.table_names() else 0
        
        logger.info(f"Found {preprint_count:,} preprints in database")
        logger.info(f"Current UI table has {ui_count:,} preprints")
        
        # Build or update the UI table
        processed_count = optimizer_ui.populate_preprints_ui(
            full_rebuild=args.full_rebuild,
            batch_size=args.batch_size
        )
        
        # Get final statistics
        final_size = database.get_database_size()
        final_ui_count = db["preprints_ui"].count if "preprints_ui" in db.table_names() else 0
        
        # Log results
        elapsed = time.time() - start_time
        logger.info(f"Processed {processed_count:,} preprints in {elapsed:.2f} seconds")
        logger.info(f"UI table now contains {final_ui_count:,} preprints")
        logger.info(f"Database size after UI table build: {final_size:,} bytes ({final_size / (1024*1024):.2f} MB)")
        size_diff = final_size - initial_size
        logger.info(f"Change in size: {size_diff:+,} bytes ({size_diff / (1024*1024):+.2f} MB)")
        
        return 0
    except Exception as e:
        logger.error(f"UI table build failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 