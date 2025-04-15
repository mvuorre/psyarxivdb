#!/usr/bin/env python3
"""
OSF Preprints Database Optimizer - Run VACUUM, ANALYZE, and recreate indexes

Usage: python scripts/optimize.py [--vacuum] [--analyze] [--reindex] [--all]
"""

import argparse
import logging
import sys
import time

from osf import database

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('optimize_cli')

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description='Optimize the SQLite database')
    parser.add_argument('--vacuum', action='store_true', help='Run VACUUM')
    parser.add_argument('--analyze', action='store_true', help='Run ANALYZE')
    parser.add_argument('--reindex', action='store_true', help='Recreate indexes')
    parser.add_argument('--all', action='store_true', help='Run all optimizations')
    args = parser.parse_args()
    
    try:
        # Initialize database
        db = database.get_db()
        db_size_before = database.get_database_size()
        
        # Log database info
        logger.info(f"Database size: {db_size_before:,} bytes ({db_size_before/(1024*1024):.2f} MB)")
        
        # Determine operations to run (default to analyze if none specified)
        do_vacuum = args.vacuum or args.all
        do_analyze = args.analyze or args.all or (not any([args.vacuum, args.reindex]))
        do_reindex = args.reindex or args.all
        
        # Recreate indexes
        if do_reindex:
            logger.info("Recreating indexes...")
            start_time = time.time()
            index_count = database.recreate_indexes()
            logger.info(f"Recreated {index_count} indexes in {time.time()-start_time:.2f}s")
        
        # Analyze
        if do_analyze:
            logger.info("Running ANALYZE...")
            start_time = time.time()
            db.execute("ANALYZE")
            logger.info(f"ANALYZE completed in {time.time()-start_time:.2f}s")
        
        # Vacuum
        if do_vacuum:
            logger.info("Running VACUUM (may take a while)...")
            start_time = time.time()
            db.execute("PRAGMA auto_vacuum = FULL")
            db.execute("VACUUM")
            logger.info(f"VACUUM completed in {time.time()-start_time:.2f}s")
        
        # Log results
        db_size_after = database.get_database_size()
        size_diff = db_size_after - db_size_before
        
        logger.info(f"Final size: {db_size_after:,} bytes ({db_size_after/(1024*1024):.2f} MB)")
        logger.info(f"Change: {size_diff:+,} bytes ({size_diff/(1024*1024):+.2f} MB)")
        
        return 0
    except Exception as e:
        logger.error(f"Optimization failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 