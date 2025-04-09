#!/usr/bin/env python3
"""
Database Status Tool

This script shows the current status of the OSF Preprints databases and tracker.
It's useful for diagnosing issues with ingestion or database synchronization.

Usage:
    python tools/show_status.py [--verbose]

Options:
    --verbose   Show more detailed information
"""
import argparse
import logging
import sys
import os
from pathlib import Path
import sqlite3

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import tracker, database, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("show_status")

def check_direct_db_stats(db_path, verbose=False):
    """Check database stats directly using SQLite3."""
    if not Path(db_path).exists():
        print(f"Database file not found: {db_path}")
        return {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    stats = {"tables": tables, "counts": {}}
    
    # Get count of rows in each table
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        stats["counts"][table] = count
    
    # For tracker database, check ingestion status
    if "harvested_preprints" in tables:
        cursor.execute("SELECT COUNT(*) FROM harvested_preprints WHERE is_ingested = 0")
        pending_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM harvested_preprints WHERE is_ingested = 1")
        ingested_count = cursor.fetchone()[0]
        
        stats["pending_ingestion"] = pending_count
        stats["already_ingested"] = ingested_count
    
    # For main database, get more detailed info
    if verbose and "preprints" in tables:
        cursor.execute("SELECT COUNT(DISTINCT id) FROM preprints")
        unique_preprints = cursor.fetchone()[0]
        stats["unique_preprints"] = unique_preprints
    
    conn.close()
    
    return stats

def main():
    """Main function to show database status."""
    parser = argparse.ArgumentParser(description="Show database status")
    parser.add_argument('--verbose', action='store_true', help='Show more detailed information')
    args = parser.parse_args()
    
    # Show database locations
    print("Database Locations:")
    print(f"- Main DB: {config.DB_PATH}")
    print(f"- Tracker DB: {config.TRACKER_DB_PATH}")
    
    # Check main database
    print("\nMain Database Status:")
    db_exists = Path(config.DB_PATH).exists()
    print(f"- File exists: {'Yes' if db_exists else 'No'}")
    
    if db_exists:
        db_size = Path(config.DB_PATH).stat().st_size
        print(f"- File size: {db_size} bytes ({db_size/1024/1024:.2f} MB)")
        
        # Get preprint count using our API
        preprint_count = database.get_preprint_count()
        print(f"- Preprints (via API): {preprint_count}")
        
        # Direct database check
        main_stats = check_direct_db_stats(config.DB_PATH, args.verbose)
        print(f"- Tables: {', '.join(main_stats.get('tables', []))}")
        
        for table, count in main_stats.get('counts', {}).items():
            print(f"  - {table}: {count} rows")
    
    # Check tracker database
    print("\nTracker Database Status:")
    tracker_exists = Path(config.TRACKER_DB_PATH).exists()
    print(f"- File exists: {'Yes' if tracker_exists else 'No'}")
    
    if tracker_exists:
        tracker_size = Path(config.TRACKER_DB_PATH).stat().st_size
        print(f"- File size: {tracker_size} bytes ({tracker_size/1024/1024:.2f} MB)")
        
        # Get counts using our API
        harvested_count = tracker.get_harvested_preprint_count()
        pending_count = tracker.get_pending_ingestion_count()
        print(f"- Total harvested (via API): {harvested_count}")
        print(f"- Pending ingestion (via API): {pending_count}")
        
        # Direct database check
        tracker_stats = check_direct_db_stats(config.TRACKER_DB_PATH, args.verbose)
        print(f"- Tables: {', '.join(tracker_stats.get('tables', []))}")
        
        for table, count in tracker_stats.get('counts', {}).items():
            print(f"  - {table}: {count} rows")
        
        # Show ingestion status breakdown
        pending = tracker_stats.get("pending_ingestion", 0)
        ingested = tracker_stats.get("already_ingested", 0)
        print(f"- Direct query results:")
        print(f"  - Pending ingestion: {pending}")
        print(f"  - Already ingested: {ingested}")
        
        # Check if there's a mismatch
        if pending != pending_count:
            print(f"  - WARNING: Mismatch in pending count: API reports {pending_count}, direct query shows {pending}")
    
    # Validate file paths
    valid_count, invalid_count, total_count = tracker.validate_harvested_file_paths()
    
    print("\nFile Path Validation:")
    print(f"- Total files: {total_count}")
    print(f"- Valid paths: {valid_count}")
    print(f"- Invalid paths: {invalid_count}")
    
    # Reconciliation status
    print("\nReconciliation Status:")
    if db_exists and tracker_exists:
        harvested = tracker_stats.get("counts", {}).get("harvested_preprints", 0)
        ingested = tracker_stats.get("already_ingested", 0)
        preprints = main_stats.get("counts", {}).get("preprints", 0)
        
        print(f"- Harvested preprints: {harvested}")
        print(f"- Ingested according to tracker: {ingested}")
        print(f"- Preprints in main database: {preprints}")
        
        if ingested != preprints:
            print(f"- WARNING: Mismatch between ingested preprints ({ingested}) and main database ({preprints})")

if __name__ == "__main__":
    main() 