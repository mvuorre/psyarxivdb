#!/usr/bin/env python3
"""
Database Status Tool

This script shows the current status of the PsyArXiv database.

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

from osf import database, config

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
    
    # For raw_data table, check processing status
    if "raw_data" in tables and "preprints" in tables:
        cursor.execute("""
            SELECT COUNT(*) FROM raw_data r
            LEFT JOIN preprints p ON r.id = p.id
            WHERE p.id IS NULL
        """)
        pending_count = cursor.fetchone()[0]
        stats["pending_processing"] = pending_count
    
    # Get latest preprint timestamp
    if "preprints" in tables:
        cursor.execute("SELECT MAX(date_modified) FROM preprints WHERE date_modified IS NOT NULL")
        latest_preprint = cursor.fetchone()[0]
        stats["latest_preprint_timestamp"] = latest_preprint
    
    # Get more detailed info
    if verbose and "preprints" in tables:
        cursor.execute("SELECT COUNT(DISTINCT id) FROM preprints")
        unique_preprints = cursor.fetchone()[0]
        stats["unique_preprints"] = unique_preprints
        
        cursor.execute("SELECT MIN(date_created), MAX(date_created) FROM preprints WHERE date_created IS NOT NULL")
        date_range = cursor.fetchone()
        stats["date_range"] = date_range
    
    conn.close()
    return stats

def main():
    """Main function to show database status."""
    parser = argparse.ArgumentParser(description="Show PsyArXiv database status")
    parser.add_argument('--verbose', action='store_true', help='Show more detailed information')
    args = parser.parse_args()
    
    # Show database location
    print("Database Location:")
    print(f"- Main DB: {config.DB_PATH}")
    
    # Check main database
    print("\nDatabase Status:")
    db_exists = Path(config.DB_PATH).exists()
    print(f"- File exists: {'Yes' if db_exists else 'No'}")
    
    if db_exists:
        db_size = Path(config.DB_PATH).stat().st_size
        print(f"- File size: {db_size} bytes ({db_size/1024/1024:.2f} MB)")
        
        # Direct database check
        main_stats = check_direct_db_stats(config.DB_PATH, args.verbose)
        print(f"- Tables: {', '.join(main_stats.get('tables', []))}")
        
        for table, count in main_stats.get('counts', {}).items():
            print(f"  - {table}: {count} rows")
        
        # Show processing status
        pending = main_stats.get("pending_processing", 0)
        if pending > 0:
            print(f"- Unprocessed raw data: {pending} preprints")
        else:
            print("- All raw data has been processed")
        
        # Show latest preprint timestamp
        latest_timestamp = main_stats.get("latest_preprint_timestamp")
        if latest_timestamp:
            print(f"- Latest preprint timestamp: {latest_timestamp}")
        
        # Show date range if verbose
        if args.verbose:
            date_range = main_stats.get("date_range", (None, None))
            if date_range[0] and date_range[1]:
                print(f"- Date range: {date_range[0]} to {date_range[1]}")

if __name__ == "__main__":
    main()