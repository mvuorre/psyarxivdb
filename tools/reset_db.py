#!/usr/bin/env python3
"""
Reset Database Tool

This script resets the PsyArXiv database, which is useful during development
when schema changes are made. It will:

1. Drop all tables in the main database (preprints.db) and recreate them
2. Keep raw data in the raw_data table for reprocessing

This allows you to quickly test schema changes.

Usage:
    python tools/reset_db.py [--force] [--keep-raw]

Options:
    --force       Skip confirmation prompt
    --keep-raw    Keep raw_data table (recommended)
"""
import argparse
import logging
import sys
import os
from datetime import datetime
from pathlib import Path

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import database, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("reset_db")

def main():
    """Main function to reset the database."""
    parser = argparse.ArgumentParser(description="Reset the PsyArXiv database")
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--keep-raw', action='store_true', default=True, help='Keep raw_data table')
    args = parser.parse_args()
    
    # Show status before reset
    db_path = config.DB_PATH
    print(f"Database: {db_path}")
    
    # Get counts if database exists
    if db_path.exists():
        db = database.get_db()
        raw_count = db["raw_data"].count if "raw_data" in db.table_names() else 0
        preprint_count = db["preprints"].count if "preprints" in db.table_names() else 0
        
        print(f"\nCurrent status:")
        print(f"- {raw_count} preprints in raw_data table")
        print(f"- {preprint_count} preprints in normalized tables")
    else:
        print("Database does not exist yet")
        raw_count = preprint_count = 0
    
    print("\nThis will:")
    print("1. Reset the database schema")
    if args.keep_raw:
        print("2. Keep raw_data table (recommended)")
    else:
        print("2. DELETE all raw data (you'll need to re-harvest)")
    print("3. Clear all normalized tables")
    
    # Confirm unless --force is used
    if not args.force:
        answer = input("\nProceed with reset? (y/N): ")
        if answer.lower() not in ['y', 'yes']:
            print("Operation cancelled.")
            return
    
    # Record start time
    start_time = datetime.now()
    
    # Reset database
    print("\nResetting database...")
    
    if db_path.exists():
        # Backup raw data if keeping it
        raw_data = []
        if args.keep_raw and raw_count > 0:
            print("Backing up raw_data...")
            db = database.get_db()
            raw_data = list(db.execute("SELECT * FROM raw_data"))
            print(f"Backed up {len(raw_data)} raw preprints")
        
        # Remove database file
        db_path.unlink()
    
    # Recreate database
    db = database.init_db()
    
    # Restore raw data if we backed it up
    if raw_data:
        print("Restoring raw_data...")
        # Convert rows to dictionaries for sqlite-utils
        raw_records = []
        for row in raw_data:
            raw_records.append({
                "id": row[0],
                "date_created": row[1], 
                "date_modified": row[2],
                "payload": row[3],
                "fetch_date": row[4]
            })
        db["raw_data"].insert_all(raw_records)
        print(f"Restored {len(raw_data)} raw preprints")
    
    # Calculate time taken
    elapsed = (datetime.now() - start_time).total_seconds()
    
    # Display success message
    print(f"\nDatabase reset completed in {elapsed:.2f} seconds")
    
    if raw_data:
        print(f"\nNext steps:")
        print("1. Run 'make ingest' to reprocess all preprints")
        print("2. Run 'make setup-fts' to enable full-text search")
    else:
        print(f"\nNext steps:")
        print("1. Run 'make harvest' to harvest PsyArXiv data")
        print("2. Run 'make ingest' to process the data")
        print("3. Run 'make setup-fts' to enable full-text search")

if __name__ == "__main__":
    main()