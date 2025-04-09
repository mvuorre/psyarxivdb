#!/usr/bin/env python3
"""
Reset Database Tool

This script resets the OSF Preprints database, which is useful during development
when schema changes are made. It will:

1. Drop all tables in the main database (preprints.db) and recreate them
2. Preserve the tracker database (tracker.db) which contains records of harvested preprints
3. Mark all previously ingested preprints as "not ingested" so they'll be reprocessed
4. Preserve all harvested data files (no need to re-download from OSF API)

This allows you to quickly test schema changes without having to re-harvest data.

Usage:
    python tools/reset_db.py [--force]

Options:
    --force     Skip confirmation prompt
"""
import argparse
import logging
import sys
import os
from datetime import datetime

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import database, tracker, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("reset_db")

def main():
    """Main function to reset the database."""
    parser = argparse.ArgumentParser(description="Reset the OSF Preprints database")
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()
    
    # Show status before reset
    db_main_path = config.DB_PATH
    db_tracker_path = config.TRACKER_DB_PATH
    
    print(f"Main database: {db_main_path}")
    print(f"Tracker database: {db_tracker_path}")
    
    # Get counts if databases exist
    main_count = database.get_preprint_count() if db_main_path.exists() else 0
    tracker_harvested = tracker.get_harvested_preprint_count() if db_tracker_path.exists() else 0
    tracker_pending = tracker.get_pending_ingestion_count() if db_tracker_path.exists() else 0
    
    # Validate file paths
    valid_count, invalid_count, total_count = tracker.validate_harvested_file_paths()
    
    print(f"\nCurrent status:")
    print(f"- {main_count} preprints in main database")
    print(f"- {tracker_harvested} preprints harvested in tracker database")
    print(f"- {tracker_pending} preprints pending ingestion")
    print(f"- {tracker_harvested - tracker_pending} preprints already ingested")
    print(f"- {valid_count} preprints with valid file paths")
    print(f"- {invalid_count} preprints with invalid file paths")
    
    if invalid_count > 0:
        print("\nWARNING: Some preprints have invalid file paths. These will be skipped during ingestion.")
        print("You may need to re-harvest these preprints or fix the file paths.")
    
    print("\nThis will:")
    print("1. Reset the main database schema")
    print("2. Keep harvested files (no need to re-download)")
    print("3. Mark all preprints as 'not ingested' to be reprocessed")
    
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
    success, message = database.reset_database()
    
    if success:
        # Calculate time taken
        elapsed = (datetime.now() - start_time).total_seconds()
        
        # Get updated counts
        new_pending = tracker.get_pending_ingestion_count()
        
        # Display success message
        print(f"\nSuccess: {message}")
        print(f"Completed in {elapsed:.2f} seconds")
        print(f"\nUpdated status:")
        print(f"- {new_pending} preprints now pending ingestion")
        
        # Show next steps
        print("\nNext steps:")
        print("1. Run 'python -m scripts.ingest' to reprocess all preprints")
        print("2. Run 'python -m scripts.build_ui_table' to rebuild the UI table")
        print("3. Run 'python -m scripts.optimize --all' to optimize the database")
    else:
        print(f"\nFailed: {message}")

if __name__ == "__main__":
    main() 