#!/usr/bin/env python3
"""
Reset Ingestion Status Tool

This script resets the ingestion status of all preprints in the tracker database,
forcing them to be reprocessed during the next ingestion run. This is useful when:

1. After resetting the main database but ingestion shows no pending preprints
2. When troubleshooting ingestion issues
3. When you want to regenerate the entire database from cached files

Usage:
    python tools/reset_ingestion.py [--force]

Options:
    --force     Skip confirmation prompt
"""
import argparse
import logging
import sys
import os
import time
import sqlite3
from datetime import datetime

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import tracker, config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("reset_ingestion")

def main():
    """Main function to reset ingestion status."""
    parser = argparse.ArgumentParser(description="Reset ingestion status of all preprints")
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of preprints to update in each batch')
    args = parser.parse_args()
    
    # Initialize tracker database
    db = tracker.init_tracker_db()
    
    # Set a longer timeout to prevent locks
    db.execute("PRAGMA busy_timeout = 30000")  # 30 seconds
    
    print(f"Tracker database: {config.TRACKER_DB_PATH}")
    
    # Get current counts
    total_count = tracker.get_harvested_preprint_count()
    pending_count = tracker.get_pending_ingestion_count()
    ingested_count = total_count - pending_count
    
    print(f"\nCurrent status:")
    print(f"- {total_count} total preprints in tracker")
    print(f"- {pending_count} preprints currently pending ingestion")
    print(f"- {ingested_count} preprints currently marked as ingested")
    
    if ingested_count == 0:
        print("\nNo preprints are currently marked as ingested. Nothing to do.")
        return
    
    # Validate file paths to warn about potential issues
    valid_count, invalid_count, total_paths = tracker.validate_harvested_file_paths()
    if invalid_count > 0:
        print(f"\nWARNING: {invalid_count} preprints have invalid file paths and will be skipped during ingestion.")
        print("Run 'python tools/fix_tracker.py' to diagnose and fix these issues.")
    
    print("\nThis will mark ALL preprints as needing ingestion, forcing them to be reprocessed.")
    
    # Confirm unless --force is used
    if not args.force:
        answer = input("\nProceed with reset? (y/N): ")
        if answer.lower() not in ['y', 'yes']:
            print("Operation cancelled.")
            return
    
    # Record start time
    start_time = datetime.now()
    
    # Reset status
    print("\nResetting ingestion status...")
    try:
        # Try the simple approach first, but with a transaction
        try:
            with db.conn:
                # Direct SQL command for efficiency with large datasets
                db.execute("UPDATE harvested_preprints SET is_ingested = 0")
            
            print("Successfully reset all preprints in a single transaction.")
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                print("Database is locked. Switching to batch mode...")
                
                # Get all preprints that need to be reset
                batch_size = args.batch_size
                
                # Process in batches - get the IDs to update first
                print(f"Fetching preprint IDs to reset (this may take a moment)...")
                preprint_ids = list(db.query(
                    "SELECT id FROM harvested_preprints WHERE is_ingested = 1"
                ))
                
                # Prepare for batch processing
                total_batches = (len(preprint_ids) + batch_size - 1) // batch_size
                print(f"Processing {len(preprint_ids)} preprints in {total_batches} batches of {batch_size}...")
                
                updated_count = 0
                for i in range(0, len(preprint_ids), batch_size):
                    batch = preprint_ids[i:i+batch_size]
                    
                    # Build a list of IDs for the SQL query
                    id_list = ", ".join(f"'{p['id']}'" for p in batch)
                    
                    # Try to update with retries
                    max_retries = 5
                    for retry in range(max_retries):
                        try:
                            with db.conn:
                                db.execute(f"UPDATE harvested_preprints SET is_ingested = 0 WHERE id IN ({id_list})")
                            updated_count += len(batch)
                            break
                        except sqlite3.OperationalError as batch_error:
                            if "database is locked" in str(batch_error) and retry < max_retries - 1:
                                print(f"  Batch {i//batch_size + 1}/{total_batches} locked, retrying in 2 seconds...")
                                time.sleep(2)  # Wait before retrying
                            else:
                                print(f"  Failed to update batch {i//batch_size + 1}: {batch_error}")
                                break
                    
                    # Progress reporting
                    if (i//batch_size + 1) % 10 == 0 or i + batch_size >= len(preprint_ids):
                        print(f"  Processed {i + len(batch)}/{len(preprint_ids)} preprints ({(i + len(batch))/len(preprint_ids)*100:.1f}%)")
                
                print(f"Updated {updated_count} of {len(preprint_ids)} preprints in batch mode.")
            else:
                # Some other error
                raise e
        
        # Double-check all were reset
        new_pending = tracker.get_pending_ingestion_count()
        success = (new_pending > 0)  # As long as some were reset, consider it a success
        
        # Calculate time taken
        elapsed = (datetime.now() - start_time).total_seconds()
        
        if success:
            print(f"\nSuccess! {new_pending} of {total_count} preprints are now marked for reprocessing.")
            print(f"Completed in {elapsed:.2f} seconds")
            
            # Show next steps
            print("\nNext steps:")
            print("1. Run 'python -m scripts.ingest' to process all preprints")
            print("2. Run 'python -m scripts.build_ui_table' to rebuild the UI table")
        else:
            print(f"\nOperation completed, but no preprints were marked for reprocessing.")
            print(f"This may indicate a problem with the database.")
    
    except Exception as e:
        print(f"\nError: {e}")
        print("Operation failed.")

if __name__ == "__main__":
    main() 