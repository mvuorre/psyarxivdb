#!/usr/bin/env python3
"""
Gap Detection and Filling Tool

This script detects gaps in the harvested data and fills them by re-harvesting
the missing date ranges.

Usage:
    python tools/fix_gaps.py [--dry-run] [--max-gap-days N]
"""
import argparse
import logging
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import database, config
from osf.harvester import harvest_preprints, build_api_url, save_preprint
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("fix_gaps")

def detect_gaps(db, max_gap_hours=24):
    """
    Detect gaps in the harvested data by looking for time periods where
    there are no preprints but there should be based on typical activity.
    
    Args:
        db: Database connection
        max_gap_hours: Maximum expected gap in hours before considering it suspicious
    
    Returns:
        List of (start_date, end_date) tuples representing gaps
    """
    if "raw_data" not in db.table_names():
        logger.warning("No raw_data table found")
        return []
    
    # Get all date_modified values, sorted
    query = """
        SELECT date_modified 
        FROM raw_data 
        WHERE date_modified IS NOT NULL 
        ORDER BY date_modified
    """
    
    results = db.execute(query).fetchall()
    if len(results) < 2:
        logger.info("Not enough data to detect gaps")
        return []
    
    gaps = []
    prev_date = None
    
    for row in results:
        current_date_str = row[0]
        current_date = datetime.fromisoformat(current_date_str.replace('Z', '+00:00'))
        
        if prev_date:
            gap_hours = (current_date - prev_date).total_seconds() / 3600
            if gap_hours > max_gap_hours:
                gaps.append((prev_date, current_date, gap_hours))
                logger.info(f"Gap detected: {prev_date} to {current_date} ({gap_hours:.1f} hours)")
        
        prev_date = current_date
    
    return gaps

def fill_gap(start_date, end_date, dry_run=False):
    """
    Fill a gap by harvesting data between start_date and end_date.
    
    Args:
        start_date: datetime object for start of gap
        end_date: datetime object for end of gap  
        dry_run: If True, just show what would be done
    """
    start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
    
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Filling gap from {start_str} to {end_str}")
    
    if dry_run:
        # Just check how many preprints we would get
        url = build_api_url(start_str)
        try:
            response = requests.get(url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            total_count = 0
            for preprint in data.get('data', []):
                preprint_date_str = preprint.get('attributes', {}).get('date_modified', '')
                if preprint_date_str:
                    preprint_date = datetime.fromisoformat(preprint_date_str.replace('Z', '+00:00'))
                    if start_date <= preprint_date <= end_date:
                        total_count += 1
            
            logger.info(f"[DRY RUN] Would harvest approximately {total_count} preprints")
            
        except Exception as e:
            logger.error(f"[DRY RUN] Error checking gap: {e}")
        return
    
    # Actually fill the gap
    db = database.get_db()
    initial_count = db["raw_data"].count
    
    # Harvest from start_date with a filter for the date range
    saved_count = harvest_date_range(start_str, end_str)
    
    final_count = db["raw_data"].count
    logger.info(f"Gap fill complete: added {final_count - initial_count} preprints")

def harvest_date_range(start_date, end_date):
    """
    Harvest preprints within a specific date range.
    
    Args:
        start_date: ISO string for start date
        end_date: ISO string for end date
    
    Returns:
        Number of preprints saved
    """
    db = database.get_db()
    url = build_api_url(start_date)
    saved_count = 0
    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    logger.info(f"Harvesting range {start_date} to {end_date}")
    
    while url:
        try:
            response = requests.get(url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # Process preprints, but stop when we exceed end_date
            stop_processing = False
            for preprint in data.get('data', []):
                preprint_date_str = preprint.get('attributes', {}).get('date_modified', '')
                if preprint_date_str:
                    preprint_date = datetime.fromisoformat(preprint_date_str.replace('Z', '+00:00'))
                    
                    # Stop if we've gone past the end date
                    if preprint_date > end_dt:
                        logger.info(f"Reached end of date range at {preprint_date_str}")
                        stop_processing = True
                        break
                    
                    if save_preprint(db, preprint):
                        saved_count += 1
                        logger.info(f"Filled gap: saved preprint {preprint.get('id')} (modified: {preprint_date_str})")
            
            if stop_processing:
                break
                
            # Get next page
            url = data.get('links', {}).get('next')
            
        except Exception as e:
            logger.error(f"Error during gap fill: {e}")
            break
    
    return saved_count

def main():
    """Main function to detect and fill gaps."""
    parser = argparse.ArgumentParser(description="Detect and fill gaps in harvested data")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--max-gap-hours', type=int, default=24, help='Maximum gap in hours before considering suspicious (default: 24)')
    args = parser.parse_args()
    
    # Check database exists
    if not Path(config.DB_PATH).exists():
        logger.error(f"Database not found: {config.DB_PATH}")
        return 1
    
    db = database.get_db()
    
    logger.info("Detecting gaps in harvested data...")
    gaps = detect_gaps(db, args.max_gap_hours)
    
    if not gaps:
        logger.info("No gaps detected!")
        return 0
    
    logger.info(f"Found {len(gaps)} gaps")
    
    for i, (start_date, end_date, gap_hours) in enumerate(gaps, 1):
        logger.info(f"\nGap {i}/{len(gaps)}: {gap_hours:.1f} hours")
        fill_gap(start_date, end_date, args.dry_run)
    
    if args.dry_run:
        logger.info("\nDry run complete. Use without --dry-run to actually fill gaps.")
    else:
        logger.info(f"\nGap filling complete! Processed {len(gaps)} gaps.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())