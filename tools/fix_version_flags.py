#!/usr/bin/env python3
"""
Fix is_latest_version Flags - Standalone Script

This script fixes the is_latest_version flags in both the preprints and 
preprint_contributors tables to ensure only the highest version of each 
preprint is marked as the latest version.

Usage:
    python tools/fix_version_flags.py [--dry-run] [--verbose]

Options:
    --dry-run    Show what would be changed without making changes
    --verbose    Show detailed information about changes
"""

import argparse
import logging
import sys
import os
from pathlib import Path

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from osf import database
from osf.ingestor import fix_latest_version_flags, fix_contributor_latest_version_flags

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("fix_version_flags")

def check_current_status(db):
    """Check the current status of version flags in the database."""
    logger.info("Checking current database status...")
    
    # Count total preprints and unique base IDs
    result = db.execute("""
        SELECT 
            COUNT(*) as total_preprints,
            COUNT(DISTINCT CASE 
                WHEN id LIKE '%_v%' THEN SUBSTR(id, 1, INSTR(id, '_v') - 1)
                ELSE id
            END) as unique_base_ids
        FROM preprints
    """).fetchone()
    
    total_preprints = result[0]
    unique_base_ids = result[1]
    
    logger.info(f"Total preprints: {total_preprints}")
    logger.info(f"Unique base IDs: {unique_base_ids}")
    logger.info(f"Preprints with multiple versions: {total_preprints - unique_base_ids}")
    
    # Count preprints marked as latest
    latest_count = db.execute("SELECT COUNT(*) FROM preprints WHERE is_latest_version = 1").fetchone()[0]
    logger.info(f"Preprints marked as latest version: {latest_count}")
    
    # Check for inconsistencies
    if latest_count != unique_base_ids:
        logger.warning(f"INCONSISTENCY: Expected {unique_base_ids} latest versions, found {latest_count}")
        return False
    else:
        logger.info("Version flags appear to be consistent")
        return True

def dry_run_preview(db, verbose=False):
    """Preview what changes would be made without applying them."""
    logger.info("Running dry-run analysis...")
    
    # Query to find problematic entries
    query = """
        WITH version_analysis AS (
            SELECT 
                id,
                version,
                is_latest_version,
                CASE 
                    WHEN id LIKE '%_v%' THEN SUBSTR(id, 1, INSTR(id, '_v') - 1)
                    ELSE id
                END as base_id,
                ROW_NUMBER() OVER (
                    PARTITION BY CASE 
                        WHEN id LIKE '%_v%' THEN SUBSTR(id, 1, INSTR(id, '_v') - 1)
                        ELSE id
                    END 
                    ORDER BY version DESC
                ) as version_rank
            FROM preprints
        )
        SELECT 
            id,
            version,
            is_latest_version,
            base_id,
            version_rank,
            CASE WHEN version_rank = 1 THEN 1 ELSE 0 END as should_be_latest
        FROM version_analysis
        WHERE is_latest_version != CASE WHEN version_rank = 1 THEN 1 ELSE 0 END
        ORDER BY base_id, version DESC
    """
    
    problems = list(db.execute(query))
    
    if not problems:
        logger.info("No version flag problems found!")
        return 0
    
    logger.info(f"Found {len(problems)} preprints with incorrect is_latest_version flags")
    
    if verbose:
        logger.info("Detailed changes that would be made:")
        for row in problems:
            preprint_id, version, current_flag, base_id, rank, should_be = row
            logger.info(f"  {preprint_id} (v{version}): {current_flag} -> {should_be}")
    
    # Count contributor relationship fixes needed
    contrib_query = """
        SELECT COUNT(*) FROM preprint_contributors pc
        JOIN preprints p ON pc.preprint_id = p.id
        WHERE pc.is_latest_version != p.is_latest_version
    """
    contrib_problems = db.execute(contrib_query).fetchone()[0]
    logger.info(f"Would also fix {contrib_problems} contributor relationship flags")
    
    return len(problems)

def main():
    """Main function to fix version flags."""
    parser = argparse.ArgumentParser(description="Fix is_latest_version flags in PsyArXiv database")
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without making changes')
    parser.add_argument('--verbose', action='store_true', help='Show detailed information')
    args = parser.parse_args()
    
    try:
        # Initialize database
        db = database.get_db()
        
        # Check if tables exist
        if "preprints" not in db.table_names():
            logger.error("preprints table not found - database may not be initialized")
            return 1
        
        # Show current status
        status_ok = check_current_status(db)
        
        if args.dry_run:
            # Just show what would be changed
            problems_found = dry_run_preview(db, args.verbose)
            if problems_found == 0:
                logger.info("No changes needed")
                return 0
            else:
                logger.info(f"Dry run complete - would fix {problems_found} issues")
                return 0
        
        if status_ok and not args.verbose:
            logger.info("Database appears consistent - no fixes needed")
            return 0
        
        # Apply fixes
        logger.info("Applying version flag fixes...")
        
        preprints_fixed = fix_latest_version_flags(db)
        contributors_fixed = fix_contributor_latest_version_flags(db)
        
        logger.info(f"Fix complete:")
        logger.info(f"  - Updated {preprints_fixed} preprint version flags")
        logger.info(f"  - Updated {contributors_fixed} contributor relationship flags")
        
        # Verify the fix worked
        logger.info("Verifying fixes...")
        final_status_ok = check_current_status(db)
        
        if final_status_ok:
            logger.info("All version flags are now consistent!")
            return 0
        else:
            logger.error("Some inconsistencies remain after fixing")
            return 1
        
    except Exception as e:
        logger.error(f"Error during version flag fix: {e}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())