"""Tracker for OSF Preprints harvesting - Track harvested preprints in SQLite DB"""
import logging
import sqlite_utils
from datetime import datetime
from pathlib import Path

from osf import config

logger = logging.getLogger("osf.tracker")

def get_tracker_db():
    """Get a sqlite-utils Database instance for the tracker database."""
    # Ensure parent directory exists
    config.TRACKER_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite_utils.Database(config.TRACKER_DB_PATH)
    
    # Set busy timeout to prevent locks
    db.execute("PRAGMA busy_timeout = 10000")
    
    return db

def init_tracker_db():
    """Initialize the tracker database with required tables."""
    db = get_tracker_db()
    
    # Create table if it doesn't exist
    if "harvested_preprints" not in db.table_names():
        db["harvested_preprints"].create({
            "id": str,               # Preprint ID from OSF
            "date_created": str,     # When the preprint was created on OSF
            "date_modified": str,    # When the preprint was last modified on OSF
            "date_harvested": str,   # When we harvested it
            "file_path": str,        # Path to the JSON file
            "is_ingested": int       # Whether it has been ingested (0/1)
        }, pk="id")
        
        # Create indexes
        db["harvested_preprints"].create_index(["date_created"])
        db["harvested_preprints"].create_index(["date_harvested"])
        db["harvested_preprints"].create_index(["is_ingested"])
        
        logger.info("Created harvested_preprints table in tracker database")
    
    return db

def add_harvested_preprint(preprint_id, date_created, date_modified, file_path):
    """Record a preprint that has been harvested."""
    try:
        db = init_tracker_db()
        
        db["harvested_preprints"].upsert({
            "id": preprint_id,
            "date_created": date_created,
            "date_modified": date_modified,
            "date_harvested": datetime.now().isoformat(),
            "file_path": str(file_path),
            "is_ingested": 0  # Default to not ingested
        }, pk="id")
        
        return True
    except Exception as e:
        logger.error(f"Error recording harvested preprint {preprint_id}: {e}")
        return False

def mark_as_ingested(preprint_id):
    """Mark a preprint as having been ingested."""
    try:
        db = get_tracker_db()
        db["harvested_preprints"].update(preprint_id, {"is_ingested": 1})
        return True
    except Exception as e:
        logger.error(f"Error marking preprint {preprint_id} as ingested: {e}")
        return False

def get_most_recent_harvest_date():
    """Get the most recent date_harvested from the tracker database."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return None
    
    # Query for the most recent date
    result = db.query("""
        SELECT date_harvested 
        FROM harvested_preprints 
        ORDER BY date_harvested DESC
        LIMIT 1
    """)
    
    records = list(result)
    if records:
        return records[0]["date_harvested"]
    
    return None

def get_most_recent_preprint_date():
    """Get the most recent preprint date_created from the tracker database."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return None
    
    # Query for the most recent date
    result = db.query("""
        SELECT date_created 
        FROM harvested_preprints 
        WHERE date_created IS NOT NULL 
        ORDER BY date_created DESC
        LIMIT 1
    """)
    
    records = list(result)
    if records:
        return records[0]["date_created"]
    
    return None

def is_preprint_harvested(preprint_id):
    """Check if a preprint has already been harvested."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return False
    
    return db["harvested_preprints"].count_where("id = ?", [preprint_id]) > 0

def get_pending_ingestion_preprints(limit=None):
    """Get preprints that have been harvested but not yet ingested."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return []
    
    # Find preprints that need processing
    query = """
        SELECT id, date_created, date_modified, date_harvested, file_path
        FROM harvested_preprints
        WHERE is_ingested = 0
        ORDER BY date_created ASC
    """
    
    if limit:
        query += f" LIMIT {int(limit)}"
    
    return list(db.query(query))

def get_harvested_preprint_count():
    """Get the total number of preprints in the tracker database."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return 0
    
    return db["harvested_preprints"].count

def get_pending_ingestion_count():
    """Get the number of preprints pending ingestion."""
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        return 0
    
    # Use simple count_where
    return db["harvested_preprints"].count_where("is_ingested = 0")

def reset_ingestion_status():
    """Reset the ingestion status of all preprints in the tracker database.
    This should be called when the main database is reset, so all harvested
    preprints can be re-ingested.
    
    Returns:
        tuple: (reset_count, success_flag)
    """
    try:
        db = get_tracker_db()
        
        if "harvested_preprints" not in db.table_names():
            logger.info("No harvested_preprints table found in tracker database")
            return (0, True)
        
        # Count how many preprints are already ingested
        ingested_count = db["harvested_preprints"].count_where("is_ingested = 1")
        
        if ingested_count == 0:
            logger.info("No ingested preprints found in tracker database")
            return (0, True)
        
        # Reset all preprints to not ingested
        db.execute("UPDATE harvested_preprints SET is_ingested = 0 WHERE is_ingested = 1")
        
        logger.info(f"Reset ingestion status for {ingested_count} preprints in tracker database")
        return (ingested_count, True)
    
    except Exception as e:
        error_msg = f"Error resetting ingestion status: {e}"
        logger.error(error_msg)
        return (0, False)

def validate_harvested_file_paths():
    """Check if the file paths in the tracker database exist.
    
    Returns:
        tuple: (valid_count, invalid_count, total_count)
    """
    db = get_tracker_db()
    
    if "harvested_preprints" not in db.table_names():
        logger.info("No harvested_preprints table found in tracker database")
        return (0, 0, 0)
    
    # Get all file paths
    results = db.query("SELECT id, file_path FROM harvested_preprints")
    records = list(results)
    total_count = len(records)
    
    if total_count == 0:
        logger.info("No harvested preprints found in tracker database")
        return (0, 0, 0)
    
    # Check if files exist
    valid_count = 0
    invalid_count = 0
    invalid_ids = []
    
    for record in records:
        file_path = record.get("file_path")
        preprint_id = record.get("id")
        
        if file_path and Path(file_path).is_file():
            valid_count += 1
        else:
            invalid_count += 1
            invalid_ids.append(preprint_id)
    
    if invalid_count > 0:
        logger.warning(f"Found {invalid_count} preprints with invalid file paths")
        # Log a subset of invalid IDs for debugging
        sample_ids = invalid_ids[:5] if len(invalid_ids) > 5 else invalid_ids
        logger.warning(f"Sample invalid preprint IDs: {', '.join(sample_ids)}")
    
    return (valid_count, invalid_count, total_count) 