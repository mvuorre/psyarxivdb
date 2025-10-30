"""
Database utilities for OSF Preprints using sqlite-utils.
"""
import json
import logging
import sqlite_utils
from datetime import datetime
from pathlib import Path

from osf import config

logger = logging.getLogger("osf.database")

def get_db():
    """Get a sqlite-utils Database instance connected to the main database.
    
    Returns:
        sqlite_utils.Database: Database instance
    """
    # Ensure parent directory exists
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite_utils.Database(config.DB_PATH)
    
    # Set a busy timeout to prevent "database is locked" errors
    # This waits up to 10 seconds if the database is locked
    db.execute("PRAGMA busy_timeout = 10000")
    
    return db

def init_db():
    """Initialize the database schema for PsyArXiv preprint data."""
    db = get_db()
    
    # Store original JSON payloads from OSF API for reprocessing and data safety
    if "raw_data" not in db.table_names():
        db["raw_data"].create({
            "id": str,
            "date_created": str,
            "date_modified": str,
            "payload": str,  # Complete JSON response from OSF API
            "fetch_date": str
        }, pk="id")
        db["raw_data"].create_index(["date_created"])
        db["raw_data"].create_index(["date_modified"])
        logger.info("Created raw_data table")
    
    # Main preprints table with JSON columns for complex data - serves Datasette directly
    if "preprints" not in db.table_names():
        db["preprints"].create({
            "id": str,
            "title": str,
            "description": str, 
            "date_created": str,
            "date_modified": str,
            "date_published": str,
            "original_publication_date": str,
            "publication_doi": str,
            "preprint_doi": str,
            "download_url": str,
            "license": str,
            "is_published": int,
            "reviews_state": str,
            "version": int,
            "is_latest_version": int,
            "has_coi": int,
            "conflict_of_interest_statement": str,
            "has_data_links": str,
            "why_no_data": str,
            "data_links": str,  # JSON array
            "has_prereg_links": str,
            "why_no_prereg": str,
            "prereg_links": str,  # JSON array
            "prereg_link_info": str,
            # JSON columns - queryable via SQLite JSON functions in Datasette
            "contributors": str,   # Array of contributor objects with names, ORCID, etc.
            "subjects": str,       # Array of subject classifications  
            "tags": str           # Array of keyword tags
        }, pk="id")
        db["preprints"].create_index(["date_created"])
        db["preprints"].create_index(["date_modified"])
        logger.info("Created preprints table")
    
    # Contributors table - normalized contributor data from OSF users
    if "contributors" not in db.table_names():
        db["contributors"].create({
            "osf_user_id": str,
            "full_name": str,
            "date_registered": str,
            "employment": str,  # JSON array of employment history
        }, pk="osf_user_id")
        logger.info("Created contributors table")
    
    # Preprint-contributors mapping table
    if "preprint_contributors" not in db.table_names():
        db["preprint_contributors"].create({
            "preprint_id": str,
            "osf_user_id": str,
            "author_index": int,
            "bibliographic": int,  # Boolean as int
            "is_latest_version": int,  # Boolean as int, from preprints table
        }, pk=["preprint_id", "osf_user_id"])
        db["preprint_contributors"].create_index(["osf_user_id"])
        db["preprint_contributors"].create_index(["author_index"])
        logger.info("Created preprint_contributors table")
    
    return db

def add_raw_data(preprint_id, data, file_path=None):
    """Add raw preprint data to the database.
    
    Args:
        preprint_id: The OSF ID of the preprint
        data: The preprint JSON data from the API
        file_path: Optional path where the JSON file is stored
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract dates from the data
        attributes = data.get('attributes', {})
        date_created = attributes.get('date_created')
        date_modified = attributes.get('date_modified')
        
        # Determine what to store - either file path or full JSON payload
        if config.STORE_JSON_AS == "path" and file_path:
            payload = str(file_path)
        else:
            payload = json.dumps(data)
        
        # Initialize the database if it doesn't exist
        db = init_db()
        
        # Insert or replace in the raw_data table
        db["raw_data"].upsert({
            "id": preprint_id,
            "date_created": date_created,
            "date_modified": date_modified,
            "payload": payload,
            "fetch_date": datetime.now().isoformat()
        }, pk="id")
        
        return True
    except Exception as e:
        logger.error(f"Error adding raw data for preprint {preprint_id}: {e}")
        return False

def get_most_recent_date():
    """Get the most recent preprint date_created from the raw_data table.
    
    Returns:
        str: ISO format date of the most recent preprint, or None if no preprints found
    """
    # Check if database file exists
    if not Path(config.DB_PATH).exists():
        logger.info("Database file does not exist yet")
        return None
    
    db = get_db()
    
    if "raw_data" not in db.table_names():
        logger.info("raw_data table does not exist yet")
        return None
    
    # Query for the most recent date - properly handling ISO date strings
    # SQLite will sort ISO8601 strings chronologically if the format is consistent
    result = db.query("""
        SELECT date_created 
        FROM raw_data 
        WHERE date_created IS NOT NULL 
        ORDER BY 
            CASE
                WHEN date_created LIKE '____-__-__T__:__:__.__%' THEN date_created 
                WHEN date_created LIKE '____-__-__T__:__:__%' THEN date_created
                ELSE '0001-01-01T00:00:00.000000'
            END DESC
        LIMIT 1
    """)
    records = list(result)
    
    if records:
        most_recent = records[0]["date_created"]
        logger.info(f"Found most recent preprint date_created: {most_recent}")
        return most_recent
    
    logger.info("No preprints found in database")
    return None

def get_preprint_count():
    """Get the total number of preprints in the database.
    
    Returns:
        int: The number of preprints in the database
    """
    db = get_db()
    if "preprints" not in db.table_names():
        return 0
    return db["preprints"].count

def is_preprint_harvested(preprint_id):
    """Check if a preprint has already been harvested.
    
    Args:
        preprint_id: The OSF ID of the preprint
        
    Returns:
        bool: True if the preprint is already in the database
    """
    # Check if database file exists
    if not Path(config.DB_PATH).exists():
        return False
    
    db = get_db()
    
    if "raw_data" not in db.table_names():
        return False
    
    return db["raw_data"].count_where("id = ?", [preprint_id]) > 0

def get_unprocessed_preprints():
    """Get list of preprints that are in raw_data but not in the preprints table.
    
    Returns:
        list: List of dictionaries containing raw preprint data
    """
    # Check if database file exists
    if not Path(config.DB_PATH).exists():
        logger.info("Database file does not exist yet")
        return []
    
    db = get_db()
    
    if "raw_data" not in db.table_names() or "preprints" not in db.table_names():
        return []
    
    # Find preprints that need processing
    results = db.query("""
        SELECT raw.id, raw.date_created, raw.date_modified, raw.payload, raw.fetch_date
        FROM raw_data AS raw
        LEFT JOIN preprints AS p ON raw.id = p.id
        WHERE p.id IS NULL
    """)
    
    return list(results)

def commit_db():
    """Manually commit any pending database changes."""
    db = get_db()
    db.conn.commit()
    return True

def add_raw_data_batch(records, db=None):
    """Add multiple raw preprint data records to the database in a batch.
    
    Args:
        records: List of dictionaries containing preprint data
        db: Optional database connection to use (to avoid locking)
        
    Returns:
        int: Number of records successfully added
    """
    try:
        # Use provided db or initialize a new one
        if db is None:
            db = init_db()
        
        # Insert all records in a single transaction
        db["raw_data"].upsert_all(records, pk="id")
        
        return len(records)
    except Exception as e:
        logger.error(f"Error adding batch of {len(records)} records: {e}")
        return 0

def get_database_size():
    """Get the current size of the database file in bytes.
    
    Returns:
        int: Size of the database file in bytes, or 0 if it doesn't exist
    """
    if Path(config.DB_PATH).exists():
        return Path(config.DB_PATH).stat().st_size
    return 0

def recreate_indexes():
    """Recreate all indexes in the database for optimization.
    
    Returns:
        int: Number of indexes recreated
    """
    db = get_db()
    index_count = 0
    
    # Dictionary of table name to list of columns to index
    indexes = {
        "preprints": ["date_created", "date_modified"],
        "raw_data": ["date_created", "date_modified"]
    }
    
    # For each table, drop and recreate indexes
    for table, columns in indexes.items():
        if table not in db.table_names():
            continue
        
        # Simple approach: drop all indexes with our naming convention and recreate
        # This avoids the need to parse the PRAGMA index_list results
        for column in columns:
            index_name = f"idx_{table}_{column}"
            # Drop the index if it exists
            db.execute(f"DROP INDEX IF EXISTS {index_name}")
            # Recreate the index
            db.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
            index_count += 1
    
    return index_count

 