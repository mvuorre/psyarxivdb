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
    """Initialize the database with required tables."""
    db = get_db()
    
    # Raw data table to store the JSON responses from OSF API
    if "raw_data" not in db.table_names():
        db["raw_data"].create({
            "id": str,
            "date_created": str,
            "date_modified": str,
            "payload": str,  # Either JSON content or file path depending on config
            "fetch_date": str
        }, pk="id")
        db["raw_data"].create_index(["date_created"])
        logger.info("Created raw_data table")
    
    # Create the main tables for normalized data
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
            "provider": str,
            "is_published": int,  # SQLite boolean (0/1)
            "reviews_state": str,
            "version": int,
            "is_latest_version": int,
            "download_url": str,
            "preprint_doi": str,
            "license": str,
            "tags": str,  # JSON string array of tags
            "has_coi": int,  # SQLite boolean (0/1)
            "conflict_of_interest_statement": str,
            "has_data_links": str,
            "why_no_data": str,
            "data_links": str,  # JSON string array
            "has_prereg_links": str,
            "why_no_prereg": str,
            "prereg_links": str,  # JSON string array
            "prereg_link_info": str
        }, pk="id")
        db["preprints"].create_index(["date_created"])
        db["preprints"].create_index(["provider"])
        logger.info("Created preprints table")

    # Contributors table
    if "contributors" not in db.table_names():
        db["contributors"].create({
            "id": str,
            "full_name": str,
            "given_name": str,
            "middle_names": str,
            "family_name": str,
            "suffix": str,
            "date_registered": str,
            "active": int,
            "timezone": str,
            "locale": str,
            "orcid": str,
            "github": str,
            "scholar": str,
            "profile_websites": str,  # JSON string
            "employment": str,  # JSON string
            "education": str,  # JSON string
            "profile_url": str
        }, pk="id")
        db["contributors"].create_index(["full_name"])
        db["contributors"].create_index(["orcid"])
        logger.info("Created contributors table")

    # Many-to-many between preprints and contributors
    if "preprint_contributors" not in db.table_names():
        db["preprint_contributors"].create({
            "preprint_id": str,
            "contributor_id": str,
            "contributor_index": int,
            "bibliographic": int  # SQLite boolean (0/1)
        }, pk=("preprint_id", "contributor_id"))
        db["preprint_contributors"].create_index(["preprint_id"])
        db["preprint_contributors"].create_index(["contributor_id"])
        logger.info("Created preprint_contributors table")

    # Providers table
    if "providers" not in db.table_names():
        db["providers"].create({
            "id": str
        }, pk="id")
        logger.info("Created providers table")

    # Subjects table
    if "subjects" not in db.table_names():
        db["subjects"].create({
            "id": str,
            "text": str
        }, pk="id")
        logger.info("Created subjects table")

    # Many-to-many between preprints and subjects
    if "preprint_subjects" not in db.table_names():
        db["preprint_subjects"].create({
            "preprint_id": str,
            "subject_id": str
        }, pk=("preprint_id", "subject_id"))
        db["preprint_subjects"].create_index(["preprint_id"])
        logger.info("Created preprint_subjects table")
    
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
        "preprints": ["date_created", "date_modified", "provider"],
        "contributors": ["full_name"],
        "preprint_contributors": ["preprint_id", "contributor_id"],
        "preprint_subjects": ["preprint_id", "subject_id"],
        "preprints_ui": ["date_created", "date_modified", "provider", "fulltext"]
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

def reset_database():
    """Reset the database by dropping all tables and recreating them.
    This should only be used during development when the schema changes.
    
    Returns:
        tuple: (success, message)
    """
    try:
        from osf import tracker
        
        db = get_db()
        
        # Check and drop FTS tables first (they need special handling)
        # Get the list of all tables, including virtual tables
        all_tables = db.execute(
            "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view', 'virtual')"
        ).fetchall()
        
        # First drop any FTS tables which need special handling
        for table in all_tables:
            table_name = table[0]
            table_type = table[1]
            
            # Check if it's an FTS table
            if 'fts' in table_name.lower() or table_type == 'virtual':
                logger.info(f"Dropping virtual table {table_name}")
                try:
                    db.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception as e:
                    logger.warning(f"Could not drop {table_name}: {e}")
        
        # Now drop regular tables
        regular_tables = [t for t in db.table_names() if 'fts' not in t.lower()]
        for table in regular_tables:
            db[table].drop()
            logger.info(f"Dropped table {table}")
        
        # Reinitialize the database
        init_db()
        
        # Reset ingestion status in the tracker database
        reset_count, reset_success = tracker.reset_ingestion_status()
        status_msg = f" and reset {reset_count} preprints for re-ingestion" if reset_success else ""
        
        return (True, f"Reset database successfully. Dropped and recreated {len(regular_tables)} tables{status_msg}.")
    except Exception as e:
        error_msg = f"Error resetting database: {e}"
        logger.error(error_msg)
        return (False, error_msg) 