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

CONTRIBUTORS_WITH_COUNTS_VIEW_SQL = """
CREATE VIEW contributors_with_counts AS
SELECT
    c.osf_user_id,
    c.full_name,
    c.date_registered,
    c.employment,
    COALESCE(counts.n_preprints, 0) AS n_preprints
FROM contributors AS c
LEFT JOIN (
    SELECT
        pc.osf_user_id,
        COUNT(DISTINCT pc.preprint_id) AS n_preprints
    FROM preprint_contributors AS pc
    JOIN preprints AS p ON p.id = pc.preprint_id
    WHERE pc.bibliographic = 1
      AND pc.is_latest_version = 1
      AND p.is_latest_version = 1
    GROUP BY pc.osf_user_id
) AS counts ON counts.osf_user_id = c.osf_user_id
"""

CONTRIBUTOR_FIRST_APPEARANCE_REFRESH_SQL = """
INSERT INTO dashboard_contributor_first_appearance_by_date (date, count)
SELECT
    first_date AS date,
    COUNT(*) AS count
FROM (
    SELECT
        pc.osf_user_id AS contributor_id,
        MIN(DATE(p.date_created)) AS first_date
    FROM preprints AS p
    JOIN preprint_contributors AS pc ON pc.preprint_id = p.id
    WHERE p.is_latest_version = 1
      AND pc.bibliographic = 1
      AND pc.is_latest_version = 1
    GROUP BY pc.osf_user_id
) AS first_appearances
WHERE first_date IS NOT NULL
GROUP BY first_date
ORDER BY first_date
"""

AFFILIATION_FIRST_APPEARANCE_REFRESH_SQL = """
INSERT INTO dashboard_affiliation_first_appearance_by_date (date, count)
SELECT
    first_date AS date,
    COUNT(*) AS count
FROM (
    SELECT
        i.id AS institution_id,
        MIN(DATE(p.date_created)) AS first_date
    FROM preprints AS p
    JOIN preprint_contributors AS pc ON pc.preprint_id = p.id
    JOIN contributor_affiliations AS ca ON ca.contributor_id = pc.osf_user_id
    JOIN institutions AS i ON i.id = ca.institution_id
    WHERE p.is_latest_version = 1
      AND pc.bibliographic = 1
      AND pc.is_latest_version = 1
      AND ca.end_date IS NULL
      AND i.name IS NOT NULL
    GROUP BY i.id
) AS first_appearances
WHERE first_date IS NOT NULL
GROUP BY first_date
ORDER BY first_date
"""

CONTRIBUTORS_ON_PREPRINTS_REFRESH_SQL = """
INSERT INTO dashboard_contributors_on_preprints_by_date (date, count)
SELECT
    DATE(p.date_created) AS date,
    COUNT(DISTINCT pc.osf_user_id) AS count
FROM preprints AS p
JOIN preprint_contributors AS pc ON pc.preprint_id = p.id
WHERE p.is_latest_version = 1
  AND pc.bibliographic = 1
  AND pc.is_latest_version = 1
GROUP BY date
ORDER BY date
"""

TOP_CONTRIBUTORS_REFRESH_SQL = """
INSERT INTO dashboard_top_contributors (osf_user_id, contributor_name, preprint_count)
SELECT
    c.osf_user_id,
    c.full_name AS contributor_name,
    counts.n_preprints AS preprint_count
FROM contributors AS c
JOIN (
    SELECT
        pc.osf_user_id,
        COUNT(DISTINCT pc.preprint_id) AS n_preprints
    FROM preprint_contributors AS pc
    JOIN preprints AS p ON p.id = pc.preprint_id
    WHERE pc.bibliographic = 1
      AND pc.is_latest_version = 1
      AND p.is_latest_version = 1
    GROUP BY pc.osf_user_id
) AS counts ON counts.osf_user_id = c.osf_user_id
WHERE c.full_name IS NOT NULL
  AND counts.n_preprints > 0
"""

AFFILIATIONS_ON_PREPRINTS_REFRESH_SQL = """
INSERT INTO dashboard_affiliations_on_preprints_by_date (date, count)
SELECT
    DATE(p.date_created) AS date,
    COUNT(DISTINCT i.id) AS count
FROM preprints AS p
JOIN preprint_contributors AS pc ON pc.preprint_id = p.id
JOIN contributor_affiliations AS ca ON ca.contributor_id = pc.osf_user_id
JOIN institutions AS i ON i.id = ca.institution_id
WHERE p.is_latest_version = 1
  AND pc.bibliographic = 1
  AND pc.is_latest_version = 1
  AND ca.end_date IS NULL
  AND i.name IS NOT NULL
GROUP BY date
ORDER BY date
"""

QUERY_SUPPORT_INDEXES = (
    """
    CREATE INDEX IF NOT EXISTS idx_preprint_contributors_bibliographic_latest_user_preprint
    ON preprint_contributors (osf_user_id, preprint_id)
    WHERE bibliographic = 1 AND is_latest_version = 1
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_contributor_affiliations_current_contributor_institution
    ON contributor_affiliations (contributor_id, institution_id)
    WHERE end_date IS NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_contributor_affiliations_current_institution_contributor
    ON contributor_affiliations (institution_id, contributor_id)
    WHERE end_date IS NULL
    """,
)


def get_db(db_path=None):
    """Get a sqlite-utils Database instance connected to the main database.
    
    Returns:
        sqlite_utils.Database: Database instance
    """
    db_path = Path(db_path or config.DB_PATH)

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite_utils.Database(db_path)
    
    # Set a busy timeout to prevent "database is locked" errors
    # This waits up to 10 seconds if the database is locked
    db.execute("PRAGMA busy_timeout = 10000")
    
    return db

def ensure_derived_views(db):
    """Create derived views for common queries."""
    db.execute("DROP VIEW IF EXISTS contributors_with_counts")
    db.execute(CONTRIBUTORS_WITH_COUNTS_VIEW_SQL)


def ensure_dashboard_query_support(db):
    """Create the small tables and indexes used by the dashboard."""
    for index_sql in QUERY_SUPPORT_INDEXES:
        db.execute(index_sql)

    if "dashboard_contributor_first_appearance_by_date" not in db.table_names():
        db["dashboard_contributor_first_appearance_by_date"].create({
            "date": str,
            "count": int,
        }, pk="date")
        logger.info("Created dashboard_contributor_first_appearance_by_date table")

    if "dashboard_affiliation_first_appearance_by_date" not in db.table_names():
        db["dashboard_affiliation_first_appearance_by_date"].create({
            "date": str,
            "count": int,
        }, pk="date")
        logger.info("Created dashboard_affiliation_first_appearance_by_date table")

    if "dashboard_contributors_on_preprints_by_date" not in db.table_names():
        db["dashboard_contributors_on_preprints_by_date"].create({
            "date": str,
            "count": int,
        }, pk="date")
        logger.info("Created dashboard_contributors_on_preprints_by_date table")

    if "dashboard_top_contributors" not in db.table_names():
        db["dashboard_top_contributors"].create({
            "osf_user_id": str,
            "contributor_name": str,
            "preprint_count": int,
        }, pk="osf_user_id")
        logger.info("Created dashboard_top_contributors table")

    if "dashboard_affiliations_on_preprints_by_date" not in db.table_names():
        db["dashboard_affiliations_on_preprints_by_date"].create({
            "date": str,
            "count": int,
        }, pk="date")
        logger.info("Created dashboard_affiliations_on_preprints_by_date table")


def refresh_dashboard_summary_tables(db=None):
    """Rebuild the small summary tables used by the dashboard."""
    db = db or get_db()
    ensure_dashboard_query_support(db)

    with db.conn:
        db.execute("DELETE FROM dashboard_contributor_first_appearance_by_date")
        db.execute(CONTRIBUTOR_FIRST_APPEARANCE_REFRESH_SQL)
        db.execute("DELETE FROM dashboard_affiliation_first_appearance_by_date")
        db.execute(AFFILIATION_FIRST_APPEARANCE_REFRESH_SQL)
        db.execute("DELETE FROM dashboard_contributors_on_preprints_by_date")
        db.execute(CONTRIBUTORS_ON_PREPRINTS_REFRESH_SQL)
        db.execute("DELETE FROM dashboard_top_contributors")
        db.execute(TOP_CONTRIBUTORS_REFRESH_SQL)
        db.execute("DELETE FROM dashboard_affiliations_on_preprints_by_date")
        db.execute(AFFILIATIONS_ON_PREPRINTS_REFRESH_SQL)

    summary_counts = {
        "dashboard_contributor_first_appearance_by_date": db.execute(
            "SELECT COUNT(*) FROM dashboard_contributor_first_appearance_by_date"
        ).fetchone()[0],
        "dashboard_affiliation_first_appearance_by_date": db.execute(
            "SELECT COUNT(*) FROM dashboard_affiliation_first_appearance_by_date"
        ).fetchone()[0],
        "dashboard_contributors_on_preprints_by_date": db.execute(
            "SELECT COUNT(*) FROM dashboard_contributors_on_preprints_by_date"
        ).fetchone()[0],
        "dashboard_top_contributors": db.execute(
            "SELECT COUNT(*) FROM dashboard_top_contributors"
        ).fetchone()[0],
        "dashboard_affiliations_on_preprints_by_date": db.execute(
            "SELECT COUNT(*) FROM dashboard_affiliations_on_preprints_by_date"
        ).fetchone()[0],
    }
    logger.info(
        "Refreshed dashboard summary tables: %s",
        ", ".join(f"{table_name}={row_count}" for table_name, row_count in summary_counts.items()),
    )

    return summary_counts


def init_db(db=None):
    """Initialize the database schema for PsyArXiv preprint data."""
    db = db or get_db()
    
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
    
    # Subjects table - normalized subject classifications
    if "subjects" not in db.table_names():
        db["subjects"].create({
            "id": str,  # OSF subject ID 
            "text": str,  # Subject name
            "parent_id": str,  # For hierarchical structure, NULL for top-level
        }, pk="id")
        db["subjects"].create_index(["parent_id"])
        logger.info("Created subjects table")
    
    # Preprint-subjects mapping table
    if "preprint_subjects" not in db.table_names():
        db["preprint_subjects"].create({
            "preprint_id": str,
            "subject_id": str,
            "is_latest_version": int,  # Boolean as int
        }, pk=["preprint_id", "subject_id"])
        db["preprint_subjects"].create_index(["subject_id"])
        logger.info("Created preprint_subjects table")
    
    # Tags table - normalized tag data with usage tracking
    if "tags" not in db.table_names():
        db["tags"].create({
            "id": int,  # Auto-increment primary key
            "tag_text": str,  # Cleaned and normalized tag text
            "use_count": int,  # Number of preprints using this tag
        }, pk="id")
        db["tags"].create_index(["tag_text"], unique=True)
        logger.info("Created tags table")
    
    # Preprint-tags mapping table
    if "preprint_tags" not in db.table_names():
        db["preprint_tags"].create({
            "preprint_id": str,
            "tag_id": int,
            "is_latest_version": int,  # Boolean as int
        }, pk=["preprint_id", "tag_id"])
        db["preprint_tags"].create_index(["tag_id"])
        logger.info("Created preprint_tags table")
    
    # Institutions table - for future ROR integration
    if "institutions" not in db.table_names():
        db["institutions"].create({
            "id": int,  # Auto-increment primary key
            "name": str,  # Institution name (cleaned)
            "ror_id": str,  # ROR identifier (NULL for now)
            "country": str,  # Country (NULL for now)
            "metadata": str,  # JSON field for additional ROR data
        }, pk="id")
        db["institutions"].create_index(["name"])
        db["institutions"].create_index(["ror_id"])
        logger.info("Created institutions table")
    
    # Contributor-institution affiliations table
    if "contributor_affiliations" not in db.table_names():
        db["contributor_affiliations"].create({
            "contributor_id": str,  # osf_user_id
            "institution_id": int,
            "position": str,  # Job title/position
            "start_date": str,  # Employment start date
            "end_date": str,  # Employment end date (NULL if current)
        }, pk=["contributor_id", "institution_id", "start_date"])
        db["contributor_affiliations"].create_index(["institution_id"])
        logger.info("Created contributor_affiliations table")

    ensure_dashboard_query_support(db)
    ensure_derived_views(db)
    
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
        "raw_data": ["date_created", "date_modified"],
        "subjects": ["parent_id"],
        "preprint_subjects": ["subject_id"],
        "tags": ["tag_text"],
        "preprint_tags": ["tag_id"],
        "institutions": ["name", "ror_id"],
        "contributor_affiliations": ["institution_id"],
        "preprint_contributors": ["osf_user_id", "author_index"]
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

    for index_sql in QUERY_SUPPORT_INDEXES:
        db.execute(index_sql)
        index_count += 1
    
    return index_count

 
