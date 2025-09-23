"""PsyArXiv SQLite Ingestion - Process raw data into main table"""
import json
import logging
from tqdm import tqdm

from osf import config
from osf import database

logger = logging.getLogger("osf.ingestor")

def extract_preprint_data(preprint_id, preprint_data):
    """Extract and flatten PsyArXiv preprint data from API response.
    
    Args:
        preprint_id: The OSF ID of the preprint
        preprint_data: Dictionary containing preprint data from API
        
    Returns:
        dict: Dictionary containing flattened preprint data with JSON columns
    """
    attributes = preprint_data.get('attributes', {})
    relationships = preprint_data.get('relationships', {})
    links = preprint_data.get('links', {})
    embeds = preprint_data.get('embeds', {})
    
    # Extract and structure contributors data
    contributors = []
    contributors_data = embeds.get('contributors', {}).get('data', [])
    for contrib in contributors_data:
        contrib_attrs = contrib.get('attributes', {})
        user_data = contrib.get('embeds', {}).get('users', {}).get('data', {}).get('attributes', {})
        
        contributors.append({
            "full_name": user_data.get('full_name', ''),
            "date_registered": user_data.get('date_registered'),
            "index": contrib_attrs.get('index', 0),
            "bibliographic": contrib_attrs.get('bibliographic', True)
        })
    
    # Extract subjects data - subjects is a nested array in attributes
    subjects = attributes.get('subjects', [])
    
    # Extract tags
    tags = attributes.get('tags', [])
    
    return {
        "id": preprint_id,
        "title": attributes.get('title'),
        "description": attributes.get('description'),
        "date_created": attributes.get('date_created'),
        "date_modified": attributes.get('date_modified'),
        "date_published": attributes.get('date_published'),
        "original_publication_date": attributes.get('original_publication_date'),
        "publication_doi": f"https://doi.org/{attributes.get('doi')}" if attributes.get('doi') else None,
        "preprint_doi": links.get('preprint_doi'),
        "download_url": f"https://osf.io/download/{relationships.get('primary_file', {}).get('data', {}).get('id')}" if relationships.get('primary_file', {}).get('data') else None,
        "license": embeds.get('license', {}).get('data', {}).get('attributes', {}).get('name'),
        "is_published": 1 if attributes.get('is_published') else 0,
        "reviews_state": attributes.get('reviews_state'),
        "version": attributes.get('version'),
        "is_latest_version": 1 if attributes.get('is_latest_version', True) else 0,
        "has_coi": 1 if attributes.get('has_coi') else 0,
        "conflict_of_interest_statement": attributes.get('conflict_of_interest_statement'),
        "has_data_links": attributes.get('has_data_links'),
        "why_no_data": attributes.get('why_no_data'),
        "data_links": json.dumps(attributes.get('data_links', [])),
        "has_prereg_links": attributes.get('has_prereg_links'),
        "why_no_prereg": attributes.get('why_no_prereg'),
        "prereg_links": json.dumps(attributes.get('prereg_links', [])),
        "prereg_link_info": attributes.get('prereg_link_info'),
        # JSON columns for complex structured data
        "contributors": json.dumps(contributors),
        "subjects": json.dumps(subjects),
        "tags": json.dumps(tags)
    }

def process_preprint(db, preprint_id, preprint_data):
    """Process a single preprint into the main table with JSON columns."""
    try:
        # Set timeout for database operations
        db.execute("PRAGMA busy_timeout = 5000")
        
        # Extract flattened preprint data with JSON columns
        preprint = extract_preprint_data(preprint_id, preprint_data)
        db["preprints"].upsert(preprint, pk="id")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing preprint {preprint_id}: {e}")
        return False

def process_all_new_preprints(limit=None):
    """Process all unprocessed preprints from raw_data table."""
    # Initialize database
    db = database.init_db()
    
    # Set pragmas for better performance
    db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA journal_mode = MEMORY")
    
    # Get preprints to process from raw_data that aren't in preprints table yet
    unprocessed = get_unprocessed_preprints(db, limit=limit)
    if not unprocessed:
        logger.info("No unprocessed preprints found")
        return 0, 0, 0
    
    # Progress tracking
    total = len(unprocessed)
    logger.info(f"Processing {total} preprints" + (f" (limit: {limit})" if limit else ""))
    
    # Process each preprint
    processed = success = errors = 0
    with tqdm(total=total, desc="Processing preprints", unit="preprint") as pbar:
        with db.conn:  # Use a single transaction for all processing
            for record in unprocessed:
                preprint_id = record["id"]
                payload = record.get("payload")
                
                if not payload:
                    logger.warning(f"No payload found for preprint {preprint_id}")
                    errors += 1
                    pbar.update(1)
                    continue
                
                # Parse JSON and process
                try:
                    preprint_data = json.loads(payload)
                    
                    if process_preprint(db, preprint_id, preprint_data):
                        success += 1
                    else:
                        errors += 1
                        
                except Exception as e:
                    logger.error(f"Error processing {preprint_id}: {e}")
                    errors += 1
                
                processed += 1
                pbar.update(1)
                
                # Commit periodically
                if processed % 100 == 0:
                    db.conn.commit()
    
    logger.info(f"Processing complete: {success} added, {errors} failed")
    return processed, success, errors

def get_unprocessed_preprints(db, limit=None):
    """Get preprints from raw_data that haven't been processed yet."""
    if "raw_data" not in db.table_names():
        return []
    
    # Find preprints in raw_data that don't exist in preprints table
    query = """
        SELECT r.id, r.payload
        FROM raw_data r
        LEFT JOIN preprints p ON r.id = p.id
        WHERE p.id IS NULL
        ORDER BY r.date_modified ASC
    """
    
    if limit:
        query += f" LIMIT {int(limit)}"
    
    # Convert to list of dicts for consistent access
    results = []
    for row in db.execute(query):
        results.append({"id": row[0], "payload": row[1]})
    
    return results 