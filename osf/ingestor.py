"""PsyArXiv SQLite Ingestion - Process raw data into main table"""
import json
import logging
import re
from tqdm import tqdm

from osf import config
from osf import database

logger = logging.getLogger("osf.ingestor")

def clean_and_parse_tags(tags_data):
    """Clean and parse tags from various input formats.
    
    Args:
        tags_data: List of tag strings or strings with comma/semicolon separation
        
    Returns:
        list: List of cleaned tag strings
    """
    if not tags_data:
        return []
    
    cleaned_tags = []
    
    for tag in tags_data:
        if not tag or not isinstance(tag, str):
            continue
            
        # Split on commas and semicolons to handle user input errors
        split_tags = re.split(r'[,;]+', tag)
        
        for split_tag in split_tags:
            # Clean the tag: strip whitespace, normalize case
            clean_tag = split_tag.strip()
            if clean_tag:
                # Convert to lowercase for normalization but preserve original case in display
                cleaned_tags.append(clean_tag)
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(cleaned_tags))

def normalize_tag_text(tag_text):
    """Normalize tag text for storage and comparison.
    
    Args:
        tag_text: Original tag text
        
    Returns:
        str: Normalized tag text
    """
    if not tag_text:
        return ""
    
    # Strip whitespace, normalize to lowercase
    normalized = tag_text.strip().lower()
    
    # Remove excessive whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized

def get_or_create_tag_id(db, tag_text):
    """Get existing tag ID or create new tag, updating usage count.
    
    Args:
        db: Database instance
        tag_text: Tag text to process
        
    Returns:
        int: Tag ID
    """
    if not tag_text:
        return None
    
    normalized_tag = normalize_tag_text(tag_text)
    if not normalized_tag:
        return None
    
    # Try to find existing tag
    existing = list(db.execute(
        "SELECT id, use_count FROM tags WHERE tag_text = ?", 
        [normalized_tag]
    ))
    
    if existing:
        tag_id = existing[0][0]  # First column: id
        current_count = existing[0][1]  # Second column: use_count
        # Update usage count
        db.execute(
            "UPDATE tags SET use_count = ? WHERE id = ?",
            [current_count + 1, tag_id]
        )
        return tag_id
    else:
        # Create new tag
        result = db["tags"].insert({
            "tag_text": normalized_tag,
            "use_count": 1
        })
        return result.last_pk

def get_or_create_institution_id(db, institution_name):
    """Get existing institution ID or create new institution.
    
    Args:
        db: Database instance
        institution_name: Institution name to process
        
    Returns:
        int: Institution ID or None if invalid
    """
    if not institution_name or not isinstance(institution_name, str):
        return None
    
    # Clean institution name
    clean_name = institution_name.strip()
    if not clean_name:
        return None
    
    # Try to find existing institution (case-insensitive)
    existing = list(db.execute(
        "SELECT id FROM institutions WHERE LOWER(name) = LOWER(?)", 
        [clean_name]
    ))
    
    if existing:
        return existing[0][0]  # First column: id
    else:
        # Create new institution
        result = db["institutions"].insert({
            "name": clean_name,
            "ror_id": None,
            "country": None,
            "metadata": json.dumps({})
        })
        return result.last_pk

def process_subjects_data(db, subjects_data):
    """Process subjects data and ensure all subjects and hierarchies are stored.
    
    Args:
        db: Database instance
        subjects_data: List of subject classification arrays
        
    Returns:
        list: List of subject IDs that were processed
    """
    if not subjects_data:
        return []
    
    all_subject_ids = []
    
    for subject_hierarchy in subjects_data:
        if not subject_hierarchy or not isinstance(subject_hierarchy, list):
            continue
        
        parent_id = None
        
        # Process each level in the hierarchy
        for level_idx, subject in enumerate(subject_hierarchy):
            if not isinstance(subject, dict):
                continue
                
            subject_id = subject.get('id')
            subject_text = subject.get('text')
            
            if not subject_id or not subject_text:
                continue
            
            # Upsert the subject
            db["subjects"].upsert({
                "id": subject_id,
                "text": subject_text,
                "parent_id": parent_id
            }, pk="id")
            
            all_subject_ids.append(subject_id)
            
            # Set this subject as parent for next level
            parent_id = subject_id
    
    return all_subject_ids

def extract_employment_data(employment_json):
    """Extract and parse employment data from contributor JSON.
    
    Args:
        employment_json: JSON string containing employment array
        
    Returns:
        list: List of employment dictionaries with institution info
    """
    if not employment_json:
        return []
    
    try:
        employment_data = json.loads(employment_json) if isinstance(employment_json, str) else employment_json
        if not employment_data or not isinstance(employment_data, list):
            return []
        
        parsed_employment = []
        for employment in employment_data:
            if not isinstance(employment, dict):
                continue
                
            # Extract institution name and employment details
            institution_name = employment.get('institution')
            position = employment.get('title') or employment.get('position')
            start_date = employment.get('startMonth') or employment.get('start_date')
            end_date = employment.get('endMonth') or employment.get('end_date')
            
            if institution_name:
                parsed_employment.append({
                    'institution': institution_name,
                    'position': position,
                    'start_date': start_date,
                    'end_date': end_date
                })
        
        return parsed_employment
        
    except (json.JSONDecodeError, TypeError):
        return []

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

def extract_contributor_data(preprint_id, preprint_data):
    """Extract contributor and relationship data from API response.
    
    Args:
        preprint_id: The OSF ID of the preprint
        preprint_data: Dictionary containing preprint data from API
        
    Returns:
        tuple: (contributors_dict, relationships_list)
    """
    attributes = preprint_data.get('attributes', {})
    embeds = preprint_data.get('embeds', {})
    contributors_data = embeds.get('contributors', {}).get('data', [])
    
    contributors = {}  # osf_user_id -> contributor_data
    relationships = []  # preprint-contributor relationships
    
    for contrib in contributors_data:
        contrib_attrs = contrib.get('attributes', {})
        user_embeds = contrib.get('embeds', {})
        user_data = user_embeds.get('users', {}).get('data', {})
        
        if not user_data:
            continue
            
        user_attrs = user_data.get('attributes', {})
        osf_user_id = user_data.get('id')
        
        if not osf_user_id:
            continue
        
        # Extract contributor data
        contributor_data = {
            'osf_user_id': osf_user_id,
            'full_name': user_attrs.get('full_name', ''),
            'date_registered': user_attrs.get('date_registered'),
            'employment': json.dumps(user_attrs.get('employment', []))
        }
        
        # Store contributor (will be deduplicated later)
        contributors[osf_user_id] = contributor_data
        
        # Extract relationship data
        relationship_data = {
            'preprint_id': preprint_id,
            'osf_user_id': osf_user_id,
            'author_index': contrib_attrs.get('index', 0),
            'bibliographic': 1 if contrib_attrs.get('bibliographic', True) else 0,
            'is_latest_version': 1 if attributes.get('is_latest_version', True) else 0
        }
        
        relationships.append(relationship_data)
    
    return contributors, relationships

def process_preprint(db, preprint_id, preprint_data):
    """Process a single preprint into the main table with JSON columns and populate all normalized tables."""
    try:
        # Set timeout for database operations
        db.execute("PRAGMA busy_timeout = 5000")
        
        # Extract flattened preprint data with JSON columns
        preprint = extract_preprint_data(preprint_id, preprint_data)
        db["preprints"].upsert(preprint, pk="id")
        
        # Get basic preprint attributes for normalized processing
        attributes = preprint_data.get('attributes', {})
        is_latest = 1 if attributes.get('is_latest_version', True) else 0
        
        # Process subjects data
        subjects_data = attributes.get('subjects', [])
        if subjects_data:
            subject_ids = process_subjects_data(db, subjects_data)
            if subject_ids:
                # Create preprint-subject relationships
                subject_relationships = []
                for subject_id in subject_ids:
                    subject_relationships.append({
                        'preprint_id': preprint_id,
                        'subject_id': subject_id,
                        'is_latest_version': is_latest
                    })
                if subject_relationships:
                    db["preprint_subjects"].upsert_all(subject_relationships, pk=["preprint_id", "subject_id"])
        
        # Process tags data
        tags_data = attributes.get('tags', [])
        if tags_data:
            cleaned_tags = clean_and_parse_tags(tags_data)
            if cleaned_tags:
                tag_relationships = []
                for tag_text in cleaned_tags:
                    tag_id = get_or_create_tag_id(db, tag_text)
                    if tag_id:
                        tag_relationships.append({
                            'preprint_id': preprint_id,
                            'tag_id': tag_id,
                            'is_latest_version': is_latest
                        })
                if tag_relationships:
                    db["preprint_tags"].upsert_all(tag_relationships, pk=["preprint_id", "tag_id"])
        
        # Extract and process contributor data
        contributors, relationships = extract_contributor_data(preprint_id, preprint_data)
        
        # Upsert contributors (will deduplicate by osf_user_id)
        if contributors:
            db["contributors"].upsert_all(contributors.values(), pk="osf_user_id")
            
            # Process employment/affiliation data for each contributor
            for osf_user_id, contributor_data in contributors.items():
                employment_json = contributor_data.get('employment', '[]')
                employment_list = extract_employment_data(employment_json)
                
                if employment_list:
                    affiliation_relationships = []
                    for employment in employment_list:
                        institution_id = get_or_create_institution_id(db, employment['institution'])
                        if institution_id:
                            affiliation_relationships.append({
                                'contributor_id': osf_user_id,
                                'institution_id': institution_id,
                                'position': employment.get('position'),
                                'start_date': employment.get('start_date'),
                                'end_date': employment.get('end_date')
                            })
                    
                    if affiliation_relationships:
                        db["contributor_affiliations"].upsert_all(
                            affiliation_relationships, 
                            pk=["contributor_id", "institution_id", "start_date"]
                        )
        
        # Upsert preprint-contributor relationships
        if relationships:
            db["preprint_contributors"].upsert_all(relationships, pk=["preprint_id", "osf_user_id"])
        
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
    
    # Fix is_latest_version flags after processing if any preprints were successfully added
    if success > 0:
        logger.info("Fixing is_latest_version flags after batch processing")
        preprints_fixed = fix_latest_version_flags(db)
        contributors_fixed = fix_contributor_latest_version_flags(db)
        logger.info(f"Version flag fixes: {preprints_fixed} preprints, {contributors_fixed} contributor relationships")
    
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

def fix_latest_version_flags(db):
    """Fix is_latest_version flags in preprints table based on actual version numbers.
    
    This function groups preprints by their base ID (everything before '_v') and ensures
    only the highest version number has is_latest_version=1.
    
    Args:
        db: sqlite-utils database instance
        
    Returns:
        int: Number of preprints that had their is_latest_version flag updated
    """
    logger.info("Starting to fix is_latest_version flags in preprints table")
    
    # Query to find all preprints grouped by base ID with their versions
    query = """
        SELECT 
            id,
            version,
            is_latest_version,
            CASE 
                WHEN id LIKE '%_v%' THEN SUBSTR(id, 1, INSTR(id, '_v') - 1)
                ELSE id
            END as base_id
        FROM preprints
        ORDER BY base_id, version DESC
    """
    
    results = list(db.execute(query))
    if not results:
        logger.info("No preprints found to process")
        return 0
    
    # Group by base_id and track what needs to be updated
    updates_needed = []
    current_base_id = None
    highest_version_seen = False
    
    for row in results:
        preprint_id = row[0]
        version = row[1]
        current_is_latest = row[2]
        base_id = row[3]
        
        # If we're starting a new base_id group, reset the flag
        if base_id != current_base_id:
            current_base_id = base_id
            highest_version_seen = False
        
        # The first record for each base_id (sorted by version DESC) should be latest
        should_be_latest = not highest_version_seen
        
        # If the current flag doesn't match what it should be, mark for update
        if (should_be_latest and current_is_latest != 1) or (not should_be_latest and current_is_latest != 0):
            updates_needed.append({
                'id': preprint_id,
                'is_latest_version': 1 if should_be_latest else 0
            })
        
        # Mark that we've seen the highest version for this base_id
        if not highest_version_seen:
            highest_version_seen = True
    
    # Apply the updates
    if updates_needed:
        logger.info(f"Updating is_latest_version for {len(updates_needed)} preprints")
        with db.conn:
            for update in updates_needed:
                db.execute(
                    "UPDATE preprints SET is_latest_version = ? WHERE id = ?",
                    [update['is_latest_version'], update['id']]
                )
        logger.info(f"Successfully updated {len(updates_needed)} preprints")
    else:
        logger.info("No updates needed - all is_latest_version flags are correct")
    
    return len(updates_needed)

def fix_contributor_latest_version_flags(db):
    """Fix is_latest_version flags in preprint_contributors table to match preprints table.
    
    Args:
        db: sqlite-utils database instance
        
    Returns:
        int: Number of contributor relationships that had their is_latest_version flag updated
    """
    logger.info("Starting to fix is_latest_version flags in preprint_contributors table")
    
    # Update contributor table to match the corrected preprints table
    query = """
        UPDATE preprint_contributors 
        SET is_latest_version = (
            SELECT p.is_latest_version 
            FROM preprints p 
            WHERE p.id = preprint_contributors.preprint_id
        )
        WHERE EXISTS (
            SELECT 1 FROM preprints p 
            WHERE p.id = preprint_contributors.preprint_id
            AND p.is_latest_version != preprint_contributors.is_latest_version
        )
    """
    
    with db.conn:
        cursor = db.execute(query)
        updated_count = cursor.rowcount
    
    logger.info(f"Updated is_latest_version for {updated_count} contributor relationships")
    return updated_count