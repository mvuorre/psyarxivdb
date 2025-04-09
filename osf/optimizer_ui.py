"""
Preprints UI Table Optimizer

Creates and maintains a denormalized preprints_ui table for efficient UI access.
This table joins information from preprints, contributors, subjects, etc. into a single
optimized table for fast querying.
"""
import json
import logging
from datetime import datetime
from tqdm import tqdm

from osf import database

logger = logging.getLogger("osf.optimizer_ui")

def create_preprints_ui_table():
    """Create the preprints_ui table for fast UI access."""
    db = database.get_db()
    
    # Create the table if it doesn't exist
    if "preprints_ui" not in db.table_names():
        db["preprints_ui"].create({
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
            "is_latest_version": int,  # SQLite boolean (0/1)
            "preprint_doi": str,
            "license": str,
            "tags_list": str,
            "tags_data": str,  # JSON string
            "contributors_list": str,
            "contributors_data": str,  # JSON string
            "first_author": str,
            "subjects_list": str,
            "subjects_data": str,  # JSON string
            "download_url": str,
            "has_coi": int,  # SQLite boolean (0/1)
            "conflict_of_interest_statement": str,
            "has_data_links": str,
            "why_no_data": str,
            "data_links": str,  # JSON string
            "has_prereg_links": str,
            "why_no_prereg": str,
            "prereg_links": str,  # JSON string
            "prereg_link_info": str,
            "last_updated": str
        }, pk="id")
        
        # Add indexes for common query patterns
        db.execute("CREATE INDEX IF NOT EXISTS idx_preprints_ui_date ON preprints_ui(date_created)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_preprints_ui_provider ON preprints_ui(provider)")
        
        logger.info("Created preprints_ui table with indexes")
    else:
        # Note: During early development, we prefer to rebuild the entire database
        # when schema changes rather than adding columns incrementally
        pass
    
    return db

def populate_preprints_ui(full_rebuild=False, batch_size=500):
    """
    Populate the preprints_ui table from normalized tables.
    
    Args:
        full_rebuild: If True, rebuilds the entire table. Otherwise, only updates
                     new or modified records.
        batch_size: Number of preprints to process in each batch.
                    
    Returns:
        int: Number of preprints processed
    """
    db = create_preprints_ui_table()
    
    # Determine which preprints to process
    if full_rebuild:
        # Process all preprints
        preprint_ids = [r["id"] for r in db.query("SELECT id FROM preprints")]
        logger.info(f"Full rebuild requested, processing all {len(preprint_ids)} preprints")
    else:
        # First find preprints that don't exist in the UI table at all
        new_preprint_ids = [r["id"] for r in db.query("""
            SELECT p.id 
            FROM preprints p
            LEFT JOIN preprints_ui u ON p.id = u.id
            WHERE u.id IS NULL
        """)]
        
        # Then find preprints that have been modified since the last UI update
        # Normalize date format for consistent comparison
        modified_preprint_ids = [r["id"] for r in db.query("""
            SELECT p.id 
            FROM preprints p
            JOIN preprints_ui u ON p.id = u.id
            WHERE 
                -- Ensure dates are properly formatted for comparison
                datetime(p.date_modified) > datetime(u.last_updated)
        """)]
        
        preprint_ids = list(set(new_preprint_ids + modified_preprint_ids))
        
        if new_preprint_ids:
            logger.info(f"Found {len(new_preprint_ids)} new preprints to add to UI table")
        
        if modified_preprint_ids:
            logger.info(f"Found {len(modified_preprint_ids)} modified preprints to update in UI table")
            
        if not preprint_ids:
            logger.info("No new or modified preprints found for UI table update")
    
    if not preprint_ids:
        logger.info("No preprints to process")
        return 0
    
    # Process preprints in batches to avoid memory issues
    total_processed = 0
    
    # Create progress bar
    print(f"Processing {len(preprint_ids)} preprints...")
    pbar = tqdm(total=len(preprint_ids), desc="Building UI table", unit="preprint")
    
    for i in range(0, len(preprint_ids), batch_size):
        batch = preprint_ids[i:i+batch_size]
        records = []
        
        for preprint_id in batch:
            try:
                # Get the basic preprint data
                preprint_query = db.query("SELECT * FROM preprints WHERE id = ?", [preprint_id])
                preprint = next(iter(preprint_query), None)
                if not preprint:
                    continue
                    
                # Get contributors for this preprint
                contributors = list(db.query("""
                    SELECT c.*, pc.contributor_index, pc.bibliographic
                    FROM contributors c
                    JOIN preprint_contributors pc ON c.id = pc.contributor_id
                    WHERE pc.preprint_id = ?
                    ORDER BY pc.contributor_index
                """, [preprint_id]))
                
                # Format contributor data with safety checks
                contributors_with_names = [c for c in contributors if c.get("full_name")]
                contributors_list = "; ".join([c["full_name"] for c in contributors_with_names if c.get("bibliographic")])
                
                contributors_data = []
                for c in contributors:
                    if c.get("id"):
                        contributors_data.append({
                            "id": c["id"],
                            "name": c.get("full_name", ""),
                            "index": c.get("contributor_index", 0),
                            "orcid": c.get("orcid", ""),
                            "bibliographic": c.get("bibliographic", 0) == 1
                        })
                
                # Find first author safely
                first_author = ""
                for c in contributors:
                    if c.get("contributor_index") == 0 and c.get("full_name"):
                        first_author = c["full_name"]
                        break
                
                # Get subjects
                subjects = list(db.query("""
                    SELECT s.* 
                    FROM subjects s
                    JOIN preprint_subjects ps ON s.id = ps.subject_id
                    WHERE ps.preprint_id = ?
                """, [preprint_id]))
                
                # Format subjects safely
                subjects_with_text = [s for s in subjects if s.get("text")]
                subjects_list = "; ".join([s["text"] for s in subjects_with_text])
                
                subjects_data = []
                for s in subjects:
                    if s.get("id"):
                        subjects_data.append({
                            "id": s["id"],
                            "text": s.get("text", "")
                        })
                
                # Get tags (stored as JSON in preprints table)
                tags = []
                if preprint.get("tags"):
                    try:
                        tags = json.loads(preprint["tags"])
                    except json.JSONDecodeError:
                        tags = []
                
                # Filter out None values
                tags = [t for t in tags if t]
                tags_list = "; ".join(tags)
                tags_data = preprint.get("tags", "[]")
                
                # Create the UI record
                records.append({
                    "id": preprint_id,
                    "title": preprint.get("title", ""),
                    "description": preprint.get("description", ""),
                    "date_created": preprint.get("date_created", ""),
                    "date_modified": preprint.get("date_modified", ""),
                    "date_published": preprint.get("date_published", ""),
                    "original_publication_date": preprint.get("original_publication_date", ""),
                    "publication_doi": preprint.get("publication_doi", ""),
                    "provider": preprint.get("provider", ""),
                    "is_published": preprint.get("is_published", 0),
                    "reviews_state": preprint.get("reviews_state", ""),
                    "version": preprint.get("version", 1),
                    "is_latest_version": preprint.get("is_latest_version", 1),
                    "preprint_doi": preprint.get("preprint_doi", ""),
                    "license": preprint.get("license", ""),
                    "tags_list": tags_list,
                    "tags_data": tags_data,
                    "contributors_list": contributors_list,
                    "contributors_data": json.dumps(contributors_data),
                    "first_author": first_author,
                    "subjects_list": subjects_list,
                    "subjects_data": json.dumps(subjects_data),
                    "download_url": preprint.get("download_url", ""),
                    "has_coi": preprint.get("has_coi", 0),
                    "conflict_of_interest_statement": preprint.get("conflict_of_interest_statement", ""),
                    "has_data_links": preprint.get("has_data_links", ""),
                    "has_prereg_links": preprint.get("has_prereg_links", ""),
                    "prereg_links": preprint.get("prereg_links", ""),
                    "prereg_link_info": preprint.get("prereg_link_info", ""),
                    "last_updated": datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error processing preprint {preprint_id}: {e}")
                continue
        
        # Insert/update the records
        if records:
            try:
                db["preprints_ui"].upsert_all(records, pk="id")
                total_processed += len(records)
                # Update progress bar instead of logging each batch
                pbar.update(len(batch))
            except Exception as e:
                logger.error(f"Error inserting batch of records: {e}")
    
    # Close the progress bar
    pbar.close()
    logger.info(f"Completed processing {total_processed} preprints for UI table")
    
    return total_processed 