"""OSF Preprints SQLite Ingestion - Store preprint data in database"""
import json
import logging
from pathlib import Path
from tqdm import tqdm

from osf import config
from osf import database
from osf import entities
from osf import tracker
from osf import optimizer_ui

logger = logging.getLogger("osf.ingestor")

def process_preprint(preprint_id, preprint_data):
    """Process a single preprint and insert into normalized tables."""
    try:
        db = database.get_db()
        
        # Set timeout for database operations
        db.execute("PRAGMA busy_timeout = 5000")
        
        # Extract normalized data
        attributes = preprint_data.get('attributes', {})
        relationships = preprint_data.get('relationships', {})
        
        # Process provider and preprint data
        provider_id = entities.process_provider(db, relationships)
        preprint = entities.extract_preprint_data(preprint_id, preprint_data)
        db["preprints"].upsert(preprint, pk="id")
        
        # Process related entities
        entities.process_contributors(db, preprint_id, preprint_data)
        entities.process_subjects(db, preprint_id, preprint_data)
        entities.process_tags(db, preprint_id, attributes.get('tags', []))
        
        # Commit transaction and mark as ingested
        db.conn.commit()
        tracker.mark_as_ingested(preprint_id)
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing preprint {preprint_id}: {e}")
        try:
            db.conn.commit()  # Commit any successful parts
        except:
            pass
        return False

def process_all_new_preprints(limit=None):
    """Process all unprocessed preprints in the tracker database."""
    # Initialize databases
    db = database.init_db()
    tracker.init_tracker_db()
    
    # Set pragmas for better performance
    db.execute("PRAGMA synchronous = OFF")
    db.execute("PRAGMA journal_mode = MEMORY")
    
    # Get preprints to process
    unprocessed = tracker.get_pending_ingestion_preprints(limit=limit)
    if not unprocessed:
        logger.info("No pending preprints found")
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
                file_path = record.get("file_path")
                
                # Verify file exists
                if not file_path or not Path(file_path).is_file():
                    logger.warning(f"File not found: {file_path}")
                    errors += 1
                    pbar.update(1)
                    continue
                
                # Load and process
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        preprint_data = json.load(f)
                    
                    if process_preprint(preprint_id, preprint_data):
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
    
    # Update UI table if needed
    if success > 0:
        logger.info(f"Updating UI table with {success} new preprints")
        try:
            ui_count = optimizer_ui.populate_preprints_ui(full_rebuild=False)
            logger.info(f"Updated {ui_count} preprints in UI table")
        except Exception as e:
            logger.error(f"UI table update error: {e}")
    
    logger.info(f"Ingestion complete: {success} added, {errors} failed")
    return processed, success, errors 