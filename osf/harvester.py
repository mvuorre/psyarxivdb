"""
PsyArXiv Harvester

Core functionality to fetch PsyArXiv preprint data from the OSF API and store directly in database.
"""
import json
import time
import logging
from datetime import datetime, timedelta

import requests

from osf import config
from osf import database

logger = logging.getLogger("osf.harvester")

def build_api_url(from_date=None):
    """Build the OSF API URL with appropriate parameters for PsyArXiv preprints.
    
    Args:
        from_date: Date string to use as filter for date_modified greater than

    Returns:
        str: Fully qualified API URL with query parameters
    """
    params = config.DEFAULT_PARAMS.copy()
    
    if from_date:
        params["filter[date_modified][gt]"] = from_date
    
    # Always sort by date_modified to get oldest first
    params["sort"] = "date_modified"
    
    # Build the URL with parameters
    url_parts = []
    for key, value in params.items():
        if isinstance(value, list):
            for item in value:
                url_parts.append(f"{key}={item}")
        else:
            url_parts.append(f"{key}={value}")
    
    return f"{config.OSF_API_URL}?{'&'.join(url_parts)}"

def save_preprint(db, preprint_data):
    """Save a preprint's raw JSON data directly to the database.
    
    Args:
        db: sqlite-utils database instance
        preprint_data: Dictionary containing preprint data from API

    Returns:
        bool: True if save was successful, False otherwise
    """
    preprint_id = preprint_data.get('id')
    if not preprint_id:
        logger.warning("Preprint missing ID, skipping")
        return False
    
    # Get dates from the preprint
    attributes = preprint_data.get('attributes', {})
    date_created = attributes.get('date_created')
    date_modified = attributes.get('date_modified')
    
    try:
        # Store raw JSON data in database
        db["raw_data"].upsert({
            "id": preprint_id,
            "date_created": date_created,
            "date_modified": date_modified,
            "payload": json.dumps(preprint_data),
            "fetch_date": datetime.now().isoformat()
        }, pk="id")
        
        logger.info(f"Saved preprint {preprint_id} to raw_data table")
        return True
        
    except Exception as e:
        logger.error(f"Error saving preprint {preprint_id} to database: {e}")
        return False

def harvest_preprints(limit=None):
    """Harvest PsyArXiv preprints from the OSF API and store directly in database.
    
    Args:
        limit: Maximum number of preprints to harvest, or None for unlimited

    Returns:
        int: Number of preprints successfully saved
    """
    # Initialize the main database
    db = database.init_db()
    
    # Get the most recent modified date from raw_data table
    from_date = get_most_recent_modified_date(db)
    
    url = build_api_url(from_date)
    preprint_count = 0
    saved_count = 0
    
    # Get the initial count
    initial_count = db["raw_data"].count
    
    logger.info(f"Starting PsyArXiv harvesting with URL: {url}")
    logger.info(f"Limit: {limit or 'None'}, Continue from: {from_date or 'beginning'}")
    
    # Continue fetching next pages until limit is reached or no more results
    while url and (limit is None or preprint_count < limit):
        try:
            logger.info(f"Fetching from: {url}")
            response = requests.get(url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # Process each preprint in the results
            last_processed_date = None
            for preprint in data.get('data', []):
                preprint_count += 1
                
                # Log the current preprint's modified date to show progress
                attributes = preprint.get('attributes', {})
                current_date = attributes.get('date_modified', 'unknown')
                logger.info(f"Processing preprint {preprint.get('id', 'unknown')} (modified: {current_date})")
                
                if save_preprint(db, preprint):
                    saved_count += 1
                    # Track the last successfully processed date
                    last_processed_date = current_date
                
                # Check if we've reached the limit
                if limit and preprint_count >= limit:
                    logger.info(f"Reached limit of {limit} preprints")
                    break
            
            # Update from_date to the last processed date to prevent gaps
            if last_processed_date and last_processed_date != 'unknown':
                from_date = last_processed_date
                logger.info(f"Continue from: {from_date}")
            
            # Get URL for the next page - rebuild with updated date to prevent gaps
            next_url = data.get('links', {}).get('next')
            if next_url and last_processed_date and last_processed_date != 'unknown':
                # Use our updated date instead of the next page URL to prevent gaps
                url = build_api_url(from_date)
            else:
                url = next_url
            
            # If there are more pages, add a delay to avoid rate limits
            if url:
                time.sleep(config.REQUEST_DELAY)
        
        except requests.exceptions.RequestException as e:
            # Handle 404 errors by trying an earlier date
            if "404" in str(e) and from_date:
                logger.warning(f"API 404 error with date {from_date}, trying 6 hours earlier")
                dt = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                earlier_date = dt - timedelta(hours=6)
                from_date = earlier_date.strftime('%Y-%m-%dT%H:%M:%S')
                url = build_api_url(from_date)
                logger.info(f"Retrying with earlier date: {from_date}")
                continue
            
            logger.error(f"API request error: {e}")
            # Add a longer delay on error before retrying
            time.sleep(config.REQUEST_DELAY * 5)
            continue
    
    # Get the final count
    final_count = db["raw_data"].count
    
    logger.info(f"Harvesting complete. Processed {preprint_count} preprints, saved {saved_count}")
    logger.info(f"Raw data table now contains {final_count} preprints (added {final_count - initial_count})")
    
    return saved_count

def get_most_recent_modified_date(db):
    """Get the most recent date_modified from the raw_data table."""
    if "raw_data" not in db.table_names():
        return None
    
    # Query for the most recent modified date
    result = db.execute("""
        SELECT date_modified 
        FROM raw_data 
        WHERE date_modified IS NOT NULL 
        ORDER BY date_modified DESC
        LIMIT 1
    """).fetchone()
    
    if result and result[0]:
        # Remove microseconds from the datetime string to avoid 403 errors
        date_str = result[0]
        if '.' in date_str:
            date_str = date_str.split('.')[0]
        return date_str
    
    return None 