"""
OSF Preprints Harvester

Core functionality to fetch preprint data from the Open Science Framework (OSF) API.
"""
import json
import time
import logging
from pathlib import Path
from datetime import datetime

import requests

from osf import config
from osf import tracker

logger = logging.getLogger("osf.harvester")

def build_api_url(from_date=None):
    """Build the OSF API URL with appropriate parameters.
    
    Args:
        from_date: Date string to use as filter for date_created greater than

    Returns:
        str: Fully qualified API URL with query parameters
    """
    params = config.DEFAULT_PARAMS.copy()
    
    if from_date:
        params["filter[date_created][gt]"] = from_date
    
    # Always sort by date_created to get oldest first
    params["sort"] = "date_created"
    
    # Build the URL with parameters
    url_parts = []
    for key, value in params.items():
        if isinstance(value, list):
            for item in value:
                url_parts.append(f"{key}={item}")
        else:
            url_parts.append(f"{key}={value}")
    
    return f"{config.OSF_API_URL}?{'&'.join(url_parts)}"

def save_preprint(preprint_data):
    """Save a preprint to file storage and track it in the tracker database.
    
    Args:
        preprint_data: Dictionary containing preprint data from API

    Returns:
        bool: True if save was successful, False otherwise
    """
    preprint_id = preprint_data.get('id')
    if not preprint_id:
        logger.warning("Preprint missing ID, skipping")
        return False
    
    # Get and validate date_created from the preprint
    attributes = preprint_data.get('attributes', {})
    date_created = attributes.get('date_created')
    date_modified = attributes.get('date_modified')
    
    try:
        if date_created:
            date_obj = datetime.fromisoformat(date_created.replace('Z', '+00:00'))
        else:
            logger.warning(f"Preprint {preprint_id} missing date_created, using current date")
            date_obj = datetime.now()
            date_created = date_obj.isoformat()
    except ValueError:
        logger.warning(f"Couldn't parse date_created: {date_created}, using current date")
        date_obj = datetime.now()
        date_created = date_obj.isoformat()
    
    # Create directory structure based on preprint creation date
    dir_path = Path(config.RAW_DATA_DIR) / date_obj.strftime("%Y/%m/%d")
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / f"{preprint_id}.json"
    
    # Save the JSON file
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(preprint_data, f, ensure_ascii=False, indent=2)
        
        # File was saved successfully
        file_saved = True
        logger.info(f"Saved JSON file for preprint {preprint_id} to {file_path}")
    except Exception as e:
        file_saved = False
        logger.error(f"Error saving JSON file for preprint {preprint_id}: {e}")
    
    # Add to tracker database
    try:
        tracker_success = tracker.add_harvested_preprint(
            preprint_id=preprint_id,
            date_created=date_created,
            date_modified=date_modified,
            file_path=str(file_path)
        )
        if tracker_success:
            logger.info(f"Added preprint {preprint_id} to tracker database (date_created: {date_created})")
    except Exception as e:
        tracker_success = False
        logger.error(f"Error adding preprint {preprint_id} to tracker database: {e}")
    
    # Consider the operation successful if at least the file was saved
    if file_saved:
        return True
    else:
        return False

def harvest_preprints(limit=None):
    """Harvest preprints from the OSF API, continuing from the latest in the tracker.
    
    Args:
        limit: Maximum number of preprints to harvest, or None for unlimited

    Returns:
        int: Number of preprints successfully saved
    """
    # Initialize the tracker database
    tracker.init_tracker_db()
    
    # Get the most recent date from tracker database
    from_date = tracker.get_most_recent_preprint_date()
    
    url = build_api_url(from_date)
    preprint_count = 0
    saved_count = 0
    
    # Get the initial count
    initial_count = tracker.get_harvested_preprint_count()
    
    logger.info(f"Starting harvesting with URL: {url}")
    logger.info(f"Limit: {limit or 'None'}, Continue from: {from_date or 'beginning'}")
    
    # Continue fetching next pages until limit is reached or no more results
    while url and (limit is None or preprint_count < limit):
        try:
            logger.info(f"Fetching from: {url}")
            response = requests.get(url, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # Process each preprint in the results
            for preprint in data.get('data', []):
                preprint_count += 1
                
                # Skip preprints we've already harvested
                preprint_id = preprint.get('id')
                if preprint_id and tracker.is_preprint_harvested(preprint_id):
                    logger.debug(f"Skipping already harvested preprint {preprint_id}")
                    continue
                
                if save_preprint(preprint):
                    saved_count += 1
                
                # Check if we've reached the limit
                if limit and preprint_count >= limit:
                    logger.info(f"Reached limit of {limit} preprints")
                    break
            
            # Get URL for the next page
            url = data.get('links', {}).get('next')
            
            # If there are more pages, add a delay to avoid rate limits
            if url:
                time.sleep(config.REQUEST_DELAY)
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request error: {e}")
            # Add a longer delay on error before retrying
            time.sleep(config.REQUEST_DELAY * 5)
            continue
    
    # Get the final count
    final_count = tracker.get_harvested_preprint_count()
    
    logger.info(f"Harvesting complete. Processed {preprint_count} preprints, saved {saved_count}")
    logger.info(f"Tracker DB now contains {final_count} preprints (added {final_count - initial_count})")
    
    return saved_count 