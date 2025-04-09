"""
SQLite Database Optimizer

Functionality to perform routine maintenance on the SQLite database:
1. Runs VACUUM to reclaim unused space
2. Runs ANALYZE to update statistics
"""

import logging
from pathlib import Path

from osf import config
from osf import database

logger = logging.getLogger("osf.optimizer")

def optimize_database():
    """Optimize the SQLite database.
    
    Returns:
        bool: True if optimization was successful, False otherwise
    """
    db_path = config.DB_PATH
    
    # Make sure the database exists
    if not Path(db_path).exists():
        logger.error(f"Database not found at {db_path}")
        return False
    
    logger.info(f"Optimizing database at {db_path}")
    
    try:
        # Get the database connection
        db = database.get_db()
        
        # Get initial database size
        current_size = Path(db_path).stat().st_size / (1024 * 1024)  # Size in MB
        logger.info(f"Database size before optimization: {current_size:.2f} MB")
        
        # Run VACUUM to reclaim space
        logger.info("Running VACUUM...")
        db.vacuum()
        
        # Run ANALYZE to update statistics
        logger.info("Running ANALYZE...")
        db.executescript("ANALYZE")
        
        # Get final database size
        final_size = Path(db_path).stat().st_size / (1024 * 1024)  # Size in MB
        
        # Calculate space saved
        space_saved = current_size - final_size
        
        logger.info(f"Database size after optimization: {final_size:.2f} MB")
        logger.info(f"Space saved: {space_saved:.2f} MB")
        
        logger.info("Database optimization complete")
        
        return True
    except Exception as e:
        logger.error(f"Error optimizing database: {e}")
        return False 