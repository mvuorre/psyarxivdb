#!/usr/bin/env python3
"""
Refresh the small precomputed tables used by the dashboard.
"""

import logging
import os
import sys

# Add parent directory to path so we can import osf modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from osf import database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("refresh_dashboard_summaries")


def main():
    db = database.init_db()
    summary_counts = database.refresh_dashboard_summary_tables(db)
    for table_name, row_count in summary_counts.items():
        logger.info("%s: %s rows", table_name, row_count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
