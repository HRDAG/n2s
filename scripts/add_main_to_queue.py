#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/add_main_to_queue.py

"""
Add main=true files needing blobids to work_queue efficiently.
Uses batching and temp table for performance.
"""

import psycopg2
from loguru import logger
import sys

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def add_main_to_queue():
    """Add main=true files needing blobids to work_queue."""
    
    logger.info("Adding main=true files to work_queue")
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # First check how many we need to add
        cur.execute("""
            SELECT COUNT(*) 
            FROM fs 
            WHERE main = true 
              AND blobid IS NULL
        """)
        total_need_blobid = cur.fetchone()[0]
        
        # Check how many are already in queue
        cur.execute("""
            SELECT COUNT(*) 
            FROM fs f
            WHERE f.main = true 
              AND f.blobid IS NULL
              AND EXISTS (SELECT 1 FROM work_queue w WHERE w.pth = f.pth)
        """)
        already_in_queue = cur.fetchone()[0]
        
        to_add = total_need_blobid - already_in_queue
        
        logger.info(f"Total main=true needing blobid: {total_need_blobid:,}")
        logger.info(f"Already in work_queue: {already_in_queue:,}")
        logger.info(f"Need to add: {to_add:,}")
        
        if to_add == 0:
            logger.info("All main=true files already in queue!")
            return
        
        # Create temp table with files to add
        logger.info("Creating temp table with files to add...")
        cur.execute("""
            CREATE TEMP TABLE files_to_add AS
            SELECT f.pth
            FROM fs f
            WHERE f.main = true 
              AND f.blobid IS NULL
              AND NOT EXISTS (SELECT 1 FROM work_queue w WHERE w.pth = f.pth)
        """)
        
        cur.execute("SELECT COUNT(*) FROM files_to_add")
        temp_count = cur.fetchone()[0]
        logger.info(f"Temp table has {temp_count:,} files")
        
        # Insert from temp table
        logger.info("Inserting into work_queue...")
        cur.execute("""
            INSERT INTO work_queue (pth)
            SELECT pth FROM files_to_add
            ON CONFLICT (pth) DO NOTHING
        """)
        
        added = cur.rowcount
        conn.commit()
        
        logger.info(f"✓ Added {added:,} files to work_queue")
        
        # Final verification
        cur.execute("SELECT COUNT(*) FROM work_queue")
        final_size = cur.fetchone()[0]
        logger.info(f"Final work_queue size: {final_size:,}")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    add_main_to_queue()