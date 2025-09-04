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
# scripts/populate_work_queue.py

"""
Populate work_queue with all files needing blobid processing.
Includes files where main=false (duplicates in backups).
"""

import psycopg2
from loguru import logger
import sys
import time

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def populate_queue(batch_size: int = 10000, include_duplicates: bool = True):
    """Add files to work_queue in batches."""
    
    logger.info("Starting work_queue population")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # First check what we're dealing with
    cur.execute("""
        SELECT 
            COUNT(CASE WHEN main = true THEN 1 END) as main_files,
            COUNT(CASE WHEN main = false THEN 1 END) as duplicate_files,
            COUNT(*) as total
        FROM fs 
        WHERE blobid IS NULL
    """)
    main_files, duplicate_files, total = cur.fetchone()
    
    logger.info(f"Files needing blobid: {total:,} total")
    logger.info(f"  Main files: {main_files:,}")
    logger.info(f"  Duplicate files: {duplicate_files:,}")
    
    # Check current queue size
    cur.execute("SELECT COUNT(*) FROM work_queue")
    queue_size = cur.fetchone()[0]
    logger.info(f"Current work_queue size: {queue_size:,}")
    
    # Build the WHERE clause
    where_clause = "f.blobid IS NULL"
    if not include_duplicates:
        where_clause += " AND f.main = true"
    
    # Process in batches using OFFSET
    offset = 0
    total_added = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        logger.info(f"Processing batch {batch_num} (offset {offset:,})...")
        
        # Get batch of paths not already in queue
        cur.execute(f"""
            SELECT f.pth
            FROM fs f
            WHERE {where_clause}
            AND NOT EXISTS (SELECT 1 FROM work_queue w WHERE w.pth = f.pth)
            ORDER BY f.pth
            LIMIT %s
            OFFSET %s
        """, (batch_size, offset))
        
        paths = cur.fetchall()
        
        if not paths:
            logger.info("No more files to add")
            break
        
        # Insert batch into work_queue
        try:
            cur.executemany(
                "INSERT INTO work_queue (pth) VALUES (%s) ON CONFLICT (pth) DO NOTHING",
                paths
            )
            added = cur.rowcount
            conn.commit()
            
            total_added += added
            logger.info(f"  Added {added} files (total: {total_added:,})")
            
            # If we added fewer than requested, we might be done
            if len(paths) < batch_size:
                logger.info("Reached end of files")
                break
                
        except psycopg2.Error as e:
            logger.error(f"Error adding batch: {e}")
            conn.rollback()
            break
        
        offset += batch_size
        
        # Small delay to avoid overloading
        time.sleep(0.1)
    
    # Final stats
    cur.execute("SELECT COUNT(*) FROM work_queue")
    final_size = cur.fetchone()[0]
    
    logger.info("=" * 60)
    logger.info(f"Population complete!")
    logger.info(f"Added {total_added:,} files to work_queue")
    logger.info(f"Final queue size: {final_size:,}")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--include-duplicates", action="store_true", 
                       help="Include files where main=false (duplicates)")
    
    args = parser.parse_args()
    populate_queue(args.batch_size, args.include_duplicates)