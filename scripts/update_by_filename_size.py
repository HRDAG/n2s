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
# scripts/update_by_filename_size.py

"""
Update main=false files with blobids based on filename+size matches
from main=true files that already have blobids.
"""

import psycopg2
from loguru import logger
import sys
from typing import Optional

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def update_by_filename_size(batch_size: int = 1000, limit: Optional[int] = None):
    """Update main=false files using filename+size matching."""
    
    logger.info("Starting filename+size based blobid updates")
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # First check the potential
        logger.info("Checking how many main=false files could be updated...")
        
        cur.execute("""
            SELECT COUNT(DISTINCT (substring(pth from '[^/]+$'), size))
            FROM fs
            WHERE main = false 
              AND blobid IS NULL
              AND size IS NOT NULL
        """)
        unique_pairs = cur.fetchone()[0]
        logger.info(f"Unique (filename, size) pairs in main=false: {unique_pairs:,}")
        
        # Process in batches
        offset = 0
        total_updated = 0
        batch_num = 0
        
        query_limit = f"LIMIT {limit}" if limit else ""
        
        while True:
            batch_num += 1
            logger.info(f"Processing batch {batch_num} (offset {offset:,})...")
            
            # Find main=false files that can be updated
            cur.execute(f"""
                WITH candidates AS (
                    SELECT DISTINCT
                        substring(pth from '[^/]+$') as filename,
                        size
                    FROM fs
                    WHERE main = false 
                      AND blobid IS NULL
                      AND size IS NOT NULL
                    ORDER BY size DESC
                    LIMIT {batch_size}
                    OFFSET {offset}
                ),
                matches AS (
                    SELECT DISTINCT ON (c.filename, c.size)
                        c.filename,
                        c.size,
                        f.blobid
                    FROM candidates c
                    JOIN fs f ON 
                        f.main = true 
                        AND f.blobid IS NOT NULL
                        AND substring(f.pth from '[^/]+$') = c.filename
                        AND f.size = c.size
                )
                UPDATE fs
                SET blobid = m.blobid
                FROM matches m
                WHERE main = false
                  AND fs.blobid IS NULL
                  AND substring(fs.pth from '[^/]+$') = m.filename
                  AND fs.size = m.size
                  AND m.blobid IS NOT NULL
                RETURNING fs.pth
            """)
            
            updated = cur.rowcount
            
            if updated == 0:
                logger.info("No more updates found")
                break
            
            conn.commit()
            total_updated += updated
            logger.info(f"  Updated {updated} files (total: {total_updated:,})")
            
            if limit and total_updated >= limit:
                logger.info(f"Reached limit of {limit}")
                break
            
            offset += batch_size
            
            # Check if we're done
            if updated < batch_size:
                logger.info("Processed all available matches")
                break
        
        # Final statistics
        cur.execute("""
            SELECT 
                COUNT(*) as still_need_blobid,
                COUNT(CASE WHEN size > 1000000 THEN 1 END) as large_files
            FROM fs
            WHERE main = false AND blobid IS NULL
        """)
        still_need, large_files = cur.fetchone()
        
        logger.info("=" * 60)
        logger.info(f"✓ Updated {total_updated:,} main=false files with blobids")
        logger.info(f"  Remaining main=false without blobid: {still_need:,}")
        logger.info(f"  Large files (>1MB) still needing blobid: {large_files:,}")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of unique (filename,size) pairs per batch")
    parser.add_argument("--limit", type=int, 
                       help="Maximum number of files to update")
    
    args = parser.parse_args()
    update_by_filename_size(args.batch_size, args.limit)