#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
# ]
# ///

"""
Set up work queue table for pbnas_blob_worker_wq.py

Creates a separate work_queue table containing only unprocessed files.
This makes claiming work extremely fast regardless of table size.
"""

import psycopg2
import sys
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


def setup_logging():
    """Configure loguru for console output."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="INFO",
    )


def get_connection():
    """Create database connection."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME}"
    return psycopg2.connect(conn_string)


def create_work_queue(conn):
    """Create and populate the work queue table."""
    with conn.cursor() as cur:
        # Create work queue table if it doesn't exist
        logger.info("Creating work_queue table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS work_queue (
                pth TEXT PRIMARY KEY,
                claimed_at TIMESTAMP,
                claimed_by TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create index for fast claiming
        logger.info("Creating indexes...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_work_queue_claimed 
            ON work_queue(claimed_at) 
            WHERE claimed_at IS NULL
        """)
        
        # Count existing work
        cur.execute("""
            SELECT COUNT(*) FROM work_queue
        """)
        existing_count = cur.fetchone()[0]
        
        if existing_count > 0:
            logger.info(f"Work queue already has {existing_count:,} entries")
            
            # Clean up stale claims (older than 30 minutes)
            cur.execute("""
                UPDATE work_queue 
                SET claimed_at = NULL, claimed_by = NULL
                WHERE claimed_at < NOW() - INTERVAL '30 minutes'
                RETURNING pth
            """)
            reset_rows = cur.fetchall()
            if reset_rows:
                logger.info(f"Reset {len(reset_rows)} stale claims")
        else:
            # Populate work queue with unprocessed files
            logger.info("Populating work queue with unprocessed files...")
            cur.execute("""
                INSERT INTO work_queue (pth)
                SELECT pth 
                FROM fs
                WHERE main = true
                  AND blobid IS NULL
                  AND last_missing_at IS NULL
                  AND pth NOT LIKE '%/'
                  AND pth NOT LIKE '%/status'
                  AND pth NOT LIKE '%/.git'
                  AND pth NOT LIKE '%/.svn'
                ON CONFLICT (pth) DO NOTHING
            """)
            
            inserted = cur.rowcount
            logger.info(f"Added {inserted:,} files to work queue")
        
        # Get current statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE claimed_at IS NULL) as unclaimed,
                COUNT(*) FILTER (WHERE claimed_at IS NOT NULL) as claimed
            FROM work_queue
        """)
        stats = cur.fetchone()
        
        conn.commit()
        
        logger.info(f"Work queue ready: {stats[0]:,} total, {stats[1]:,} unclaimed, {stats[2]:,} claimed")
        
        return stats


def main():
    """Set up the work queue."""
    setup_logging()
    logger.info("Setting up work queue for pbnas_blob_worker")
    
    conn = get_connection()
    try:
        stats = create_work_queue(conn)
        
        if stats[1] == 0:
            logger.warning("No unclaimed work in queue!")
        else:
            logger.info(f"✓ Work queue ready with {stats[1]:,} files to process")
            
    except Exception as e:
        logger.error(f"Failed to set up work queue: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
