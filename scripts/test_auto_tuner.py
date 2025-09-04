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
# scripts/test_auto_tuner.py

"""
Test script for auto-tuner - adds test files to work queue.
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

def add_test_files(limit: int = 100):
    """Add some test files to work queue."""
    
    logger.info(f"Adding up to {limit} test files to work_queue")
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        # Find files that need blobids but aren't in queue
        cur.execute("""
            INSERT INTO work_queue (pth)
            SELECT f.pth
            FROM fs f
            WHERE f.main = true 
              AND f.blobid IS NULL
              AND NOT EXISTS (SELECT 1 FROM work_queue w WHERE w.pth = f.pth)
              AND f.size < 10000000  -- Small files for testing
            ORDER BY f.size
            LIMIT %s
            ON CONFLICT (pth) DO NOTHING
        """, (limit,))
        
        added = cur.rowcount
        conn.commit()
        
        logger.info(f"Added {added} files to work_queue")
        
        # Show queue status
        cur.execute("SELECT COUNT(*) FROM work_queue WHERE claimed_at IS NULL")
        available = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM work_queue WHERE claimed_at IS NOT NULL")
        claimed = cur.fetchone()[0]
        
        logger.info(f"Queue status: {available} available, {claimed} claimed")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

def clear_test_claims():
    """Clear any stale claims for testing."""
    
    logger.info("Clearing stale claims...")
    
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE work_queue
            SET claimed_at = NULL, claimed_by = NULL
            WHERE claimed_at < NOW() - INTERVAL '5 minutes'
            RETURNING pth
        """)
        
        cleared = cur.rowcount
        conn.commit()
        
        logger.info(f"Cleared {cleared} stale claims")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test auto-tuner with sample files")
    parser.add_argument("--add", type=int, help="Add N test files to queue")
    parser.add_argument("--clear", action="store_true", help="Clear stale claims")
    
    args = parser.parse_args()
    
    if args.clear:
        clear_test_claims()
        
    if args.add:
        add_test_files(args.add)
    
    if not args.clear and not args.add:
        parser.print_help()

if __name__ == "__main__":
    main()