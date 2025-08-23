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
# Original date: 2025.01.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/migration/add_processing_column.py

"""
Add processing_started column to fs table for improved worker coordination.

This migration adds a timezone-aware timestamp column to track when files
are claimed for processing, enabling row-level locking without advisory locks.
"""

import sys
from loguru import logger
import psycopg2

# Configuration (same as workers)
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


def setup_logging():
    """Configure loguru for console output."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="INFO",
    )


def get_db_connection():
    """Create database connection with timezone set."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} connect_timeout=10"
    conn = psycopg2.connect(conn_string)
    # Set timezone for this session
    with conn.cursor() as cur:
        cur.execute("SET timezone = 'America/Los_Angeles'")
    conn.commit()
    return conn


def run_migration():
    """Add processing_started column and index."""
    logger.info("Starting processing_started column migration")
    
    migration_sql = """
-- Add timezone-aware timestamp column to track when files are claimed for processing
-- This enables row-level locking without advisory locks, reducing contention

-- Add the column (safe operation - nullable column with no default)
ALTER TABLE fs ADD COLUMN IF NOT EXISTS processing_started TIMESTAMP WITH TIME ZONE;

-- Create partial index for efficient querying (only indexes non-null values)
CREATE INDEX IF NOT EXISTS idx_fs_processing_started
ON fs(processing_started)
WHERE processing_started IS NOT NULL;

-- Create compound index for worker queries
CREATE INDEX IF NOT EXISTS idx_fs_worker_selection
ON fs(main, blobid, last_missing_at, processing_started, tree)
WHERE main = true 
  AND blobid IS NULL 
  AND last_missing_at IS NULL
  AND processing_started IS NULL;
"""

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            logger.info("Executing migration SQL...")
            cur.execute(migration_sql)
            
        conn.commit()
        logger.info("✓ Migration completed successfully!")
        
        # Verify the column was added
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'fs' 
                  AND column_name = 'processing_started'
            """)
            
            result = cur.fetchone()
            if result:
                logger.info(f"✓ Column verified: {result[0]} ({result[1]}, nullable: {result[2]})")
            else:
                logger.error("✗ Column not found after migration!")
                
        # Check indexes
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = 'fs'
                  AND indexname LIKE '%processing%'
            """)
            
            indexes = cur.fetchall()
            logger.info(f"✓ Created indexes: {[idx[0] for idx in indexes]}")
            
    except psycopg2.Error as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Migration complete! Workers can now use row-level locking.")


def main():
    """Main migration runner."""
    setup_logging()
    
    try:
        run_migration()
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    
    logger.info("All done!")


if __name__ == "__main__":
    main()