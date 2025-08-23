#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/migration/run_migration.py

import psycopg2
import sys
from pathlib import Path

# Configuration (same as pbnas_blob_worker)
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


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
    """Run the last_missing_at migration."""
    migration_sql = """
-- Add timezone-aware timestamp column to track when files were last found missing
-- This allows pbnas_blob_worker to skip files that don't exist and avoid infinite loops

-- Add the column (safe operation - nullable column with no default)
ALTER TABLE fs ADD COLUMN IF NOT EXISTS last_missing_at TIMESTAMP WITH TIME ZONE;

-- Create index for efficient querying (worker filters on this column)
CREATE INDEX IF NOT EXISTS idx_fs_last_missing_at
ON fs(last_missing_at)
WHERE last_missing_at IS NOT NULL;
"""
    
    stats_sql = """
-- Show current stats
SELECT
    COUNT(*) as total_files,
    COUNT(*) FILTER (WHERE main = true) as main_files,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL) as unprocessed_main_files,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL AND last_missing_at IS NULL) as ready_to_process
FROM fs;
"""

    trees_sql = """
-- Show which trees have unprocessed files
SELECT
    tree,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL AND last_missing_at IS NULL) as ready_to_process
FROM fs
WHERE tree IN ('osxgather', 'dump-2019')
GROUP BY tree
ORDER BY ready_to_process DESC;
"""

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print("Running migration: Adding last_missing_at column...")
            
            # Run migration
            cur.execute(migration_sql)
            conn.commit()
            print("✓ Migration completed successfully")
            
            print("\n--- Current Database Stats ---")
            
            # Show stats
            cur.execute(stats_sql)
            result = cur.fetchone()
            if result:
                total, main, unprocessed, ready = result
                print(f"Total files: {total:,}")
                print(f"Main files: {main:,}")
                print(f"Unprocessed main files: {unprocessed:,}")
                print(f"Ready to process: {ready:,}")
            
            print("\n--- Files Ready to Process by Tree ---")
            
            # Show trees
            cur.execute(trees_sql)
            for row in cur.fetchall():
                tree, ready = row
                print(f"{tree}: {ready:,} files")
                
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

    print("\n✓ Migration completed! Worker can now track missing files.")


if __name__ == "__main__":
    run_migration()