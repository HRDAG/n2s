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
# Original date: 2025.09.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/propagate_blobids.py

"""
Propagate blobids from main records to their duplicates.

This script copies blobids from main records to duplicate records that share
the same content hash or tree+inode. Designed to run repeatedly as workers
process more main records.

Key features:
- Idempotent: safe to run multiple times
- Fast batches: keeps updates under 1 second to avoid blocking workers
- Two phases: hash duplicates first, then inode duplicates
"""

import sys
import time
from pathlib import Path

import psycopg2
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"

# Batch sizes (optimized based on performance testing)
HASH_BATCH_SIZE = 10000
INODE_BATCH_SIZE = 100000


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


def propagate_hash_duplicates(conn, batch_size=HASH_BATCH_SIZE):
    """
    Propagate blobids from main records to hash duplicates.
    
    Returns number of records updated.
    """
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE fs 
            SET blobid = main_fs.blobid, 
                uploaded = NOW()
            FROM fs AS main_fs
            WHERE fs.pth IN (
                SELECT fs_inner.pth
                FROM fs AS fs_inner
                JOIN fs AS main_inner ON fs_inner.hash = main_inner.hash
                WHERE fs_inner.main = false 
                  AND fs_inner.blobid IS NULL
                  AND fs_inner.hash IS NOT NULL
                  AND main_inner.main = true 
                  AND main_inner.blobid IS NOT NULL
                LIMIT %s
            )
            AND fs.main = false 
            AND fs.blobid IS NULL
            AND fs.hash IS NOT NULL
            AND main_fs.main = true 
            AND main_fs.blobid IS NOT NULL
            AND fs.hash = main_fs.hash
        """, (batch_size,))
        
        updated_count = cur.rowcount
        conn.commit()
    
    elapsed = time.time() - start_time
    return updated_count, elapsed


def propagate_inode_duplicates(conn, batch_size=INODE_BATCH_SIZE):
    """
    Propagate blobids from main records to tree+inode duplicates.
    
    Returns number of records updated.
    """
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE fs 
            SET blobid = main_fs.blobid, 
                uploaded = NOW()
            FROM fs AS main_fs
            WHERE fs.pth IN (
                SELECT fs_inner.pth
                FROM fs AS fs_inner
                JOIN fs AS main_inner ON (fs_inner.tree = main_inner.tree AND fs_inner.inode = main_inner.inode)
                WHERE fs_inner.main = false 
                  AND fs_inner.blobid IS NULL
                  AND fs_inner.tree IS NOT NULL
                  AND fs_inner.inode IS NOT NULL
                  AND main_inner.main = true 
                  AND main_inner.blobid IS NOT NULL
                LIMIT %s
            )
            AND fs.main = false 
            AND fs.blobid IS NULL
            AND fs.tree IS NOT NULL
            AND fs.inode IS NOT NULL
            AND main_fs.main = true 
            AND main_fs.blobid IS NOT NULL
            AND fs.tree = main_fs.tree
            AND fs.inode = main_fs.inode
        """, (batch_size,))
        
        updated_count = cur.rowcount
        conn.commit()
    
    elapsed = time.time() - start_time
    return updated_count, elapsed


def get_progress_stats(conn):
    """Get current progress statistics."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE main = false AND blobid IS NULL) as remaining_dups,
                COUNT(*) FILTER (WHERE main = false AND blobid IS NOT NULL) as completed_dups,
                COUNT(*) FILTER (WHERE main = true AND blobid IS NOT NULL) as completed_main
            FROM fs
        """)
        return cur.fetchone()


def main():
    """Main execution loop."""
    setup_logging()
    logger.info("Starting blobid propagation script")
    
    conn = get_db_connection()
    logger.info(f"Connected to {DB_NAME} at {DB_HOST}")
    
    try:
        # Get initial stats
        remaining_dups, completed_dups, completed_main = get_progress_stats(conn)
        logger.info(f"Initial state: {remaining_dups:,} duplicate records need blobids")
        logger.info(f"Available sources: {completed_main:,} main records with blobids")
        
        total_hash_updated = 0
        total_inode_updated = 0
        
        # Phase 1: Hash duplicates
        logger.info("Phase 1: Processing hash duplicates")
        while True:
            updated_count, elapsed = propagate_hash_duplicates(conn)
            if updated_count == 0:
                break
                
            total_hash_updated += updated_count
            logger.info(f"Hash batch: {updated_count:,} records in {elapsed:.3f}s")
            
            # Brief pause to let workers continue
            time.sleep(0.1)
        
        # Phase 2: Inode duplicates  
        logger.info("Phase 2: Processing inode duplicates")
        while True:
            updated_count, elapsed = propagate_inode_duplicates(conn)
            if updated_count == 0:
                break
                
            total_inode_updated += updated_count
            logger.info(f"Inode batch: {updated_count:,} records in {elapsed:.3f}s")
            
            # Brief pause to let workers continue
            time.sleep(0.1)
        
        # Final stats
        remaining_dups_final, completed_dups_final, _ = get_progress_stats(conn)
        
        logger.info("=" * 50)
        logger.info(f"Hash duplicates updated: {total_hash_updated:,}")
        logger.info(f"Inode duplicates updated: {total_inode_updated:,}")
        logger.info(f"Total updated this run: {total_hash_updated + total_inode_updated:,}")
        logger.info(f"Remaining duplicates needing blobids: {remaining_dups_final:,}")
        logger.info(f"Total duplicates with blobids: {completed_dups_final:,}")
        
        if total_hash_updated + total_inode_updated == 0:
            logger.info("No updates possible - all available duplicates already processed")
        else:
            logger.info("Run this script again as more main records are processed by workers")
            
    except Exception as e:
        logger.error(f"Error during propagation: {e}")
        raise
    finally:
        conn.close()
        logger.info("Propagation script completed")


if __name__ == "__main__":
    main()
