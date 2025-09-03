#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "humanize",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/cleanup_bad_blobs_db.py

"""
Database cleanup for bad blobs.
Reads bad-blobids file and updates database accordingly.

Input file format: {type} | {blobid} | {uploaded} | {path}
"""

import sys
import psycopg2
from datetime import datetime
from typing import List
import argparse
from loguru import logger
import humanize

# Database configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


def setup_logging(verbose: bool = False):
    """Configure loguru for console output."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level=level,
    )


def read_bad_blobids(filename: str) -> List[str]:
    """
    Read bad blob IDs from file.
    Format: {type} | {blobid} | {uploaded} | {path}
    We only need the blobid - paths will be looked up from database.
    """
    blobids = []
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('-'):
                continue
            
            # Parse pipe-delimited format
            parts = line.split(' | ')
            if len(parts) == 4:  # type | blobid | uploaded | path
                blobid = parts[1]
                blobids.append(blobid)
            else:
                logger.warning(f"Line {line_num}: Skipping malformed line: {line}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_blobids = []
    for blobid in blobids:
        if blobid not in seen:
            seen.add(blobid)
            unique_blobids.append(blobid)
    
    logger.info(f"Read {len(blobids)} blob entries, {len(unique_blobids)} unique blobids")
    return unique_blobids


def cleanup_database(bad_blobids: List[str], batch_size: int = 100, dry_run: bool = False):
    """
    Clean up database by blobid:
    1. Find all paths with each blobid
    2. Set blobid=NULL, uploaded=NULL for those records
    3. Add paths back to work_queue
    
    Note: One blobid can have multiple paths (deduplication)
    """
    if not bad_blobids:
        logger.info("No bad blobs to process")
        return
    
    # Force local timezone to prevent UTC contamination
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} options='-c timezone=America/Los_Angeles'"
    
    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")
        return
    
    conn = psycopg2.connect(conn_string)
    
    try:
        cur = conn.cursor()
        
        logger.info(f"Processing {len(bad_blobids):,} unique bad blobids in batches of {batch_size}...")
        
        total_updated = 0
        total_queued = 0
        total_paths_affected = 0
        
        # Process in batches for efficiency
        for i in range(0, len(bad_blobids), batch_size):
            batch = bad_blobids[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (len(bad_blobids) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} (blobids {i+1}-{min(i+batch_size, len(bad_blobids))})")
            
            # Start transaction for this batch
            cur.execute("BEGIN")
            
            try:
                batch_paths = []
                batch_updated = 0
                
                for blobid in batch:
                    # First, get all paths that have this blobid
                    cur.execute("""
                        SELECT pth FROM fs WHERE blobid = %s
                    """, (blobid,))
                    paths = [row[0] for row in cur.fetchall()]
                    
                    if paths:
                        batch_paths.extend(paths)
                        
                        # Update all records with this blobid
                        cur.execute("""
                            UPDATE fs 
                            SET blobid = NULL, uploaded = NULL 
                            WHERE blobid = %s
                        """, (blobid,))
                        batch_updated += cur.rowcount
                        
                        if cur.rowcount > 1:
                            logger.debug(f"  Blobid {blobid[:16]}... found in {cur.rowcount} records (deduplication)")
                
                # Add all affected paths to work_queue
                if batch_paths:
                    # Use unnest to insert multiple values efficiently
                    cur.execute("""
                        INSERT INTO work_queue (pth, created_at)
                        SELECT DISTINCT pth, NOW() 
                        FROM unnest(%s::text[]) AS pth
                        ON CONFLICT (pth) DO NOTHING
                    """, (batch_paths,))
                    queue_count = cur.rowcount
                else:
                    queue_count = 0
                
                # Commit this batch
                cur.execute("COMMIT")
                
                total_updated += batch_updated
                total_queued += queue_count
                total_paths_affected += len(batch_paths)
                
                logger.debug(f"  Batch {batch_num}: Updated {batch_updated} records, added {queue_count} to work_queue")
                
            except Exception as e:
                cur.execute("ROLLBACK")
                logger.error(f"  Error processing batch {batch_num}: {e}")
                raise
        
        # Get final statistics
        cur.execute("""
            SELECT COUNT(*) FROM fs WHERE blobid IS NULL
        """)
        null_count = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM work_queue
        """)
        queue_count = cur.fetchone()[0]
        
        logger.info("\n" + "="*60)
        logger.info("DATABASE CLEANUP COMPLETE")
        logger.info("="*60)
        logger.info(f"Unique bad blobids processed: {len(bad_blobids):,}")
        logger.info(f"Total database records updated: {total_updated:,}")
        logger.info(f"Total paths affected: {total_paths_affected:,}")
        logger.info(f"Items added to work_queue: {total_queued:,}")
        logger.info(f"Deduplication factor: {total_updated / len(bad_blobids):.2f} paths per blobid")
        logger.info(f"Current NULL blobids in fs table: {null_count:,}")
        logger.info(f"Current items in work_queue: {queue_count:,}")
        
    finally:
        conn.close()



def main():
    parser = argparse.ArgumentParser(description='Clean up bad blobs from database')
    parser.add_argument('bad_blobs_file', 
                        default='bad-blobids',
                        nargs='?',
                        help='File containing bad blob IDs (default: bad-blobids)')
    parser.add_argument('--batch-size',
                        type=int,
                        default=100,
                        help='Batch size for database operations (default: 1000)')
    parser.add_argument('--dry-run',
                        action='store_true',
                        help='Show what would be done without making changes')
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--yes', '-y',
                        action='store_true',
                        help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    # Read bad blobids from file
    try:
        bad_blobids = read_bad_blobids(args.bad_blobs_file)
    except FileNotFoundError:
        logger.error(f"File '{args.bad_blobs_file}' not found")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        sys.exit(1)
    
    if not bad_blobids:
        logger.warning("No bad blobids found in file")
        return
    
    # Execute database cleanup
    if not args.yes and not args.dry_run:
        logger.info(f"\nAbout to update database for {len(bad_blobids):,} unique blobids...")
        confirm = input("Continue? (y/N): ")
        if confirm.lower() != 'y':
            logger.info("Aborted")
            return
    
    cleanup_database(bad_blobids, args.batch_size, args.dry_run)


if __name__ == "__main__":
    main()
