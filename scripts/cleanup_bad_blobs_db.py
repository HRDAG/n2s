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

Input file format: {blobid} {uploaded} {pth}
"""

import sys
import psycopg2
from datetime import datetime
from typing import List, Tuple
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


def read_bad_blobs(filename: str) -> List[Tuple[str, str, str]]:
    """
    Read bad blobs from file.
    Format: {blobid} {uploaded} {pth}
    """
    bad_blobs = []
    with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(' ', 2)
            if len(parts) == 3:
                blobid, uploaded, pth = parts
                bad_blobs.append((blobid, uploaded, pth))
            else:
                logger.warning(f"Line {line_num}: Skipping malformed line: {line}")
    return bad_blobs


def cleanup_database(bad_blobs: List[Tuple[str, str, str]], batch_size: int = 1000, dry_run: bool = False):
    """
    Clean up database:
    1. Set blobid=NULL for bad blobs
    2. Add paths back to work_queue
    """
    if not bad_blobs:
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
        
        logger.info(f"Processing {len(bad_blobs):,} bad blobs in batches of {batch_size}...")
        
        total_updated = 0
        total_queued = 0
        
        # Process in batches for efficiency
        for i in range(0, len(bad_blobs), batch_size):
            batch = bad_blobs[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (len(bad_blobs) + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} (items {i+1}-{min(i+batch_size, len(bad_blobs))})")
            
            # Start transaction for this batch
            cur.execute("BEGIN")
            
            try:
                # Clear blobids for this batch
                update_count = 0
                for blobid, uploaded, pth in batch:
                    # Escape single quotes in path
                    safe_pth = pth.replace("'", "''")
                    cur.execute(f"""
                        UPDATE fs 
                        SET blobid = NULL, uploaded = NULL 
                        WHERE pth = '{safe_pth}' AND blobid = '{blobid}'
                    """)
                    update_count += cur.rowcount
                
                # Add paths back to work_queue
                paths_to_queue = [pth for _, _, pth in batch]
                if paths_to_queue:
                    # Build the VALUES clause
                    values_clause = ','.join([f"('{pth.replace(\"'\", \"''\")}', NOW())" for _, _, pth in batch])
                    cur.execute(f"""
                        INSERT INTO work_queue (pth, added_at)
                        VALUES {values_clause}
                        ON CONFLICT (pth) DO NOTHING
                    """)
                    queue_count = cur.rowcount
                else:
                    queue_count = 0
                
                # Commit this batch
                cur.execute("COMMIT")
                
                total_updated += update_count
                total_queued += queue_count
                
                logger.debug(f"  Batch {batch_num}: Updated {update_count} records, added {queue_count} to work_queue")
                
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
        
        # Get total size to reprocess
        paths = [pth for _, _, pth in bad_blobs]
        format_strings = ','.join(['%s'] * len(paths))
        cur.execute(f"""
            SELECT SUM(stat_size) 
            FROM fs 
            WHERE pth IN ({format_strings})
        """, paths)
        total_size = cur.fetchone()[0] or 0
        
        logger.info("\n" + "="*60)
        logger.info("DATABASE CLEANUP COMPLETE")
        logger.info("="*60)
        logger.info(f"Total bad blobs processed: {len(bad_blobs):,}")
        logger.info(f"Database records updated: {total_updated:,}")
        logger.info(f"Items added to work_queue: {total_queued:,}")
        logger.info(f"Total size to reprocess: {humanize.naturalsize(total_size)}")
        logger.info(f"Current NULL blobids in fs table: {null_count:,}")
        logger.info(f"Current items in work_queue: {queue_count:,}")
        
    finally:
        conn.close()


def generate_sql_script(bad_blobs: List[Tuple[str, str, str]], output_file: str = "fix_bad_blobs.sql"):
    """
    Generate SQL script for manual review/execution if preferred.
    """
    logger.info(f"Generating SQL script: {output_file}")
    
    with open(output_file, 'w') as f:
        f.write("-- SQL to fix incorrectly processed blobs\n")
        f.write(f"-- Generated: {datetime.now()}\n")
        f.write(f"-- Total bad blobs: {len(bad_blobs)}\n\n")
        
        f.write("\\timing on\n\n")
        f.write("BEGIN;\n\n")
        
        # Clear blobids for bad blobs
        f.write("-- Clear bad blobids\n")
        for blobid, uploaded, pth in bad_blobs:
            safe_pth = pth.replace("'", "''")
            f.write(f"UPDATE fs SET blobid = NULL, uploaded = NULL WHERE pth = '{safe_pth}' AND blobid = '{blobid}';\n")
        
        f.write("\n-- Add files back to work queue\n")
        f.write("INSERT INTO work_queue (pth, added_at)\nVALUES\n")
        for i, (blobid, uploaded, pth) in enumerate(bad_blobs):
            safe_pth = pth.replace("'", "''")
            if i > 0:
                f.write(",\n")
            f.write(f"  ('{safe_pth}', NOW())")
        f.write("\nON CONFLICT (pth) DO NOTHING;\n\n")
        
        f.write(f"-- Should update {len(bad_blobs)} records\n")
        f.write("COMMIT;\n")
    
    logger.info(f"SQL script written to {output_file}")
    return output_file


def show_summary(bad_blobs: List[Tuple[str, str, str]]):
    """Show summary statistics about the bad blobs."""
    if not bad_blobs:
        return
    
    # Parse timestamps and find time range
    timestamps = []
    for _, uploaded, _ in bad_blobs:
        try:
            # Try different timestamp formats
            for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                try:
                    ts = datetime.strptime(uploaded, fmt)
                    timestamps.append(ts)
                    break
                except ValueError:
                    continue
        except:
            pass
    
    if timestamps:
        min_time = min(timestamps)
        max_time = max(timestamps)
        time_span = max_time - min_time
        
        logger.info("\nBad Blobs Summary:")
        logger.info(f"  Total count: {len(bad_blobs):,}")
        logger.info(f"  Time range: {min_time} to {max_time}")
        logger.info(f"  Time span: {time_span}")
        logger.info(f"  First blob: {bad_blobs[0][0][:16]}...")
        logger.info(f"  Last blob: {bad_blobs[-1][0][:16]}...")
        
        # Show sample paths
        logger.info("\n  Sample paths:")
        for i, (_, _, pth) in enumerate(bad_blobs[:3]):
            logger.info(f"    {pth}")
        if len(bad_blobs) > 3:
            logger.info(f"    ... and {len(bad_blobs) - 3:,} more")


def main():
    parser = argparse.ArgumentParser(description='Clean up bad blobs from database')
    parser.add_argument('bad_blobs_file', 
                        default='bad-blobids',
                        nargs='?',
                        help='File containing bad blob IDs (default: bad-blobids)')
    parser.add_argument('--generate-sql', 
                        action='store_true',
                        help='Generate SQL script instead of executing directly')
    parser.add_argument('--sql-output', 
                        default='fix_bad_blobs.sql',
                        help='Output file for SQL script (default: fix_bad_blobs.sql)')
    parser.add_argument('--batch-size',
                        type=int,
                        default=1000,
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
    
    # Read bad blobs from file
    try:
        bad_blobs = read_bad_blobs(args.bad_blobs_file)
        logger.info(f"Read {len(bad_blobs):,} bad blobs from {args.bad_blobs_file}")
    except FileNotFoundError:
        logger.error(f"File '{args.bad_blobs_file}' not found")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        sys.exit(1)
    
    if not bad_blobs:
        logger.warning("No bad blobs found in file")
        return
    
    # Show summary
    show_summary(bad_blobs)
    
    if args.generate_sql:
        # Generate SQL script only
        generate_sql_script(bad_blobs, args.sql_output)
        logger.info(f"\nTo execute: psql -h {DB_HOST} -U {DB_USER} -d {DB_NAME} < {args.sql_output}")
    else:
        # Execute database cleanup
        if not args.yes and not args.dry_run:
            logger.info("\nAbout to update database...")
            confirm = input("Continue? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Aborted")
                return
        
        cleanup_database(bad_blobs, args.batch_size, args.dry_run)
        
        if not args.dry_run:
            # Also generate SQL for reference
            generate_sql_script(bad_blobs, args.sql_output)
            logger.info(f"\nSQL script also saved to {args.sql_output} for reference")


if __name__ == "__main__":
    main()
