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
# scripts/fix_bad_blobs.py

"""
Find and clean up incorrectly formatted blobs created with gzip+sha256
instead of lz4+blake3 with JSON wrapper.
"""

import subprocess
import sys
from datetime import datetime, timedelta
from typing import Tuple, Optional, List
import time

import psycopg2
import humanize
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
SSH_HOST = "snowball"
SSH_PORT = "2222"
STORAGE_PATH = "/n2s/block_storage"

# Time boundaries (adjust as needed)
KNOWN_GOOD_TIME = "2025-09-02 06:00:00"  # Before new worker
SUSPECTED_BAD_TIME = "2025-09-02 16:00:00"  # After new worker started


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


def check_blob_format(blob_id: str) -> str:
    """
    Check if a blob has the correct JSON format with metadata field.
    Returns: 'JSON', 'BINARY', or 'UNKNOWN'
    """
    if not blob_id or blob_id == 'DIRECTORY_SKIPPED':
        return 'SKIP'
    
    AA = blob_id[0:2]
    BB = blob_id[2:4]
    blob_path = f"{STORAGE_PATH}/{AA}/{BB}/{blob_id}"
    
    try:
        # Get the last 200 bytes of the file to check for metadata field
        result = subprocess.run(
            ["ssh", "-p", SSH_PORT, SSH_HOST, f"tail -c 200 {blob_path}"],
            capture_output=True,
            timeout=5
        )
        
        if result.returncode != 0:
            logger.warning(f"Failed to check blob {blob_id}: file not found?")
            return 'MISSING'
        
        # Check if the content contains "metadata" field (indicates JSON wrapper)
        content = result.stdout
        
        # Look for the metadata field which should be near the end of JSON files
        if b'"metadata"' in content:
            return 'JSON'
        else:
            # If no metadata field, it's likely the raw binary/gzip format
            return 'BINARY'
            
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout checking blob {blob_id}")
        return 'ERROR'
    except Exception as e:
        logger.error(f"Error checking blob {blob_id}: {e}")
        return 'ERROR'


def get_blob_at_time(conn, timestamp: str) -> Optional[Tuple[str, str, str]]:
    """Get a blob uploaded around the given timestamp."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pth, blobid, uploaded
            FROM fs
            WHERE uploaded >= %s
              AND blobid IS NOT NULL
              AND blobid != 'DIRECTORY_SKIPPED'
            ORDER BY uploaded
            LIMIT 1
        """, (timestamp,))
        
        result = cur.fetchone()
        if result:
            return result
        return None


def binary_search_transition(conn) -> Tuple[datetime, int]:
    """
    Binary search to find when bad blobs started.
    Returns the transition timestamp and number of checks performed.
    """
    logger.info("Starting binary search for transition point...")
    
    start_time = datetime.strptime(KNOWN_GOOD_TIME, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(SUSPECTED_BAD_TIME, "%Y-%m-%d %H:%M:%S")
    
    # Verify boundaries
    early_blob = get_blob_at_time(conn, KNOWN_GOOD_TIME)
    if not early_blob:
        logger.error(f"No blobs found after {KNOWN_GOOD_TIME}")
        return None, 0
    
    late_blob = get_blob_at_time(conn, SUSPECTED_BAD_TIME)
    if not late_blob:
        logger.error(f"No blobs found after {SUSPECTED_BAD_TIME}")
        return None, 0
    
    early_format = check_blob_format(early_blob[1])
    late_format = check_blob_format(late_blob[1])
    
    logger.info(f"Early blob ({early_blob[2]}): {early_blob[1][:16]}... is {early_format}")
    logger.info(f"Late blob ({late_blob[2]}): {late_blob[1][:16]}... is {late_format}")
    
    if early_format == late_format:
        logger.warning(f"Both boundaries have same format: {early_format}")
        if early_format == 'BINARY':
            logger.error("Even early timestamp has bad format! Need earlier boundary.")
        else:
            logger.info("No bad blobs found in this time range.")
        return None, 2
    
    # Binary search
    checks = 2
    while (end_time - start_time) > timedelta(minutes=1):
        mid_time = start_time + (end_time - start_time) / 2
        
        mid_blob = get_blob_at_time(conn, mid_time.strftime("%Y-%m-%d %H:%M:%S"))
        if not mid_blob:
            logger.warning(f"No blob found at {mid_time}, trying slightly later...")
            mid_time = mid_time + timedelta(minutes=5)
            mid_blob = get_blob_at_time(conn, mid_time.strftime("%Y-%m-%d %H:%M:%S"))
            if not mid_blob:
                break
        
        format_type = check_blob_format(mid_blob[1])
        checks += 1
        
        logger.info(f"Checking {mid_blob[2]}: {mid_blob[1][:16]}... is {format_type}")
        
        if format_type in ['JSON', 'SKIP', 'MISSING']:
            # Good format or skippable, bad blobs are later
            start_time = mid_time
        else:
            # Bad format, transition is earlier  
            end_time = mid_time
    
    logger.info(f"Transition found around {end_time} after {checks} checks")
    return end_time, checks


def find_all_bad_blobs(conn, transition_time: datetime) -> List[Tuple[str, str, str]]:
    """Find all blobs created after transition time that need to be fixed."""
    logger.info(f"Finding all blobs uploaded after {transition_time}...")
    
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pth, blobid, uploaded
            FROM fs
            WHERE uploaded >= %s
              AND blobid IS NOT NULL
              AND blobid != 'DIRECTORY_SKIPPED'
            ORDER BY uploaded
        """, (transition_time,))
        
        candidates = cur.fetchall()
    
    logger.info(f"Found {len(candidates):,} candidate blobs to check")
    
    # Check each blob's format
    bad_blobs = []
    good_count = 0
    skip_count = 0
    
    for i, (pth, blobid, uploaded) in enumerate(candidates):
        if i % 100 == 0:
            logger.info(f"Checked {i}/{len(candidates)} blobs... found {len(bad_blobs)} bad")
        
        format_type = check_blob_format(blobid)
        
        if format_type == 'BINARY':
            bad_blobs.append((pth, blobid, uploaded))
        elif format_type == 'JSON':
            good_count += 1
        else:
            skip_count += 1
    
    logger.info(f"Format check complete: {len(bad_blobs)} bad, {good_count} good, {skip_count} skipped/missing")
    return bad_blobs


def generate_cleanup_script(bad_blobs: List[Tuple[str, str, str]], output_file: str = "cleanup_bad_blobs.sh"):
    """Generate shell script to clean up bad blobs."""
    logger.info(f"Generating cleanup script: {output_file}")
    
    with open(output_file, 'w') as f:
        f.write("#!/bin/bash\n")
        f.write("# Script to clean up incorrectly formatted blobs\n")
        f.write(f"# Generated: {datetime.now()}\n")
        f.write(f"# Total bad blobs: {len(bad_blobs)}\n\n")
        
        f.write("echo 'Deleting bad blob files from storage...'\n\n")
        
        # Group blobs by directory for efficiency
        from collections import defaultdict
        by_dir = defaultdict(list)
        
        for pth, blobid, uploaded in bad_blobs:
            AA = blobid[0:2]
            BB = blobid[2:4]
            dir_path = f"{STORAGE_PATH}/{AA}/{BB}"
            by_dir[dir_path].append(blobid)
        
        # Generate delete commands
        for dir_path, blobs in by_dir.items():
            f.write(f"# Directory: {dir_path} ({len(blobs)} files)\n")
            f.write(f"ssh -p {SSH_PORT} {SSH_HOST} 'cd {dir_path} && rm -f")
            for blob in blobs:
                f.write(f" {blob}")
            f.write("'\n\n")
        
        f.write(f"echo 'Deleted {len(bad_blobs)} bad blob files'\n")
    
    logger.info(f"Cleanup script written to {output_file}")
    return output_file


def generate_sql_fixes(bad_blobs: List[Tuple[str, str, str]], output_file: str = "fix_bad_blobs.sql"):
    """Generate SQL to fix the database."""
    logger.info(f"Generating SQL fixes: {output_file}")
    
    with open(output_file, 'w') as f:
        f.write("-- SQL to fix incorrectly processed blobs\n")
        f.write(f"-- Generated: {datetime.now()}\n")
        f.write(f"-- Total bad blobs: {len(bad_blobs)}\n\n")
        
        f.write("BEGIN;\n\n")
        
        # Clear blobids for bad blobs
        f.write("-- Clear bad blobids\n")
        for pth, blobid, uploaded in bad_blobs:
            # Escape single quotes in path
            safe_pth = pth.replace("'", "''")
            f.write(f"UPDATE fs SET blobid = NULL, uploaded = NULL WHERE pth = '{safe_pth}' AND blobid = '{blobid}';\n")
        
        f.write("\n-- Add files back to work queue\n")
        f.write("INSERT INTO work_queue (pth)\nSELECT pth FROM fs WHERE pth IN (\n")
        for i, (pth, _, _) in enumerate(bad_blobs):
            safe_pth = pth.replace("'", "''")
            if i > 0:
                f.write(",\n")
            f.write(f"  '{safe_pth}'")
        f.write("\n) ON CONFLICT (pth) DO NOTHING;\n\n")
        
        f.write(f"-- Should update {len(bad_blobs)} records\n")
        f.write("COMMIT;\n")
    
    logger.info(f"SQL fixes written to {output_file}")
    return output_file


def main():
    """Main execution."""
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("Bad Blob Detection and Cleanup Tool")
    logger.info("=" * 60)
    
    conn = get_connection()
    
    try:
        # Step 1: Find transition point
        transition_time, checks = binary_search_transition(conn)
        
        if not transition_time:
            logger.error("Could not find transition point")
            return
        
        logger.info(f"\nTransition to bad blobs occurred at: {transition_time}")
        
        # Step 2: Find all bad blobs
        bad_blobs = find_all_bad_blobs(conn, transition_time)
        
        if not bad_blobs:
            logger.info("No bad blobs found!")
            return
        
        # Step 3: Show statistics
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Bad blobs found: {len(bad_blobs):,}")
        logger.info(f"First bad blob: {bad_blobs[0][2]} - {bad_blobs[0][1][:16]}...")
        logger.info(f"Last bad blob: {bad_blobs[-1][2]} - {bad_blobs[-1][1][:16]}...")
        
        # Calculate size impact
        with conn.cursor() as cur:
            paths = [pth for pth, _, _ in bad_blobs]
            format_strings = ','.join(['%s'] * len(paths))
            cur.execute(f"""
                SELECT SUM(stat_size) 
                FROM fs 
                WHERE pth IN ({format_strings})
            """, paths)
            total_size = cur.fetchone()[0] or 0
        
        logger.info(f"Total size to reprocess: {humanize.naturalsize(total_size)}")
        
        # Step 4: Generate cleanup scripts
        cleanup_script = generate_cleanup_script(bad_blobs)
        sql_script = generate_sql_fixes(bad_blobs)
        
        logger.info("\n" + "=" * 60)
        logger.info("NEXT STEPS")
        logger.info("=" * 60)
        logger.info("1. Review the generated files:")
        logger.info(f"   - {cleanup_script} (delete blob files)")
        logger.info(f"   - {sql_script} (fix database)")
        logger.info("2. Stop all workers")
        logger.info(f"3. Run: bash {cleanup_script}")
        logger.info(f"4. Run: psql -h {DB_HOST} -U {DB_USER} -d {DB_NAME} < {sql_script}")
        logger.info("5. Restart workers with corrected code")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
