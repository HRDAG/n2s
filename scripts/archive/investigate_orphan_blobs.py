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
# scripts/investigate_orphan_blobs.py

"""
Investigate orphan blobs that exist on storage but not in database.
"""

import json
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import psycopg2
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
STORAGE_HOST = "snowball"
STORAGE_PATH = "/n2s/block_storage"
SSH_PORT = "2222"


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
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} options='-c timezone=America/Los_Angeles'"
    return psycopg2.connect(conn_string)


def get_blob_info(blob_id: str):
    """Get information about a blob file from storage."""
    AA = blob_id[0:2]
    BB = blob_id[2:4]
    remote_path = f"{STORAGE_PATH}/{AA}/{BB}/{blob_id}"
    
    info = {}
    
    # Get file stats
    try:
        result = subprocess.run(
            ["ssh", "-p", SSH_PORT, STORAGE_HOST, f"stat -c '%Y %s' {remote_path}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True
        )
        parts = result.stdout.strip().split()
        if len(parts) == 2:
            info['mtime'] = datetime.fromtimestamp(int(parts[0]))
            info['size'] = int(parts[1])
    except:
        pass
    
    # Get first few bytes to check if it's JSON
    try:
        result = subprocess.run(
            ["ssh", "-p", SSH_PORT, STORAGE_HOST, f"head -c 100 {remote_path}"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True
        )
        content_start = result.stdout
        info['starts_with_json'] = content_start.strip().startswith('{')
        info['content_preview'] = content_start[:50] if len(content_start) > 50 else content_start
    except:
        info['starts_with_json'] = False
        info['content_preview'] = "Error reading"
    
    return info


def check_database_history(blob_id: str, conn):
    """Check if blob ever existed in database (including with NULL uploaded)."""
    with conn.cursor() as cur:
        # Check if blobid exists anywhere in fs table
        cur.execute("""
            SELECT COUNT(*), 
                   COUNT(CASE WHEN uploaded IS NOT NULL THEN 1 END) as with_uploaded,
                   MIN(mtime_ts) as earliest_mtime,
                   MAX(mtime_ts) as latest_mtime
            FROM fs 
            WHERE blobid = %s
        """, (blob_id,))
        result = cur.fetchone()
        
        if result[0] > 0:
            return {
                'total_refs': result[0],
                'with_uploaded': result[1],
                'earliest_mtime': result[2],
                'latest_mtime': result[3]
            }
    return None


def main():
    setup_logging()
    
    # Read orphan blobs
    orphan_file = "../tmp/orphan_blobs.txt"
    if not Path(orphan_file).exists():
        logger.error(f"File not found: {orphan_file}")
        logger.info("Run find_phantom_blobs.py first to generate the list")
        return
    
    with open(orphan_file, 'r') as f:
        orphan_blobs = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Investigating {len(orphan_blobs)} orphan blobs...")
    
    conn = get_connection()
    
    # Categorize orphans
    never_in_db = []
    in_db_no_upload = []
    old_blobs = []  # Created before certain date
    recent_blobs = []  # Created recently
    
    # Sample investigation - check first 20 or all if fewer
    sample_size = min(20, len(orphan_blobs))
    
    for i, blob_id in enumerate(orphan_blobs[:sample_size], 1):
        logger.debug(f"[{i}/{sample_size}] Checking {blob_id[:16]}...")
        
        # Get blob file info
        blob_info = get_blob_info(blob_id)
        
        # Check database history
        db_history = check_database_history(blob_id, conn)
        
        if db_history:
            if db_history['with_uploaded'] == 0:
                in_db_no_upload.append((blob_id, blob_info, db_history))
            logger.warning(f"  Blob {blob_id[:16]}... IS in DB! refs={db_history['total_refs']}, uploaded={db_history['with_uploaded']}")
        else:
            never_in_db.append((blob_id, blob_info))
            
            # Categorize by age if we have mtime
            if 'mtime' in blob_info:
                if blob_info['mtime'].year < 2025:
                    old_blobs.append((blob_id, blob_info))
                elif (datetime.now() - blob_info['mtime']).days < 7:
                    recent_blobs.append((blob_id, blob_info))
    
    conn.close()
    
    # Report findings
    logger.info("="*60)
    logger.info(f"Sample Analysis of {sample_size} orphan blobs:")
    logger.info(f"  Never in database: {len(never_in_db)}")
    logger.info(f"  In DB but no upload timestamp: {len(in_db_no_upload)}")
    logger.info(f"  Old blobs (pre-2025): {len(old_blobs)}")
    logger.info(f"  Recent blobs (< 7 days): {len(recent_blobs)}")
    
    if never_in_db:
        logger.info("\nExamples of blobs never in database:")
        for blob_id, info in never_in_db[:3]:
            logger.info(f"  {blob_id[:32]}...")
            if 'mtime' in info:
                logger.info(f"    Created: {info['mtime']}")
            if 'size' in info:
                logger.info(f"    Size: {info['size']} bytes")
            if 'content_preview' in info:
                logger.info(f"    Preview: {info['content_preview'][:50]}...")
    
    if in_db_no_upload:
        logger.info("\nBlobs in DB without upload timestamp (might be processing errors):")
        for blob_id, info, db_hist in in_db_no_upload[:3]:
            logger.info(f"  {blob_id[:32]}...")
            logger.info(f"    DB refs: {db_hist['total_refs']}")
            if 'mtime' in info:
                logger.info(f"    File created: {info['mtime']}")


if __name__ == "__main__":
    main()
