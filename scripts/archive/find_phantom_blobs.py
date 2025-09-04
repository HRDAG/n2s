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
# scripts/find_phantom_blobs.py

"""
Find blobids that are marked as uploaded in the database but don't exist on storage.
"""

import re
import sys
import subprocess
from pathlib import Path
import psycopg2
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
STORAGE_HOST = "snowball"
STORAGE_PATH = "/n2s/block_storage"
SSH_PORT = "2222"

# Precompile regex for valid blobid (64 hex chars)
BLOBID_PATTERN = re.compile(r'^[0-9a-f]{64}$')


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


def get_uploaded_blobids_from_db():
    """Get all unique blobids that have uploaded timestamps."""
    logger.info("Fetching blobids with uploaded timestamps from database...")
    
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT blobid 
            FROM fs 
            WHERE uploaded IS NOT NULL 
              AND blobid IS NOT NULL
            ORDER BY blobid
        """)
        blobids = {row[0] for row in cur.fetchall()}
    
    conn.close()
    logger.info(f"Found {len(blobids)} unique blobids marked as uploaded in database")
    return blobids


def get_existing_blobs_from_storage():
    """Get list of all blob files that actually exist on storage using fd."""
    logger.info("Fetching list of actual blob files from storage using fd...")
    
    try:
        # Simple fd command to get all files, then filter in Python
        result = subprocess.run(
            ["ssh", "-p", SSH_PORT, STORAGE_HOST, 
             f"/usr/lib/cargo/bin/fd --type f . {STORAGE_PATH}"],
            capture_output=True,
            text=True,
            timeout=60,
            check=True
        )
        
        # Extract just the filename (last component) and filter for valid blobids
        blob_files = set()
        for line in result.stdout.strip().split('\n'):
            if line:
                filename = line.split('/')[-1]
                # Valid blobid: 64 hex characters - use precompiled regex
                if BLOBID_PATTERN.match(filename):
                    blob_files.add(filename)
        
        logger.info(f"Found {len(blob_files)} valid blob files on storage")
        return blob_files
        
    except subprocess.TimeoutExpired:
        logger.error("Timeout while fetching blob list from storage")
        return set()
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch blob list: {e}")
        logger.error(f"stderr: {e.stderr}")
        return set()


def main():
    setup_logging()
    
    # Get blobids from database
    db_blobids = get_uploaded_blobids_from_db()
    
    if not db_blobids:
        logger.warning("No uploaded blobids found in database")
        return
    
    # Get actual blobs from storage
    storage_blobs = get_existing_blobs_from_storage()
    
    if not storage_blobs:
        logger.error("Could not retrieve blob list from storage (is fd installed on snowball?)")
        return
    
    # Find phantom blobs (in DB but not on storage)
    phantom_blobs = db_blobids - storage_blobs
    
    # Also find orphan blobs (on storage but not in DB) for information
    orphan_blobs = storage_blobs - db_blobids
    
    logger.info("="*60)
    logger.info("Summary:")
    logger.info(f"  Blobids in database (uploaded): {len(db_blobids)}")
    logger.info(f"  Blob files on storage: {len(storage_blobs)}")
    logger.info(f"  Phantom blobs (DB but not storage): {len(phantom_blobs)}")
    logger.info(f"  Orphan blobs (storage but not DB): {len(orphan_blobs)}")
    logger.info("="*60)
    
    if phantom_blobs:
        output_file = "../tmp/phantom_blobs.txt"
        with open(output_file, 'w') as f:
            for blob_id in sorted(phantom_blobs):
                f.write(f"{blob_id}\n")
        logger.info(f"Written {len(phantom_blobs)} phantom blob IDs to {output_file}")
        
        # Show first few examples
        examples = list(sorted(phantom_blobs))[:5]
        logger.info("First few phantom blobs:")
        for blob_id in examples:
            logger.info(f"  {blob_id}")
        if len(phantom_blobs) > 5:
            logger.info(f"  ... and {len(phantom_blobs) - 5} more")
    
    if orphan_blobs:
        orphan_file = "../tmp/orphan_blobs.txt"
        with open(orphan_file, 'w') as f:
            for blob_id in sorted(orphan_blobs):
                f.write(f"{blob_id}\n")
        logger.info(f"Written {len(orphan_blobs)} orphan blob IDs to {orphan_file}")
        
        # Show first few examples
        if len(orphan_blobs) <= 20:
            logger.info("Orphan blobs (on storage but not in DB):")
            for blob_id in sorted(orphan_blobs):
                logger.info(f"  {blob_id}")
        else:
            examples = list(sorted(orphan_blobs))[:10]
            logger.info("First 10 orphan blobs (on storage but not in DB):")
            for blob_id in examples:
                logger.info(f"  {blob_id}")
            logger.info(f"  ... and {len(orphan_blobs) - 10} more")


if __name__ == "__main__":
    main()
