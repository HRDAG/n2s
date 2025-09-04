#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "lz4",
#   "blake3",
#   "python-magic",
#   "typer",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/recover_missing_blobs.py

"""
Recover missing blob files by recreating them from source files.

Usage:
    ./recover_missing_blobs.py BLOBID                  # Single blob
    ./recover_missing_blobs.py --file blobids.txt      # List from file
"""

import sys
import subprocess
from pathlib import Path
import argparse
import psycopg2
from loguru import logger

# Import blobify
sys.path.append(str(Path(__file__).parent))
from blobify import create_blob

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
UPLOAD_HOST = "snowball"
UPLOAD_PATH = "/n2s/block_storage"
SSH_PORT = "2222"


def setup_logging(verbose: bool = False):
    """Configure loguru for console output."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level=level,
    )


def get_connection():
    """Create database connection."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} options='-c timezone=America/Los_Angeles'"
    return psycopg2.connect(conn_string)


def check_blob_exists_on_storage(blob_id: str) -> bool:
    """Check if blob actually exists on storage server."""
    AA = blob_id[0:2]
    BB = blob_id[2:4]
    remote_path = f"/n2s/block_storage/{AA}/{BB}/{blob_id}"
    
    try:
        result = subprocess.run(
            ["ssh", "-p", SSH_PORT, UPLOAD_HOST, f"test -f {remote_path} && echo EXISTS"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.stdout.strip() == "EXISTS"
    except:
        return False


def find_source_file(conn, blob_id: str) -> str:
    """Find a source file path for the given blobid."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pth 
            FROM fs 
            WHERE blobid = %s 
            LIMIT 1
        """, (blob_id,))
        result = cur.fetchone()
        if result:
            return result[0]
        return None


def upload_blob(blob_path: str, blob_id: str) -> bool:
    """Upload blob to storage server using rsync."""
    AA = blob_id[0:2]
    BB = blob_id[2:4]
    
    # Create directory if it doesn't exist
    dir_path = f"/n2s/block_storage/{AA}/{BB}"
    try:
        subprocess.run(
            ["ssh", "-p", SSH_PORT, UPLOAD_HOST, f"mkdir -p {dir_path}"],
            check=True,
            capture_output=True,
            timeout=10
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create directory {dir_path}: {e}")
        return False
    
    # Upload the blob
    remote_path = f"{UPLOAD_HOST}:{UPLOAD_PATH}/{AA}/{BB}/{blob_id}"
    
    try:
        subprocess.run([
            "rsync",
            "-avz",  # archive, verbose, compress
            "-e", f"ssh -p {SSH_PORT}",
            blob_path,
            remote_path,
        ], check=True, capture_output=True, text=True, timeout=300)
        
        logger.info(f"Uploaded blob to {UPLOAD_PATH}/{AA}/{BB}/{blob_id}")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error(f"Upload timeout for {blob_id}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Upload failed for {blob_id}: {e.stderr if e.stderr else e}")
        return False


def recover_blob(blob_id: str, conn) -> bool:
    """Recover a missing blob by recreating it from source."""
    
    # Check if blob exists on storage
    if check_blob_exists_on_storage(blob_id):
        logger.info(f"Blob {blob_id[:16]}... already exists on storage")
        return True
    
    logger.warning(f"Blob {blob_id[:16]}... is missing from storage")
    
    # Find source file
    source_path = find_source_file(conn, blob_id)
    
    if not source_path:
        logger.error(f"No source file found in database for blob {blob_id[:16]}...")
        return False
    
    logger.debug(f"Found source file: {source_path}")
    
    # Check if source file exists locally
    full_path = Path("/Volumes") / Path(source_path)
    if not full_path.exists():
        logger.error(f"Source file not accessible: {full_path}")
        return False
    
    # Recreate blob
    logger.info(f"Recreating blob from {source_path}")
    created_blob_id = create_blob(full_path, "/tmp")
    
    if created_blob_id != blob_id:
        logger.error(f"Blob ID mismatch! Expected {blob_id}, got {created_blob_id}")
        return False
    
    # Upload blob
    blob_path = f"/tmp/{blob_id}"
    success = upload_blob(blob_path, blob_id)
    
    # Clean up temp file
    Path(blob_path).unlink(missing_ok=True)
    
    if success:
        # Verify it actually exists now
        if check_blob_exists_on_storage(blob_id):
            logger.success(f"✓ Successfully recovered blob {blob_id[:16]}...")
            return True
        else:
            logger.error(f"Upload claimed success but blob still missing: {blob_id[:16]}...")
            return False
    
    return False


def process_blobids(blobids: list[str]):
    """Process a list of blobids."""
    conn = get_connection()
    
    total = len(blobids)
    missing = 0
    recovered = 0
    failed = 0
    
    logger.info(f"Processing {total} blob IDs...")
    
    for i, blob_id in enumerate(blobids, 1):
        logger.info(f"[{i}/{total}] Processing {blob_id[:16]}...")
        
        if check_blob_exists_on_storage(blob_id):
            logger.debug(f"  Already exists on storage")
        else:
            missing += 1
            if recover_blob(blob_id, conn):
                recovered += 1
            else:
                failed += 1
    
    conn.close()
    
    logger.info("="*60)
    logger.info(f"Summary:")
    logger.info(f"  Total processed: {total}")
    logger.info(f"  Already existed: {total - missing}")
    logger.info(f"  Missing: {missing}")
    logger.info(f"  Recovered: {recovered}")
    logger.info(f"  Failed: {failed}")
    logger.info("="*60)
    
    return recovered, failed


def main():
    parser = argparse.ArgumentParser(description='Recover missing blob files')
    parser.add_argument('blob_id', nargs='?', help='Single blob ID to recover')
    parser.add_argument('--file', '-f', help='File containing blob IDs (one per line)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    blobids = []
    
    if args.blob_id:
        # Single blob ID provided
        blobids = [args.blob_id]
    elif args.file:
        # Read blob IDs from file
        try:
            with open(args.file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        blobids.append(line)
            logger.info(f"Read {len(blobids)} blob IDs from {args.file}")
        except FileNotFoundError:
            logger.error(f"File not found: {args.file}")
            sys.exit(1)
    else:
        # No input provided
        parser.print_help()
        sys.exit(1)
    
    if not blobids:
        logger.warning("No blob IDs to process")
        sys.exit(0)
    
    # Process the blob IDs
    recovered, failed = process_blobids(blobids)
    
    # Exit with error if any failed
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
