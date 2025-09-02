#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "colorama",
#   "humanize",
#   "lz4",
#   "blake3",
#   "python-magic",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/pbnas_blob_worker.py

"""
High-performance blob worker that uses a dedicated work queue table.

Key improvements:
- Uses separate work_queue table for extremely fast claiming
- No need for TABLESAMPLE or complex filtering
- Work queue only contains unprocessed files
- Claims are sub-millisecond operations
"""

import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple
import signal
import threading

import humanize
import psycopg2
from loguru import logger
from psycopg2 import pool

# Import our blobify function
sys.path.append(str(Path(__file__).parent))
from blobify import create_blob

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
UPLOAD_HOST = "snowball"
UPLOAD_PATH = "/n2s/block_storage"  # Correct path from main worker
SSH_PORT = "2222"  # SSH runs on port 2222

# Pool configuration
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 10

# Create a global connection pool
connection_pool = None

# Performance statistics
performance_stats = {
    'files_processed': 0,
    'files_claimed': 0,
    'files_missing': 0,
    'files_failed': 0,
    'files_skipped_dedup': 0,
    'stale_resets': 0,
    'total_time': 0.0,
    'claim_time': 0.0,
    'read_time': 0.0,
    'compress_time': 0.0,
    'upload_time': 0.0,
    'update_time': 0.0,
    'total_bytes': 0,
    'bytes_deduplicated': 0,
    'start_time': time.time()
}

# Thread lock for stats updates
stats_lock = threading.Lock()

# Track if we should continue running
should_continue = True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global should_continue
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    should_continue = False


def setup_logging():
    """Configure loguru for console output."""
    logger.remove()  # Remove default handler
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="DEBUG",
    )


def init_connection_pool():
    """Initialize the database connection pool."""
    global connection_pool
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME}"
    connection_pool = psycopg2.pool.ThreadedConnectionPool(
        MIN_CONNECTIONS,
        MAX_CONNECTIONS,
        conn_string
    )
    logger.info(f"Initialized connection pool with {MIN_CONNECTIONS}-{MAX_CONNECTIONS} connections")


def get_db_connection():
    """Get a connection from the pool."""
    if connection_pool is None:
        init_connection_pool()
    return connection_pool.getconn()


def return_db_connection(conn):
    """Return a connection to the pool."""
    if connection_pool:
        connection_pool.putconn(conn)


def claim_work(worker_id: str) -> Optional[str]:
    """
    Claim a file from the work queue - extremely fast operation.
    Returns the path of the claimed file or None if no work available.
    """
    claim_start = time.time()
    logger.debug("Starting claim_work()")
    conn = get_db_connection()
    
    try:
        logger.debug("Executing claim query with TABLESAMPLE")
        with conn.cursor() as cur:
            # Use TABLESAMPLE for fast random selection without full scan
            # This samples ~0.1% of the table (about 1250 rows)
            cur.execute("""
                UPDATE work_queue
                SET claimed_at = NOW(), claimed_by = %s
                WHERE pth = (
                    SELECT pth
                    FROM work_queue TABLESAMPLE BERNOULLI(0.1)
                    WHERE claimed_at IS NULL
                    LIMIT 1
                )
                AND claimed_at IS NULL  -- Double-check to prevent race condition
                RETURNING pth
            """, (worker_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            claim_time = time.time() - claim_start
            
            if result:
                with stats_lock:
                    performance_stats['files_claimed'] += 1
                    performance_stats['claim_time'] += claim_time
                logger.debug(f"Claimed work in {claim_time:.3f}s")
                return result[0]
            else:
                logger.debug(f"No work available (checked in {claim_time:.3f}s)")
                return None
                
    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return None
    finally:
        return_db_connection(conn)


def remove_from_queue(pth: str):
    """Remove a file from the work queue after processing."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM work_queue WHERE pth = %s", (pth,))
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to remove {pth} from queue: {e}")
        conn.rollback()
    finally:
        return_db_connection(conn)


def update_fs_table(pth: str, blob_id: Optional[str] = None, is_missing: bool = False):
    """Update the fs table with processing results."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if is_missing:
                cur.execute(
                    "UPDATE fs SET last_missing_at = NOW() WHERE pth = %s",
                    (pth,)
                )
            elif blob_id == 'DIRECTORY_SKIPPED':
                cur.execute(
                    "UPDATE fs SET blobid = %s, uploaded = NOW() WHERE pth = %s",
                    (blob_id, pth)
                )
            else:
                cur.execute(
                    "UPDATE fs SET blobid = %s, uploaded = NOW() WHERE pth = %s",
                    (blob_id, pth)
                )
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to update fs table for {pth}: {e}")
        conn.rollback()
    finally:
        return_db_connection(conn)


def check_blob_exists(blob_id: str) -> bool:
    """Check if a blob already exists in the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM fs WHERE blobid = %s LIMIT 1", (blob_id,))
            return cur.fetchone() is not None
    except psycopg2.Error as e:
        logger.warning(f"Failed to check blob existence: {e}")
        return False
    finally:
        return_db_connection(conn)


# Note: create_blob is now imported from blobify.py
# It uses blake3 hashing and lz4 compression with JSON wrapping


def upload_blob(blob_path: str, blob_id: str) -> bool:
    """Upload blob to storage server using rsync."""
    AA = blob_id[0:2]
    BB = blob_id[2:4]
    remote_path = f"{UPLOAD_HOST}:{UPLOAD_PATH}/{AA}/{BB}/{blob_id}"
    
    logger.trace(f"Uploading {blob_id} to {UPLOAD_PATH}/{AA}/{BB}/")
    
    # Use rsync like the main worker does
    try:
        # Use rsync with SSH on port 2222
        subprocess.run([
            "rsync",
            "-W",  # --whole-file
            "--no-perms", "--no-owner", "--no-group", "--no-times",
            "-e", f"ssh -p {SSH_PORT} -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=60",
            blob_path,
            remote_path,
        ], check=True, capture_output=True, text=True, timeout=300)  # 5 minute timeout like main worker
        
        return True
        
    except subprocess.TimeoutExpired:
        logger.error(f"Upload timeout for {blob_id}")
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Upload failed for {blob_id}: {e.stderr if e.stderr else e}")
        return False


def process_file(pth: str, worker_id: str) -> bool:
    """Process a single file."""
    pipeline_start = time.time()
    
    try:
        # Check if file exists
        full_path = Path("/Volumes") / Path(pth)
        
        if not full_path.exists():
            logger.warning(f"File not found: {pth}")
            update_fs_table(pth, is_missing=True)
            remove_from_queue(pth)
            with stats_lock:
                performance_stats['files_missing'] += 1
            return True
        
        # Skip directories
        if not full_path.is_file():
            if full_path.is_dir():
                logger.warning(f"Skipping directory (should not be in main files): {full_path}")
                update_fs_table(pth, blob_id='DIRECTORY_SKIPPED')
                remove_from_queue(pth)
            return True
        
        # Read file and get stats
        read_start = time.time()
        stat = full_path.stat()
        logger.trace(f"Processing: {full_path}, size={stat.st_size} bytes")
        
        # Create blob (compression step)
        compress_start = time.time()
        blob_id = create_blob(full_path, "/tmp")  # blobify.py expects output_dir
        compress_time = time.time() - compress_start
        read_time = compress_start - read_start
        
        logger.trace(f"✓ Created blob: {blob_id}")
        AA = blob_id[0:2]
        BB = blob_id[2:4]
        blob_path = f"/tmp/{blob_id}"
        
        # Check for deduplication
        upload_time = 0.0
        check_start = time.time()
        blob_exists = check_blob_exists(blob_id)
        check_time = time.time() - check_start
        
        if blob_exists:
            # Blob already exists, skip upload
            logger.info(f"Blob {blob_id[:16]}... already exists, skipping upload")
            with stats_lock:
                performance_stats['files_skipped_dedup'] += 1
                performance_stats['bytes_deduplicated'] += stat.st_size
        else:
            # New blob, need to upload
            upload_start = time.time()
            if upload_blob(blob_path, blob_id):
                upload_time = time.time() - upload_start
                logger.trace(f"✓ Uploaded: {UPLOAD_HOST}:{UPLOAD_PATH}/{AA}/{BB}/{blob_id}")
            else:
                logger.error(f"Failed to upload blob for {pth}")
                # Clean up the temp file
                Path(blob_path).unlink(missing_ok=True)
                with stats_lock:
                    performance_stats['files_failed'] += 1
                return False
        
        # Update database
        update_start = time.time()
        update_fs_table(pth, blob_id=blob_id)
        update_time = time.time() - update_start
        
        # Remove from queue
        queue_start = time.time()
        remove_from_queue(pth)
        queue_time = time.time() - queue_start
        
        # Clean up local blob file if it still exists
        try:
            Path(blob_path).unlink()
        except FileNotFoundError:
            pass  # Already cleaned up
        
        # Update performance statistics
        total_time = time.time() - pipeline_start
        with stats_lock:
            performance_stats['files_processed'] += 1
            performance_stats['total_time'] += total_time
            performance_stats['read_time'] += read_time
            performance_stats['compress_time'] += compress_time
            performance_stats['upload_time'] += upload_time
            performance_stats['update_time'] += update_time
            performance_stats['total_bytes'] += stat.st_size
        
        # Get claim time from performance stats
        with stats_lock:
            avg_claim_time = performance_stats['claim_time'] / performance_stats['files_claimed'] if performance_stats['files_claimed'] > 0 else 0
        
        # Calculate overhead (everything else)
        overhead_time = total_time - (read_time + compress_time + upload_time + update_time + check_time + queue_time)
        
        logger.info(
            f"TIMING: claim={avg_claim_time:.3f}s "
            f"read={read_time:.3f}s "
            f"compress={compress_time:.3f}s "
            f"check={check_time:.3f}s "
            f"upload={upload_time:.3f}s "
            f"update={update_time:.3f}s "
            f"queue={queue_time:.3f}s "
            f"overhead={overhead_time:.3f}s "
            f"total={total_time:.3f}s "
            f"size={stat.st_size}"
        )
        logger.trace(f"✓ Completed: {pth} -> {blob_id[:16]}...")
        
        return True
        
    except Exception as e:
        logger.error(f"Processing failed for {pth}: {e}")
        with stats_lock:
            performance_stats['files_failed'] += 1
        return False


def print_stats():
    """Print performance statistics."""
    with stats_lock:
        elapsed = time.time() - performance_stats['start_time']
        files_processed = performance_stats['files_processed']
        files_claimed = performance_stats['files_claimed']
        
        if files_processed > 0:
            avg_total = performance_stats['total_time'] / files_processed
            avg_read = performance_stats['read_time'] / files_processed
            avg_compress = performance_stats['compress_time'] / files_processed
            avg_upload = performance_stats['upload_time'] / files_processed
            avg_update = performance_stats['update_time'] / files_processed
        else:
            avg_total = avg_read = avg_compress = avg_upload = avg_update = 0
        
        if files_claimed > 0:
            avg_claim = performance_stats['claim_time'] / files_claimed
        else:
            avg_claim = 0
        
        logger.info(
            f"\n{'='*60}\n"
            f"Performance Statistics (Runtime: {elapsed:.1f}s)\n"
            f"{'='*60}\n"
            f"Files processed: {files_processed:,}\n"
            f"Files claimed: {files_claimed:,}\n"
            f"Files missing: {performance_stats['files_missing']:,}\n"
            f"Files failed: {performance_stats['files_failed']:,}\n"
            f"Files deduplicated: {performance_stats['files_skipped_dedup']:,}\n"
            f"Total bytes: {humanize.naturalsize(performance_stats['total_bytes'])}\n"
            f"Bytes deduplicated: {humanize.naturalsize(performance_stats['bytes_deduplicated'])}\n"
            f"\nAverage times per file:\n"
            f"  Claim:    {avg_claim:.3f}s\n"
            f"  Read:     {avg_read:.3f}s\n"
            f"  Compress: {avg_compress:.3f}s\n"
            f"  Upload:   {avg_upload:.3f}s\n"
            f"  Update:   {avg_update:.3f}s\n"
            f"  Total:    {avg_total:.3f}s\n"
            f"\nThroughput: {files_processed/elapsed:.1f} files/sec\n"
            f"{'='*60}"
        )


def worker_loop(worker_id: str):
    """Main worker loop."""
    logger.info(f"Worker {worker_id} starting...")
    
    consecutive_empty = 0
    last_stats_time = time.time()
    last_file_time = time.time()
    
    while should_continue:
        cycle_start = time.time()
        
        # Claim work
        pth = claim_work(worker_id)
        
        if pth:
            consecutive_empty = 0
            # Process the file
            success = process_file(pth, worker_id)
            if not success:
                logger.warning(f"Failed to process {pth}, will be retried later")
            
            # Log full cycle time
            cycle_time = time.time() - cycle_start
            gap_time = cycle_start - last_file_time
            last_file_time = time.time()
            logger.debug(f"CYCLE: gap_between_files={gap_time:.3f}s cycle_total={cycle_time:.3f}s")
        else:
            consecutive_empty += 1
            if consecutive_empty >= 10:
                logger.info("No work available for 10 attempts, checking less frequently...")
                time.sleep(5)
            else:
                time.sleep(0.5)
        
        # Print stats periodically
        if time.time() - last_stats_time > 30:
            print_stats()
            last_stats_time = time.time()
    
    logger.info(f"Worker {worker_id} shutting down")
    print_stats()


def main():
    """Main entry point."""
    setup_logging()
    
    # Generate unique worker ID
    worker_id = f"worker_{uuid.uuid4().hex[:8]}"
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize connection pool
    init_connection_pool()
    
    logger.info(f"Starting pbnas blob worker (work queue version) - ID: {worker_id}")
    
    try:
        worker_loop(worker_id)
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Worker crashed: {e}")
        raise
    finally:
        # Clean up
        if connection_pool:
            connection_pool.closeall()
            logger.info("Closed all database connections")


if __name__ == "__main__":
    main()
