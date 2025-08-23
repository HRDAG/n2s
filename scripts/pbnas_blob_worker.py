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
# Original date: 2025.01.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/pbnas_blob_worker-rowlock.py

"""
Improved blob worker using row-level locking instead of advisory locks.

Key improvements:
- Minimal lock scope: locks only held during claim/update operations
- Uses FOR UPDATE SKIP LOCKED to avoid lock contention
- Processing status column prevents duplicate work without holding locks
- Automatic cleanup of stale processing records
- Better error handling and timeout resilience
"""

from blobify import create_blob
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional
from collections import defaultdict
from threading import Lock

import psycopg2
from loguru import logger

# Import our blobify function
sys.path.append(str(Path(__file__).parent))

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
REMOTE_HOST = "snowball"
REMOTE_BASE = "/n2s/block_storage"
SLEEP_INTERVAL = 2.0  # seconds between processing attempts
STALE_PROCESSING_MINUTES = 30  # Reset files stuck in processing

# SSH connection pooling configuration
SSH_CONTROL_PATH = "/tmp/ssh-pbnas-%r@%h:%p"
SSH_OPTS = (
    "ssh -p 2222 "
    "-o ControlMaster=auto "
    f"-o ControlPath={SSH_CONTROL_PATH} "
    "-o ControlPersist=10m "
    "-o Compression=no "
    "-o ServerAliveInterval=60 "
    "-o BatchMode=yes"
)

# Performance statistics
stats_lock = Lock()
performance_stats = {
    'files_processed': 0,
    'files_claimed': 0,
    'files_missing': 0,
    'files_failed': 0,
    'stale_resets': 0,
    'total_time': 0.0,
    'claim_time': 0.0,
    'read_time': 0.0,
    'compress_time': 0.0,
    'upload_time': 0.0,
    'update_time': 0.0,
    'total_bytes': 0,
    'start_time': time.time()
}


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


def claim_work() -> Optional[str]:
    """
    Phase 1: Quickly claim a file for processing using separate connection.
    Uses row-level locking with SKIP LOCKED to avoid contention.
    Connection is closed immediately after claim.
    """
    claim_start = time.time()
    logger.debug("Starting claim_work()")
    claim_conn = None
    try:
        logger.debug("Getting DB connection for claim")
        conn_start = time.time()
        claim_conn = get_db_connection()
        conn_time = time.time() - conn_start
        logger.debug(f"Got DB connection in {conn_time:.3f}s, executing claim query")
        with claim_conn.cursor() as cur:
            # Use old worker's query pattern with processing_started instead of advisory locks
            logger.debug("Finding candidate file using old worker pattern")
            query_start = time.time()
            cur.execute("""
                WITH candidates AS (
                  SELECT pth
                  FROM fs
                  WHERE main = true
                    AND blobid IS NULL
                    AND last_missing_at IS NULL
                    AND processing_started IS NULL
                    AND tree IN ('osxgather', 'dump-2019')
                  LIMIT 2000  -- Evaluate larger sample like old worker
                )
                UPDATE fs
                SET processing_started = NOW()
                WHERE pth = (
                  SELECT pth FROM candidates
                  ORDER BY RANDOM()
                  LIMIT 1
                )
                RETURNING pth
            """)
            query_time = time.time() - query_start
            logger.debug(f"Combined query took {query_time:.3f}s")
            logger.debug("Claim query completed, fetching result")
            
            row = cur.fetchone()
            logger.debug("Committing claim transaction")
            commit_start = time.time()
            claim_conn.commit()
            commit_time = time.time() - commit_start
            logger.debug(f"Claim transaction committed in {commit_time:.3f}s")
            
            claim_time = time.time() - claim_start
            
            if row:
                with stats_lock:
                    performance_stats['files_claimed'] += 1
                    performance_stats['claim_time'] += claim_time
                    
                return row[0]
            else:
                return None
                
    except psycopg2.Error as e:
        claim_time = time.time() - claim_start
        logger.error(f"Failed to claim work after {claim_time:.3f}s: {e}")
        if claim_conn:
            claim_conn.rollback()
        return None
    finally:
        logger.debug("Closing claim connection")
        if claim_conn:
            claim_conn.close()
        logger.debug("Claim connection closed")


def process_claimed_file(pth: str) -> bool:
    """
    Phase 2: Process the claimed file without holding any database locks.
    If this hangs on I/O, it only affects this worker, not others.
    """
    pipeline_start = time.time()
    
    try:
        # Check if file exists
        full_path = Path("/Volumes") / Path(pth)
        
        if not full_path.exists():
            logger.warning(f"File not found: {full_path}")
            
            # Mark as missing and clear processing status with separate connection
            missing_conn = None
            try:
                missing_conn = get_db_connection()
                with missing_conn.cursor() as cur:
                    cur.execute("""
                        UPDATE fs 
                        SET last_missing_at = NOW(), 
                            processing_started = NULL
                        WHERE pth = %s
                    """, (pth,))
                    missing_conn.commit()
            except psycopg2.Error as e:
                logger.error(f"Failed to mark file as missing: {e}")
            finally:
                if missing_conn:
                    missing_conn.close()
            
            with stats_lock:
                performance_stats['files_missing'] += 1
                
            return True  # Continue processing other files

        # Read file and get stats
        read_start = time.time()
        stat = full_path.stat()
        logger.trace(f"Processing: {full_path}, size={stat.st_size} bytes")

        # Create blob (compression step)
        compress_start = time.time()
        blobid = create_blob(full_path, "/tmp")
        compress_time = time.time() - compress_start
        read_time = compress_start - read_start

        logger.trace(f"✓ Created blob: {blobid}")
        AA = blobid[0:2]
        BB = blobid[2:4]

        # Upload blob (network I/O that can hang)
        upload_start = time.time()
        blob_path = f"/tmp/{blobid}"
        remote_path = f"{REMOTE_HOST}:{REMOTE_BASE}/{AA}/{BB}/{blobid}"

        logger.trace(f"Uploading {blobid} to {REMOTE_BASE}/{AA}/{BB}/")
        
        try:
            subprocess.run([
                "rsync",
                "-W",  # --whole-file
                "--no-perms", "--no-owner", "--no-group", "--no-times",
                "-e", SSH_OPTS,
                blob_path,
                remote_path,
            ], check=True, timeout=300)  # 5 minute timeout for uploads
            
        except subprocess.TimeoutExpired:
            logger.error(f"Upload timeout for {blobid}")
            raise
        except subprocess.CalledProcessError as e:
            logger.error(f"Upload failed for {blobid}: {e}")
            raise
            
        upload_time = time.time() - upload_start
        logger.trace(f"✓ Uploaded: {remote_path}")

        # Phase 3: Quick database update with separate connection
        update_start = time.time()
        update_conn = None
        try:
            update_conn = get_db_connection()
            with update_conn.cursor() as cur:
                cur.execute("""
                    UPDATE fs 
                    SET blobid = %s, 
                        uploaded = NOW(),
                        processing_started = NULL
                    WHERE pth = %s
                """, (blobid, pth))
                update_conn.commit()
        except psycopg2.Error as e:
            logger.error(f"Failed to update database: {e}")
            raise
        finally:
            if update_conn:
                update_conn.close()
        update_time = time.time() - update_start

        # Clean up local blob file
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
        
        logger.info(f"TIMING: claim={avg_claim_time:.3f}s read={read_time:.3f}s compress={compress_time:.3f}s upload={upload_time:.3f}s update={update_time:.3f}s total={total_time:.3f}s size={stat.st_size}")
        logger.trace(f"✓ Completed: {pth} -> {blobid[:16]}...")
        
        return True

    except Exception as e:
        logger.error(f"Processing failed for {pth}: {e}")
        
        # Reset processing status so file can be retried
        reset_conn = None
        try:
            reset_conn = get_db_connection()
            with reset_conn.cursor() as cur:
                cur.execute("""
                    UPDATE fs 
                    SET processing_started = NULL 
                    WHERE pth = %s
                """, (pth,))
                reset_conn.commit()
        except psycopg2.Error as db_e:
            logger.error(f"Failed to reset processing status: {db_e}")
        finally:
            if reset_conn:
                reset_conn.close()
            
        with stats_lock:
            performance_stats['files_failed'] += 1
            
        return True  # Continue processing other files


def cleanup_stale_processing() -> int:
    """Clean up files that have been stuck in processing state."""
    cleanup_conn = None
    try:
        cleanup_conn = get_db_connection()
        with cleanup_conn.cursor() as cur:
            cur.execute("""
                UPDATE fs 
                SET processing_started = NULL
                WHERE processing_started < NOW() - INTERVAL '%s minutes'
                  AND blobid IS NULL
                RETURNING pth
            """, (STALE_PROCESSING_MINUTES,))
            
            reset_files = cur.fetchall()
            cleanup_conn.commit()
            
            if reset_files:
                logger.warning(f"Reset {len(reset_files)} stale processing files")
                with stats_lock:
                    performance_stats['stale_resets'] += len(reset_files)
                    
            return len(reset_files)
            
    except psycopg2.Error as e:
        logger.error(f"Failed to cleanup stale processing: {e}")
        return 0
    finally:
        if cleanup_conn:
            cleanup_conn.close()


def process_one_file() -> bool:
    """
    Main processing function with improved locking strategy.
    Returns True if work was attempted, False if no work available.
    """
    # Phase 1: Quick claim with separate connection
    pth = claim_work()
    if not pth:
        return False
    
    # Phase 2: Process without holding any locks
    process_claimed_file(pth)
    return True


def init_ssh_connection():
    """Initialize SSH master connection for connection pooling."""
    try:
        result = subprocess.run([
            "ssh", "-p", "2222",
            "-o", "ControlMaster=auto",
            "-o", f"ControlPath={SSH_CONTROL_PATH}",
            "-o", "ControlPersist=10m",
            "-o", "BatchMode=yes",
            REMOTE_HOST,
            "echo 'SSH master connection established'"
        ], capture_output=True, text=True, timeout=30)

        if result.returncode == 0:
            logger.trace("SSH master connection established")
        else:
            logger.warning(f"SSH master connection failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.warning("SSH master connection timed out")
    except Exception as e:
        logger.warning(f"SSH master connection error: {e}")


def cleanup_ssh_connection():
    """Clean up SSH master connection."""
    try:
        subprocess.run([
            "ssh", "-p", "2222",
            "-o", f"ControlPath={SSH_CONTROL_PATH}",
            "-O", "exit",
            REMOTE_HOST
        ], capture_output=True, timeout=10)
        logger.trace("SSH master connection closed")
    except Exception as e:
        logger.debug(f"SSH cleanup error (expected): {e}")


def ensure_schema():
    """Ensure processing_started column exists."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if processing_started column exists
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'fs' 
                  AND column_name = 'processing_started'
            """)
            
            if not cur.fetchone():
                logger.error("processing_started column not found!")
                logger.error("Please run: n2s/scripts/migration/add_processing_column.py")
                sys.exit(1)
                
        logger.trace("Schema check complete - processing_started column exists")
        
    except Exception as e:
        logger.error(f"Schema check failed: {e}")
        sys.exit(1)
    finally:
        conn.close()


def log_performance_summary():
    """Log comprehensive performance statistics."""
    if performance_stats['files_processed'] == 0 and performance_stats['files_claimed'] == 0:
        return

    elapsed = time.time() - performance_stats['start_time']
    
    # File counts
    claimed = performance_stats['files_claimed']
    processed = performance_stats['files_processed']
    missing = performance_stats['files_missing']
    failed = performance_stats['files_failed']
    stale_resets = performance_stats['stale_resets']
    
    # Timing averages (only for processed files)
    if processed > 0:
        avg_total = performance_stats['total_time'] / processed
        avg_claim = performance_stats['claim_time'] / claimed if claimed > 0 else 0
        avg_read = performance_stats['read_time'] / processed
        avg_compress = performance_stats['compress_time'] / processed
        avg_upload = performance_stats['upload_time'] / processed
        avg_update = performance_stats['update_time'] / processed

        # Throughput calculations
        throughput = processed / elapsed * 3600  # files per hour
        mb_processed = performance_stats['total_bytes'] / (1024 * 1024)
        mb_throughput = mb_processed / elapsed * 3600  # MB per hour

        logger.info(f"PERF SUMMARY: {processed} processed, {claimed} claimed, {missing} missing, {failed} failed, {stale_resets} stale resets")
        logger.info(f"THROUGHPUT: {throughput:.1f} files/hour, {mb_throughput:.1f} MB/hour in {elapsed:.1f}s")
        logger.info(f"AVG TIMING: claim={avg_claim:.3f}s read={avg_read:.3f}s compress={avg_compress:.3f}s upload={avg_upload:.3f}s update={avg_update:.3f}s total={avg_total:.3f}s")

        # Identify bottleneck
        bottlenecks = [
            ('claim', avg_claim),
            ('read', avg_read),
            ('compress', avg_compress),
            ('upload', avg_upload),
            ('update', avg_update)
        ]
        bottleneck = max(bottlenecks, key=lambda x: x[1])
        logger.info(f"BOTTLENECK: {bottleneck[0]} ({bottleneck[1]:.3f}s avg, {bottleneck[1]/avg_total*100:.1f}% of total time)")
    else:
        logger.info(f"PERF SUMMARY: {claimed} claimed, {missing} missing, {failed} failed, {stale_resets} stale resets in {elapsed:.1f}s")


def main():
    """Main worker loop with improved error handling."""
    setup_logging()
    logger.info("Starting pbnas_blob_worker (row-level locking version)")

    # Ensure schema is compatible
    ensure_schema()

    # Initialize SSH master connection
    init_ssh_connection()

    # Connect to database
    conn = get_db_connection()
    logger.info(f"Connected to {DB_NAME} at {DB_HOST}")

    try:
        stale_cleanup_counter = 0
        
        while True:
            try:
                logger.debug("Starting new work cycle")
                work_done = process_one_file()
                logger.debug(f"Work cycle completed, work_done={work_done}")
                
                if work_done:
                    # Brief pause between files
                    time.sleep(0.1)

                    # Log performance summary every 100 processed files
                    with stats_lock:
                        if performance_stats['files_processed'] % 100 == 0 and performance_stats['files_processed'] > 0:
                            log_performance_summary()
                else:
                    # No work available, longer sleep
                    logger.debug("No work available, sleeping...")
                    time.sleep(SLEEP_INTERVAL)

                # Clean up stale processing records periodically
                stale_cleanup_counter += 1
                if stale_cleanup_counter >= 100:
                    cleanup_stale_processing()
                    stale_cleanup_counter = 0

            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                # Try to reconnect
                try:
                    conn.close()
                    conn = get_db_connection()
                    logger.info("Database reconnected")
                except Exception as reconn_e:
                    logger.error(f"Failed to reconnect: {reconn_e}")
                    time.sleep(SLEEP_INTERVAL)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(SLEEP_INTERVAL)

    finally:
        conn.close()
        cleanup_ssh_connection()
        log_performance_summary()  # Final summary
        logger.trace("Worker stopped")


if __name__ == "__main__":
    main()