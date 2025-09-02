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
# n2s/scripts/pbnas_blob_worker.py

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
STALE_PROCESSING_TIMEOUT = 30  # minutes before resetting stale processing files

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
    'total_time': 0.0,
    'claim_time': 0.0,
    'read_time': 0.0,
    'compress_time': 0.0,
    'rsync_time': 0.0,
    'db_time': 0.0,
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


def claim_work(conn) -> Optional[str]:
    """
    Phase 1: Quickly claim a file for processing using row-level locking.
    Lock scope is minimal - just the claim operation.

    Returns:
        str: File path if claimed, None if no work available
    """
    claim_start = time.time()

    try:
        with conn.cursor() as cur:
            # Use FOR UPDATE SKIP LOCKED to avoid blocking on locked rows
            cur.execute("""
                WITH candidate AS (
                  SELECT pth
                  FROM fs
                  WHERE main = true
                    AND blobid IS NULL
                    AND last_missing_at IS NULL
                    AND processing_started IS NULL
                    AND tree IN ('osxgather', 'dump-2019')
                  ORDER BY pth  -- Deterministic ordering to reduce contention
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE fs
                SET processing_started = NOW()
                FROM candidate
                WHERE fs.pth = candidate.pth
                RETURNING fs.pth
            """)

            row = cur.fetchone()
            conn.commit()  # Release lock immediately

            claim_time = time.time() - claim_start

            if row:
                logger.trace(f"Claimed work: {row[0]} (claim_time={claim_time:.3f}s)")

                # Update performance stats
                with stats_lock:
                    performance_stats['claim_time'] += claim_time

                return row[0]
            else:
                logger.trace(f"No work available (claim_time={claim_time:.3f}s)")
                return None

    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return None


def process_claimed_file(conn, fs_pth: str) -> bool:
    """
    Phase 2: Process the claimed file without holding any database locks.
    If this hangs, it only affects this worker, not others.

    Args:
        conn: Database connection
        fs_pth: File path to process

    Returns:
        bool: True if processing completed (success or handled failure)
    """
    pipeline_start = time.time()

    try:
        logger.trace(f"Processing claimed file: {fs_pth}")

        # Validate path
        if not ("dump-2019" in fs_pth or "osxgather" in fs_pth):
            logger.trace(f"Unmounted path {fs_pth}, releasing claim")
            release_processing_claim(conn, fs_pth)
            return True

        full_path = Path("/Volumes") / Path(fs_pth)

        # Check if file exists
        if not full_path.exists():
            logger.warning(f"File not found: {full_path}")
            mark_file_missing(conn, fs_pth)
            return True

        # Read file and get stats
        read_start = time.time()
        stat = full_path.stat()
        logger.trace(f"Blobifying: {full_path}, size={stat.st_size} bytes")

        # Create blob in /tmp (this can take time but doesn't block other workers)
        compress_start = time.time()
        blobid = create_blob(full_path, "/tmp")
        compress_time = time.time() - compress_start

        logger.trace(f"Created blob {blobid}")
        AA = blobid[0:2]
        BB = blobid[2:4]

        # Upload blob (this can hang but won't block other workers)
        rsync_start = time.time()
        upload_success = upload_blob(blobid, AA, BB)
        rsync_time = time.time() - rsync_start

        if not upload_success:
            logger.error(f"Upload failed for {blobid}, releasing claim")
            release_processing_claim(conn, fs_pth)
            return True

        # Phase 3: Quick database update to complete processing
        db_start = time.time()
        complete_processing(conn, fs_pth, blobid)
        db_time = time.time() - db_start

        # Clean up local blob file
        blob_path = f"/tmp/{blobid}"
        try:
            Path(blob_path).unlink()
        except FileNotFoundError:
            pass  # Already cleaned up

        # Calculate timing
        total_time = time.time() - pipeline_start
        read_time = compress_start - read_start

        # Log detailed timing
        logger.info(f"TIMING: read={read_time:.3f}s compress={compress_time:.3f}s "
                   f"rsync={rsync_time:.3f}s db={db_time:.3f}s total={total_time:.3f}s size={stat.st_size}")

        # Update performance statistics
        with stats_lock:
            performance_stats['files_processed'] += 1
            performance_stats['total_time'] += total_time
            performance_stats['read_time'] += read_time
            performance_stats['compress_time'] += compress_time
            performance_stats['rsync_time'] += rsync_time
            performance_stats['db_time'] += db_time
            performance_stats['total_bytes'] += stat.st_size

        logger.trace(f"✓ Completed {fs_pth}, blobid={blobid[:16]}...")
        return True

    except Exception as e:
        logger.error(f"Processing failed for {fs_pth}: {e}")
        release_processing_claim(conn, fs_pth)
        return True  # Continue processing other files


def upload_blob(blobid: str, AA: str, BB: str) -> bool:
    """
    Upload blob via rsync with timeout protection.

    Args:
        blobid: Blob ID
        AA: First two chars of blob ID (directory)
        BB: Next two chars of blob ID (subdirectory)

    Returns:
        bool: True if upload succeeded
    """
    blob_path = f"/tmp/{blobid}"
    remote_path = f"{REMOTE_HOST}:{REMOTE_BASE}/{AA}/{BB}/{blobid}"

    logger.trace(f"Uploading {blobid} to {REMOTE_BASE}/{AA}/{BB}/")

    try:
        # Use timeout to prevent indefinite hangs
        result = subprocess.run(
            [
                "rsync",
                "-W",  # --whole-file (no delta, just copy)
                "--no-perms", "--no-owner", "--no-group", "--no-times",
                "-e", SSH_OPTS,
                blob_path,
                remote_path,
            ],
            check=True,
            timeout=300,  # 5 minute timeout
            capture_output=True,
            text=True
        )

        logger.trace(f"✓ Uploaded blob via rsync: {remote_path}")
        return True

    except subprocess.TimeoutExpired:
        logger.error(f"rsync timeout for {blobid}")
