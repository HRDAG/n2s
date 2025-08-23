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

# Author: PB and Claude
# Date: 2025-08-22
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

import psycopg2
from loguru import logger

# Import our blobify function
sys.path.append(str(Path(__file__).parent))

# Configuration
# DB_HOST = "192.168.86.200"
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
REMOTE_HOST = "snowball"
REMOTE_BASE = "/n2s/block_storage"
SLEEP_INTERVAL = 2.0  # seconds between processing attempts


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
    # Try connection string format instead of parameters
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={
        DB_NAME
    } connect_timeout=10"
    conn = psycopg2.connect(conn_string)
    # Set timezone for this session
    with conn.cursor() as cur:
        cur.execute("SET timezone = 'America/Los_Angeles'")
    conn.commit()
    return conn


def process_one_file(conn) -> bool:
    """
    Process one file from fs table with advisory lock.

    Returns:
        bool: True if a file was processed, False if no work available
    """
    try:
        with conn.cursor() as cur:
            # Start transaction and try to acquire advisory lock
            cur.execute("BEGIN")

            # Select a file and try to lock it
            cur.execute("""
                WITH candidates AS (
                  SELECT pth
                  FROM fs
                  WHERE main = true
                    AND blobid IS NULL
                    AND tree IN ('osxgather', 'dump-2019')
                  LIMIT 2000  -- Only evaluate 1000 rows max
                )
                SELECT pth
                FROM candidates
                WHERE pg_try_advisory_lock(hashtext(pth)::bigint)
                ORDER BY RANDOM()
                LIMIT 1
            """)

            row = cur.fetchone()
            if not row:
                cur.execute("ROLLBACK")
                return False

            fs_pth = row[0]
            logger.info(f"Processing pth={fs_pth}")

            if not ("dump-2019" in fs_pth or "osxgather" in fs_pth):
                logger.warning(f"unmounted path {fs_pth}")
                return False

            full_path = Path("/Volumes") / Path(fs_pth)

            # Check if file exists
            if not full_path.exists():
                logger.warning(f"File not found: {full_path}")
                cur.execute("ROLLBACK")
                return True  # Continue processing other files

            # Create blob in /tmp
            stat = full_path.stat()
            logger.info(f"Blobifying: {full_path}, size={stat.st_size} bytes")
            blobid = create_blob(full_path, "/tmp")

            logger.info("done blobifying")
            AA = blobid[0:2]
            BB = blobid[2:4]

            # Upload blob with rsync
            blob_path = f"/tmp/{blobid}"
            remote_path = f"{REMOTE_HOST}:{REMOTE_BASE}/{AA}/{BB}/{blobid}"

            logger.info(f"Uploading {blobid} to {REMOTE_BASE}/{AA}/{BB}/")

            # Create remote directory first, then rsync
            try:

                subprocess.run(
                    [
                        "rsync",
                        "-az",
                        "-e ssh -p 2222",
                        blob_path,
                        remote_path,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                logger.error(f"rsync command failed: {e}")
                logger.error(f"Attempted: rsync {blob_path} {remote_path}")
                raise

            # Update database
            cur.execute(
                """
                UPDATE fs
                SET blobid = %s, uploaded = NOW()
                WHERE pth = %s
            """,
                (blobid, fs_pth),
            )

            # Commit transaction (releases advisory lock)
            cur.execute("COMMIT")

            # Clean up local blob file
            Path(blob_path).unlink()

            logger.info(f"✓ Completed pth={fs_pth}, blobid={blobid[:16]}...")
            return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Upload failed: {e}")
        conn.rollback()
        return True

    except Exception as e:
        logger.error(f"Processing failed: {e}")
        conn.rollback()
        return True


def main():
    """Main worker loop."""
    setup_logging()
    logger.info("Starting pbnas_blob_worker")

    # Connect to database
    conn = get_db_connection()
    logger.info(f"Connected to {DB_NAME} at {DB_HOST}")

    try:
        while True:
            try:
                work_done = process_one_file(conn)
                if work_done:
                    # Brief pause between files
                    time.sleep(0.1)
                else:
                    # No work available, longer sleep
                    logger.debug("No work available, sleeping...")
                    time.sleep(SLEEP_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(SLEEP_INTERVAL)

    finally:
        conn.close()
        logger.info("Worker stopped")


if __name__ == "__main__":
    main()
