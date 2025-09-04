#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "blake3",
#   "lz4",
#   "humanize",
#   "psutil",
#   "numpy",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/pbnas_auto_tuner.py

"""
Auto-tuning blob processor with dynamic worker scaling.

Architecture:
- Hash workers: Read files, calculate hashes, check dedup
- Compress workers: Compress/encrypt files that need blobs
- Upload workers: Batch rsync to storage server
- Auto-tuning: Dynamically adjusts worker counts and thresholds

Data flow:
- Small files (<10MB): Pass through shared memory
- Medium files (10-50MB): Hash then reread if needed
- Large files (>50MB): Stream processing
"""

import json
import multiprocessing as mp
import os
import resource
import signal
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from multiprocessing import shared_memory
from pathlib import Path
from queue import Empty, Full
from typing import Dict, List, Optional

import blake3
import humanize
import lz4.frame
import numpy as np
import psutil
import psycopg2
from loguru import logger
from psycopg2 import pool

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
UPLOAD_HOST = "snowball"
UPLOAD_PATH = "/n2s/block_storage"
SSH_PORT = "2222"
STAGING_PATH = "/tmp/n2s_staging"

# Create staging directory
Path(STAGING_PATH).mkdir(exist_ok=True)

# Global queues (created by orchestrator)
compress_queue = None
connection_pool = None

# Worker constraints
MAX_HASH = 8
MAX_COMPRESS = 8
MAX_UPLOAD = 4

# Disk I/O coordination
# Default value - can be overridden by orchestrator
disk_io_semaphore = mp.Semaphore(3)  # Safe default

# Shutdown flag
shutdown_flag = mp.Event()
verbose_mode = False  # Global flag for workers

# Global metrics tracking
db_latencies = {'claim': [], 'dedup': [], 'update': []}
disk_read_latencies = []  # Track disk read times for thrashing detection
db_ops_queue = None  # Will be set by orchestrator for async DB ops


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    signal_name = {2: 'SIGINT (Ctrl+C)', 15: 'SIGTERM'}.get(signum, f'Signal {signum}')
    logger.info(f"\n{'='*60}")
    logger.info(f"Received {signal_name} - initiating graceful shutdown...")
    logger.info("Please wait for workers to finish current tasks...")
    logger.info(f"{'='*60}")
    shutdown_flag.set()


def init_connection_pool():
    """Initialize database connection pool."""
    global connection_pool
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} options='-c timezone=America/Los_Angeles'"
    connection_pool = psycopg2.pool.ThreadedConnectionPool(2, 10, conn_string)


def get_db_connection():
    """Get connection from pool."""
    if connection_pool is None:
        init_connection_pool()
    return connection_pool.getconn()


def return_db_connection(conn):
    """Return connection to pool."""
    if connection_pool:
        connection_pool.putconn(conn)


def claim_work(worker_id: str) -> Optional[tuple]:
    """Claim a file from work_queue and get its size from fs table."""
    t0 = time.perf_counter()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Claim work and get size from fs table in one query
            cur.execute("""
                WITH claimed AS (
                    UPDATE work_queue
                    SET claimed_at = NOW(), claimed_by = %s
                    WHERE pth = (
                        SELECT pth FROM work_queue TABLESAMPLE BERNOULLI(0.1)
                        WHERE claimed_at IS NULL
                        LIMIT 1
                    )
                    AND claimed_at IS NULL
                    RETURNING pth
                )
                SELECT c.pth, f.size 
                FROM claimed c
                LEFT JOIN fs f ON c.pth = f.pth
            """, (worker_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            # Track latency
            latency_ms = (time.perf_counter() - t0) * 1000
            if len(db_latencies['claim']) > 1000:  # Keep last 1000
                db_latencies['claim'].pop(0)
            db_latencies['claim'].append(latency_ms)
            
            if result:
                return (result[0], result[1])  # (path, size)
            return None
                
    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return None
    finally:
        return_db_connection(conn)


def check_blob_exists(blob_id: str) -> bool:
    """Check if blob already exists in database."""
    t0 = time.perf_counter()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM fs WHERE blobid = %s LIMIT 1", (blob_id,))
            result = cur.fetchone() is not None
            
            # Track latency
            latency_ms = (time.perf_counter() - t0) * 1000
            if len(db_latencies['dedup']) > 1000:
                db_latencies['dedup'].pop(0)
            db_latencies['dedup'].append(latency_ms)
            
            return result
    except psycopg2.Error as e:
        logger.warning(f"Failed to check blob existence: {e}")
        return False
    finally:
        return_db_connection(conn)


def update_fs_table(path: str, blob_id: str, is_missing: bool = False, mark_uploaded: bool = False):
    """Update fs table with blobid or missing status."""
    # Use async DB operations when available (except for missing/uploaded flags)
    if db_ops_queue is not None and not is_missing and not mark_uploaded:
        db_ops_queue.put(('update_fs', (path, blob_id)))
        return
        
    # Synchronous fallback or for special cases
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if is_missing:
                # Set cantfind=true and last_missing_at when file not found
                cur.execute("""
                    UPDATE fs 
                    SET cantfind = true, 
                        last_missing_at = NOW()
                    WHERE pth = %s
                """, (path,))
            elif mark_uploaded:
                # ONLY set uploaded when actually uploaded to remote
                cur.execute("""
                    UPDATE fs 
                    SET uploaded = NOW()
                    WHERE pth = %s
                """, (path,))
            else:
                # Just set blobid when staging (NOT uploaded yet!)
                cur.execute("""
                    UPDATE fs 
                    SET blobid = %s
                    WHERE pth = %s
                """, (blob_id, path))
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to update fs table: {e}")
        conn.rollback()
    finally:
        return_db_connection(conn)


def remove_from_queue(path: str):
    """Remove file from work_queue after processing."""
    # Use async DB operations when available
    if db_ops_queue is not None:
        db_ops_queue.put(('remove_queue', path))
        return
        
    # Synchronous fallback
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM work_queue WHERE pth = %s", (path,))
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to remove from queue: {e}")
        conn.rollback()
    finally:
        return_db_connection(conn)


class SimpleDBWorker(mp.Process):
    """Simplified DB worker - just handles async DB operations without changing data flow."""
    
    def __init__(self, worker_id: str, db_ops_queue: mp.Queue, stats: dict):
        super().__init__()
        self.worker_id = worker_id
        self.db_ops_queue = db_ops_queue  # Receives (op_type, data) tuples
        self.stop_flag = mp.Event()
        self.batch_size = 50
        self.stats = stats
        
    def run(self):
        """Main worker loop."""
        logger.info(f"SimpleDBWorker {self.worker_id} started")
        init_connection_pool()
        
        update_batch = []  # For fs table updates
        remove_batch = []  # For work_queue removals
        last_flush = time.time()
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            # Collect operations
            try:
                while not self.db_ops_queue.empty():
                    op_type, data = self.db_ops_queue.get_nowait()
                    
                    if op_type == 'update_fs':
                        update_batch.append(data)
                    elif op_type == 'remove_queue':
                        remove_batch.append(data)
                    
                    # Flush if batch is full
                    if len(update_batch) >= self.batch_size:
                        self.flush_updates(update_batch)
                        update_batch = []
                    if len(remove_batch) >= self.batch_size:
                        self.flush_removals(remove_batch)
                        remove_batch = []
            except:
                pass
            
            # Flush on timeout
            if time.time() - last_flush > 0.5:
                if update_batch:
                    self.flush_updates(update_batch)
                    update_batch = []
                if remove_batch:
                    self.flush_removals(remove_batch)
                    remove_batch = []
                last_flush = time.time()
                
            time.sleep(0.01)
            
        # Final flush
        if update_batch:
            self.flush_updates(update_batch)
        if remove_batch:
            self.flush_removals(remove_batch)
            
        logger.info(f"SimpleDBWorker {self.worker_id} stopped")
        
    def flush_updates(self, batch: list):
        """Batch update fs table."""
        if not batch:
            return
            
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                for path, blob_id in batch:
                    cur.execute("""
                        UPDATE fs SET blobid = %s
                        WHERE pth = %s
                    """, (blob_id, path))
                conn.commit()
                self.stats['db_updates'] = self.stats.get('db_updates', 0) + len(batch)
                    
        except psycopg2.Error as e:
            logger.error(f"Batch claim failed: {e}")
            conn.rollback()
        finally:
            return_db_connection(conn)
            
    def flush_removals(self, batch: list):
        """Batch remove from work_queue."""
        if not batch:
            return
            
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM work_queue
                    WHERE pth = ANY(%s)
                """, (batch,))
                conn.commit()
                self.stats['db_removals'] = self.stats.get('db_removals', 0) + len(batch)
                        
        except psycopg2.Error as e:
            logger.error(f"Batch removal failed: {e}")
            conn.rollback()
        finally:
            return_db_connection(conn)


class HashWorker(mp.Process):
    """Read files, hash them, check dedup, pass to compress if needed."""
    
    def __init__(self, worker_id: str, compress_queue: mp.Queue, thresholds: dict, stats: dict):
        super().__init__()
        self.worker_id = worker_id
        self.compress_queue = compress_queue
        self.thresholds = thresholds
        self.stop_flag = mp.Event()
        self.stats = stats  # Shared manager dict
        # Initialize timing stats
        self.stats['read_time_ms'] = 0
        self.stats['hash_time_ms'] = 0
        self.stats['dedup_time_ms'] = 0
        self.stats['queue_time_ms'] = 0
        self.stats['total_time_ms'] = 0
        self.stats['bytes_read'] = 0
        
    def run(self):
        """Main worker loop."""
        # Configure worker logging
        if not verbose_mode:
            logger.remove()
            logger.add(
                sys.stdout,
                level="INFO",
                format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
            )
        logger.info(f"HashWorker {self.worker_id} started")
        init_connection_pool()
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            # Claim work
            claim_result = claim_work(self.worker_id)
            if not claim_result:
                time.sleep(1)
                continue
                
            path, expected_size = claim_result
                
            try:
                self.process_file(path, expected_size)
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                
        logger.info(f"HashWorker {self.worker_id} stopping...")
        self.cleanup()
        logger.info(f"HashWorker {self.worker_id} stopped")
        
    def process_file(self, path: str, expected_size: Optional[int]):
        """Process a single file."""
        file_start = time.perf_counter()
        file_path = Path("/Volumes") / path
        
        # Check existence
        if not file_path.exists():
            update_fs_table(path, None, is_missing=True)
            remove_from_queue(path)
            return
            
        if not file_path.is_file():
            remove_from_queue(path)
            return
            
        # Get file size and verify it matches database
        t0 = time.perf_counter()
        size = file_path.stat().st_size
        if expected_size is not None and size != expected_size:
            logger.warning(f"Size mismatch for {path}: disk={size} db={expected_size}")
        self.stats['bytes_hashed'] = self.stats.get('bytes_hashed', 0) + size
        self.stats['bytes_read'] = self.stats.get('bytes_read', 0) + size
        
        # Decide strategy based on size and thresholds
        if size <= self.thresholds.get('shared_memory_max', 10_000_000):
            self.process_small_file(path, file_path, size)
        elif size <= self.thresholds.get('reread_threshold', 50_000_000):
            self.process_medium_file(path, file_path, size)
        else:
            self.process_large_file(path, file_path, size)
            
        self.stats['files_processed'] = self.stats.get('files_processed', 0) + 1
        
    def process_small_file(self, path: str, file_path: Path, size: int):
        """Small file: read once, pass through shared memory if needed."""
        # Time disk read with I/O coordination
        t0 = time.perf_counter()
        with disk_io_semaphore:  # Prevent disk thrashing
            data = file_path.read_bytes()
        read_time = (time.perf_counter() - t0) * 1000
        self.stats['read_time_ms'] = self.stats.get('read_time_ms', 0) + read_time
        
        # Track read latency for thrashing detection
        global disk_read_latencies
        disk_read_latencies.append(read_time)
        if len(disk_read_latencies) > 100:  # Keep last 100 readings
            disk_read_latencies.pop(0)
        
        # Time hashing
        t0 = time.perf_counter()
        blob_id = blake3.blake3(data).hexdigest()
        hash_time = (time.perf_counter() - t0) * 1000
        self.stats['hash_time_ms'] = self.stats.get('hash_time_ms', 0) + hash_time
        
        # Time dedup check
        t0 = time.perf_counter()
        exists = check_blob_exists(blob_id)
        dedup_time = (time.perf_counter() - t0) * 1000
        self.stats['dedup_time_ms'] = self.stats.get('dedup_time_ms', 0) + dedup_time
        
        if exists:
            update_fs_table(path, blob_id)
            remove_from_queue(path)
            self.stats['dedup_hits'] = self.stats.get('dedup_hits', 0) + 1
            return
            
        # Time queue operation
        t0 = time.perf_counter()
        
        # Pass to compress via shared memory
        try:
            shm = shared_memory.SharedMemory(create=True, size=size)
            shm.buf[:size] = data
            
            # Track shared memory usage
            if 'shm_metrics' not in self.stats:
                self.stats['shm_metrics'] = {}
            self.stats['shm_metrics']['active_segments'] = self.stats.get('shm_metrics', {}).get('active_segments', 0) + 1
            self.stats['shm_metrics']['total_bytes'] = self.stats.get('shm_metrics', {}).get('total_bytes', 0) + size
            
            self.compress_queue.put({
                'path': path,
                'blob_id': blob_id,
                'shm_name': shm.name,
                'size': size,
                'method': 'shared_memory'
            }, timeout=30)
            
            queue_time = (time.perf_counter() - t0) * 1000
            self.stats['queue_time_ms'] = self.stats.get('queue_time_ms', 0) + queue_time
            
            shm.close()
            
        except Full:
            logger.warning(f"Compress queue full, waiting...")
            time.sleep(1)
        except Exception as e:
            logger.error(f"Shared memory error: {e}")
            
    def cleanup(self):
        """Clean up resources on shutdown."""
        pass
            
    def process_medium_file(self, path: str, file_path: Path, size: int):
        """Medium file: hash first, reread if needed."""
        # Stream hash with I/O coordination
        hasher = blake3.blake3()
        with disk_io_semaphore:  # Prevent disk thrashing
            with open(file_path, 'rb') as f:
                while chunk := f.read(1_000_000):
                    hasher.update(chunk)
                
        blob_id = hasher.hexdigest()
        
        # Check dedup
        if check_blob_exists(blob_id):
            update_fs_table(path, blob_id)
            remove_from_queue(path)
            self.stats['dedup_hits'] = self.stats.get('dedup_hits', 0) + 1
            return
            
        # Queue for compress (will reread)
        try:
            self.compress_queue.put({
                'path': path,
                'blob_id': blob_id,
                'size': size,
                'method': 'reread'
            }, timeout=30)
        except Full:
            logger.warning(f"Compress queue full")
            
    def process_large_file(self, path: str, file_path: Path, size: int):
        """Large file: stream everything."""
        # Stream hash with I/O coordination
        hasher = blake3.blake3()
        with disk_io_semaphore:  # Prevent disk thrashing
            with open(file_path, 'rb') as f:
                while chunk := f.read(1_000_000):
                    hasher.update(chunk)
                
        blob_id = hasher.hexdigest()
        
        # Check dedup
        if check_blob_exists(blob_id):
            update_fs_table(path, blob_id)
            remove_from_queue(path)
            self.stats['dedup_hits'] = self.stats.get('dedup_hits', 0) + 1
            return
            
        # Queue for stream processing
        try:
            self.compress_queue.put({
                'path': path,
                'blob_id': blob_id,
                'size': size,
                'method': 'stream'
            }, timeout=30)
        except Full:
            logger.warning(f"Compress queue full")


class CompressWorker(mp.Process):
    """Compress and stage blobs."""
    
    def __init__(self, worker_id: str, compress_queue: mp.Queue, stats: dict):
        super().__init__()
        self.worker_id = worker_id
        self.compress_queue = compress_queue
        self.stop_flag = mp.Event()
        self.stats = stats  # Shared manager dict
        # Initialize timing stats
        self.stats['wait_time_ms'] = 0
        self.stats['work_time_ms'] = 0
        self.stats['items_processed'] = 0
        self.stats['idle_cycles'] = 0
        
    def run(self):
        """Main worker loop."""
        # Configure worker logging
        if not verbose_mode:
            logger.remove()
            logger.add(
                sys.stdout,
                level="INFO",
                format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
            )
        logger.info(f"CompressWorker {self.worker_id} started")
        init_connection_pool()
        self.active_shm = set()  # Track active shared memory segments
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            t0 = time.perf_counter()
            try:
                item = self.compress_queue.get(timeout=0.1)
                wait_time = (time.perf_counter() - t0) * 1000
                self.stats['wait_time_ms'] = self.stats.get('wait_time_ms', 0) + wait_time
                
                work_start = time.perf_counter()
                self.process_item(item)
                work_time = (time.perf_counter() - work_start) * 1000
                self.stats['work_time_ms'] = self.stats.get('work_time_ms', 0) + work_time
                self.stats['items_processed'] = self.stats.get('items_processed', 0) + 1
                
            except Empty:
                self.stats['idle_cycles'] = self.stats.get('idle_cycles', 0) + 1
                self.stats['wait_time_ms'] = self.stats.get('wait_time_ms', 0) + 100  # timeout was 0.1s
                continue
            except Exception as e:
                logger.error(f"CompressWorker error: {e}")
                
        logger.info(f"CompressWorker {self.worker_id} stopping...")
        self.cleanup()
        logger.info(f"CompressWorker {self.worker_id} stopped")
        
    def process_item(self, item: dict):
        """Process work item from hash worker."""
        path = item['path']
        blob_id = item['blob_id']
        size = item['size']
        method = item['method']
        
        # Get data based on method
        if method == 'shared_memory':
            # Get from shared memory
            try:
                shm = shared_memory.SharedMemory(name=item['shm_name'])
                self.active_shm.add(item['shm_name'])  # Track for cleanup
                data = bytes(shm.buf[:size])
                shm.close()
                shm.unlink()  # Clean up
                self.active_shm.discard(item['shm_name'])
            except Exception as e:
                logger.error(f"Shared memory error: {e}")
                return
                
        elif method == 'reread':
            # Read from disk with I/O coordination
            file_path = Path("/Volumes") / path
            if not file_path.exists():
                logger.warning(f"File disappeared: {path}")
                remove_from_queue(path)
                return
            with disk_io_semaphore:  # Prevent disk thrashing
                data = file_path.read_bytes()
            
        elif method == 'stream':
            # Stream compress (TODO: implement streaming)
            file_path = Path("/Volumes") / path
            if not file_path.exists():
                logger.warning(f"File disappeared: {path}")
                remove_from_queue(path)
                return
            with disk_io_semaphore:  # Prevent disk thrashing
                data = file_path.read_bytes()
            
        # Compress
        compressed = self.compress_data(data)
        
        # Stage for upload
        self.stage_blob(blob_id, compressed)
        
        # Update database
        update_fs_table(path, blob_id)
        remove_from_queue(path)
        
        self.stats['files_compressed'] = self.stats.get('files_compressed', 0) + 1
        self.stats['bytes_compressed'] = self.stats.get('bytes_compressed', 0) + size
        
    def compress_data(self, data: bytes) -> bytes:
        """Compress and wrap data as JSON blob."""
        # Compress with LZ4
        frames = []
        CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks
        
        offset = 0
        while offset < len(data):
            chunk = data[offset:offset + CHUNK_SIZE]
            compressed = lz4.frame.compress(chunk)
            import base64
            b64_frame = base64.b64encode(compressed).decode('ascii')
            frames.append(b64_frame)
            offset += CHUNK_SIZE
            
        # Create JSON blob
        blob = {
            "content": {
                "encoding": "lz4-multiframe",
                "frames": frames
            },
            "metadata": {
                "size": len(data),
                "mtime": time.time(),
                "filetype": "unknown",
                "encryption": False
            }
        }
        
        return json.dumps(blob).encode('utf-8')
        
    def stage_blob(self, blob_id: str, data: bytes):
        """Stage blob to disk for batch upload."""
        AA = blob_id[:2]
        BB = blob_id[2:4]
        
        # Create directory structure
        staging_dir = Path(STAGING_PATH) / AA / BB
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Write blob
        blob_path = staging_dir / blob_id
        blob_path.write_bytes(data)
        
        # Blob staged successfully
        
    def cleanup(self):
        """Clean up any remaining shared memory segments."""
        for shm_name in self.active_shm:
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.close()
                shm.unlink()
            except Exception:
                pass


class UploadWorker(mp.Process):
    """Batch upload staged blobs."""
    
    def __init__(self, worker_id: str, thresholds: dict):
        super().__init__()
        self.worker_id = worker_id
        self.thresholds = thresholds
        self.stop_flag = mp.Event()
        self.pending = []  # List of (rel_path, full_path) tuples
        self.collected = set()  # Track what we've already collected
        self.last_upload = time.time()
        
    def run(self):
        """Main worker loop."""
        # Configure worker logging
        if not verbose_mode:
            logger.remove()
            logger.add(
                sys.stdout,
                level="INFO",
                format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
            )
        logger.info(f"UploadWorker {self.worker_id} started")
        self.total_uploaded = 0
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            self.collect_staged()
            
            # Upload if batch ready or timeout
            if len(self.pending) >= self.thresholds.get('batch_size', 100):
                self.upload_batch()
            elif time.time() - self.last_upload > self.thresholds.get('batch_wait', 5.0):
                if self.pending:
                    self.upload_batch()
                    
            time.sleep(0.5)
            
        # Final upload on shutdown
        logger.info(f"UploadWorker {self.worker_id} flushing {len(self.pending)} pending files...")
        if self.pending:
            self.upload_batch()
            
        logger.info(f"UploadWorker {self.worker_id} stopped (uploaded {self.total_uploaded} total)")
        
    def collect_staged(self):
        """Collect newly staged files."""
        staging_path = Path(STAGING_PATH)
        
        for blob_path in staging_path.glob("*/*/*"):
            if blob_path.is_file():
                # Skip if already collected
                path_str = str(blob_path)
                if path_str in self.collected:
                    continue
                    
                # Mark as collected and add to pending with full path for DB update
                self.collected.add(path_str)
                rel_path = blob_path.relative_to(staging_path)
                # Store both relative path (for rsync) and full path (to extract pth for DB)
                self.pending.append((str(rel_path), str(blob_path)))
                
                # Don't collect too many at once
                if len(self.pending) >= self.thresholds.get('batch_size', 100) * 2:
                    break
                    
    def upload_batch(self):
        """Upload batch of blobs."""
        if not self.pending:
            return
            
        start = time.time()
        
        # Create manifest for rsync (just the relative paths)
        manifest_path = Path(f"/tmp/manifest_{self.worker_id}.txt")
        rel_paths = [rel_path for rel_path, _ in self.pending]
        manifest_path.write_text('\n'.join(rel_paths))
        
        # Batch rsync
        try:
            result = subprocess.run([
                "rsync",
                "-av",
                "--files-from", str(manifest_path),
                "--relative",
                "--remove-source-files",  # Delete after upload
                "-e", f"ssh -p {SSH_PORT} -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=60",
                STAGING_PATH,
                f"{UPLOAD_HOST}:{UPLOAD_PATH}/"
            ], capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                elapsed = time.time() - start
                self.total_uploaded += len(self.pending)
                logger.info(
                    f"Uploaded batch: {len(self.pending)} files in {elapsed:.1f}s"
                )
                
                # NOW mark files as uploaded in database
                self.mark_files_uploaded()
                
            else:
                logger.error(f"Rsync failed: {result.stderr}")
                # On error, remove from collected so we can retry
                for _, full_path in self.pending:
                    self.collected.discard(full_path)
                
        except subprocess.TimeoutExpired:
            logger.error("Rsync timeout")
        except Exception as e:
            logger.error(f"Upload error: {e}")
            
        # Clear pending but keep collected set to avoid re-collecting
        self.pending.clear()
        self.last_upload = time.time()
        manifest_path.unlink(missing_ok=True)
        
    def mark_files_uploaded(self):
        """Mark files as uploaded in database after successful rsync."""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                for rel_path, full_path in self.pending:
                    # Extract the original path from the staged blob path
                    # Staged path format: /tmp/n2s_staging/AA/BB/blobid
                    # We need to find the file with this blobid
                    blob_id = Path(full_path).name
                    
                    # Update ALL files with this blobid as uploaded
                    cur.execute("""
                        UPDATE fs 
                        SET uploaded = NOW()
                        WHERE blobid = %s AND uploaded IS NULL
                    """, (blob_id,))
                    
                conn.commit()
                
        except psycopg2.Error as e:
            logger.error(f"Failed to mark files as uploaded: {e}")
            conn.rollback()
        finally:
            return_db_connection(conn)


class AutoTuningOrchestrator:
    """Main orchestrator with auto-tuning."""
    
    def __init__(self):
        # Create queues
        global compress_queue, db_ops_queue
        compress_queue = mp.Queue(maxsize=100)
        self.compress_queue = compress_queue
        db_ops_queue = mp.Queue(maxsize=1000)  # For async DB operations
        self.db_ops_queue = db_ops_queue
        
        # Worker pools
        self.hash_workers = []
        self.compress_workers = []
        self.upload_workers = []
        self.db_worker = None  # Single DB worker for async operations
        
        # Shared worker stats
        self.hash_stats = []
        self.compress_stats = []
        self.db_stats = self.manager.dict()  # Stats for DB worker
        
        # Tunable thresholds
        self.manager = mp.Manager()
        # Initial thresholds - will be dynamically adjusted based on memory pressure
        available_ram = psutil.virtual_memory().available
        self.thresholds = self.manager.dict({
            'shared_memory_max': 1_000_000_000,  # Start at 1GB! We have plenty of RAM
            'reread_threshold': 50_000_000,   # 50MB
            'batch_size': 100,
            'batch_wait': 5.0,
            'disk_io_semaphores': 3,  # Start with 3, tune based on performance
        })
        
        # Create the global semaphore with initial value
        global disk_io_semaphore
        disk_io_semaphore = mp.Semaphore(self.thresholds['disk_io_semaphores'])
        logger.info(f"Initialized with shared_memory_max={humanize.naturalsize(self.thresholds['shared_memory_max'])}, disk_io_semaphores={self.thresholds['disk_io_semaphores']}")
        
        # Performance metrics
        self.metrics = {
            'start_time': time.time(),
            'files_processed': 0,
            'dedup_hits': 0,
            'last_tune': time.time(),
        }
        
        # System I/O tracking
        self.last_disk_io = psutil.disk_io_counters()
        self.last_net_io = psutil.net_io_counters()
        self.last_io_time = time.time()
        
        # Tuning parameters
        self.tune_interval = 30  # seconds
        self.aggressive_tune = True  # Start aggressive, learn over time
        
        # Throughput tracking for smarter tuning
        self.throughput_history = []  # List of (timestamp, files/sec, config)
        self.last_tuning_action = None  # What we did last
        self.last_throughput_before_tuning = 0
        self.tuning_cooldown = 20  # Wait this long to measure impact (less than tune_interval!)
        self.action_blacklist = {}  # Failed actions -> timestamp when blacklist expires
        
        # Create tuning log directory
        self.tuning_log_path = Path.home() / ".n2s" / "tuning.log"
        self.tuning_log_path.parent.mkdir(exist_ok=True)
        
        # Maintenance parameters
        self.maintenance_interval = 300  # 5 minutes
        self.last_maintenance = time.time()
        self.stale_claim_minutes = 30
        self.min_workers = 1
        self.max_workers = mp.cpu_count() * 2
        
        # Set signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    def start(self):
        """Start orchestrator."""
        logger.info("Starting auto-tuning orchestrator")
        
        # Initialize database pool
        init_connection_pool()
        
        # Start DB worker for async operations
        self.db_worker = SimpleDBWorker("db_0", self.db_ops_queue, self.db_stats)
        self.db_worker.start()
        logger.info("Started DB worker for async operations")
        
        # Start initial workers
        self.spawn_hash_workers(2)
        self.spawn_compress_workers(2)
        self.spawn_upload_workers(1)
        logger.info(f"Initial workers: Hash={len(self.hash_workers)} Compress={len(self.compress_workers)} Upload={len(self.upload_workers)}")
        
        # Main loop
        stats_counter = 0
        while not shutdown_flag.is_set():
            time.sleep(5)
            stats_counter += 5
            
            # Tune periodically
            if time.time() - self.metrics['last_tune'] > self.tune_interval:
                self.tune()
                self.metrics['last_tune'] = time.time()
                
            # Maintenance periodically
            if time.time() - self.last_maintenance > self.maintenance_interval:
                self.run_maintenance()
                self.last_maintenance = time.time()
                
            # Print stats
            self.print_stats()
            
            # Print separator every minute for readability
            if stats_counter >= 60:
                logger.info("-" * 80)
                stats_counter = 0
            
        # Shutdown
        self.shutdown()
        
    def spawn_hash_workers(self, count: int):
        """Spawn hash workers."""
        for i in range(count):
            worker_id = f"hash_{len(self.hash_workers)}"
            stats = self.manager.dict()  # Create shared dict for this worker
            worker = HashWorker(worker_id, self.compress_queue, self.thresholds, stats)
            worker.start()
            self.hash_workers.append(worker)
            self.hash_stats.append(stats)
            
        # Don't log spawns during tuning, the tune() method will log it
        pass
        
    def spawn_compress_workers(self, count: int):
        """Spawn compress workers."""
        for i in range(count):
            worker_id = f"compress_{len(self.compress_workers)}"
            stats = self.manager.dict()  # Create shared dict for this worker
            worker = CompressWorker(worker_id, self.compress_queue, stats)
            worker.start()
            self.compress_workers.append(worker)
            self.compress_stats.append(stats)
            
        # Don't log spawns during tuning, the tune() method will log it
        pass
        
    def spawn_upload_workers(self, count: int):
        """Spawn upload workers."""
        for i in range(count):
            worker_id = f"upload_{len(self.upload_workers)}"
            worker = UploadWorker(worker_id, self.thresholds)
            worker.start()
            self.upload_workers.append(worker)
            
        # Don't log spawns during tuning, the tune() method will log it
        pass
        
    def tune(self):
        """Auto-tune based on THROUGHPUT changes (both files/sec and MB/sec)."""
        
        # Calculate current throughput metrics
        elapsed = time.time() - self.metrics['start_time']
        total_processed = sum(s.get('files_processed', 0) for s in self.hash_stats)
        total_bytes = sum(s.get('bytes_hashed', 0) for s in self.hash_stats)
        current_files_per_sec = total_processed / max(1, elapsed)
        current_mb_per_sec = total_bytes / max(1, elapsed) / 1_000_000
        
        logger.debug(f"Tuning check: {current_files_per_sec:.1f} f/s | {current_mb_per_sec:.1f} MB/s")
        
        # Track throughput history (now includes MB/s)
        current_config = (len(self.hash_workers), len(self.compress_workers), len(self.upload_workers))
        self.throughput_history.append((
            time.time(), 
            current_files_per_sec, 
            current_mb_per_sec,
            current_config
        ))
        
        # Keep only last 10 measurements
        if len(self.throughput_history) > 10:
            self.throughput_history.pop(0)
        
        # Check if we're in cooldown period after last tuning
        if self.last_tuning_action and (time.time() - self.last_tuning_action['time']) < self.tuning_cooldown:
            remaining = self.tuning_cooldown - (time.time() - self.last_tuning_action['time'])
            logger.debug(f"In cooldown for {remaining:.0f}s after {self.last_tuning_action['action']}")
            return  # Still measuring impact of last change
        
        # Analyze throughput trend (prioritize MB/s over files/sec)
        throughput_trend = "stable"
        if len(self.throughput_history) >= 3:
            # Check MB/s trend (more important than file count)
            recent_mb_per_sec = [h[2] for h in self.throughput_history[-3:]]
            recent_files_per_sec = [h[1] for h in self.throughput_history[-3:]]
            
            # MB/s is primary metric
            if recent_mb_per_sec[-1] > recent_mb_per_sec[0] * 1.1:
                throughput_trend = "improving"
            elif recent_mb_per_sec[-1] < recent_mb_per_sec[0] * 0.9:
                throughput_trend = "declining"
            # If MB/s stable, check files/sec
            elif recent_files_per_sec[-1] > recent_files_per_sec[0] * 1.1:
                throughput_trend = "improving"
            elif recent_files_per_sec[-1] < recent_files_per_sec[0] * 0.9:
                throughput_trend = "declining"
        
        # If last action made things worse, blacklist it and try opposite
        if self.last_tuning_action and throughput_trend == "declining":
            logger.info(f"🔴 Last tuning made things worse (was {self.last_throughput_before_tuning:.1f} f/s, now {current_files_per_sec:.1f} f/s | {current_mb_per_sec:.1f} MB/s)")
            
            # Blacklist the failed action for 5 minutes
            failed_action = self.last_tuning_action['action'].split('+')[0] if '+' in self.last_tuning_action['action'] else self.last_tuning_action['action']
            self.action_blacklist[failed_action] = time.time() + 300  # 5 minute blacklist
            logger.info(f"Blacklisting {failed_action} actions for 5 minutes")
            
            # Revert or try opposite
            if "Hash+" in self.last_tuning_action['action']:
                # Added hash workers didn't help, try compress
                if len(self.compress_workers) < MAX_COMPRESS and not self.is_blacklisted("Compress"):
                    self.spawn_compress_workers(1)
                    self.record_tuning_action("Compress+1", current_files_per_sec, current_mb_per_sec)
            elif "Compress+" in self.last_tuning_action['action']:
                # Added compress didn't help, try hash
                if len(self.hash_workers) < MAX_HASH and not self.is_blacklisted("Hash"):
                    self.spawn_hash_workers(1)
                    self.record_tuning_action("Hash+1", current_files_per_sec, current_mb_per_sec)
            return
        
        # If throughput is improving, maybe try more of the same
        if throughput_trend == "improving" and self.last_tuning_action:
            logger.info(f"🟢 Throughput improving ({current_files_per_sec:.1f} f/s | {current_mb_per_sec:.1f} MB/s)")
            # Could repeat last action if it helped
            return
        
        # If stable or no recent action, try something new
        if throughput_trend == "stable":
            # Get worker efficiency metrics
            compress_idle_pct = self.get_compress_idle_percentage()
            
            # Experimental probing - try different things
            staged_files = len(list(Path(STAGING_PATH).glob("*/*/*")))
            worker_ratio = len(self.compress_workers) / max(1, len(self.hash_workers))
            
            # First, check if we can adjust memory thresholds based on pressure
            mem = psutil.virtual_memory()
            if mem.percent < 60 and self.thresholds['shared_memory_max'] < 2_000_000_000:
                # Plenty of memory available, increase threshold (up to 2GB)
                old_max = self.thresholds['shared_memory_max']
                new_max = min(2_000_000_000, int(old_max * 1.5))
                self.thresholds['shared_memory_max'] = new_max
                logger.info(f"🎯 TUNING: Memory threshold {humanize.naturalsize(old_max)} → {humanize.naturalsize(new_max)} (mem: {mem.percent:.0f}%)")
                self.record_tuning_action(f"MemMax→{humanize.naturalsize(new_max)}", current_files_per_sec, current_mb_per_sec)
                return
            elif mem.percent > 85 and self.thresholds['shared_memory_max'] > 100_000_000:
                # Memory pressure high, reduce threshold (but keep at least 100MB)
                old_max = self.thresholds['shared_memory_max']
                new_max = max(100_000_000, int(old_max * 0.5))
                self.thresholds['shared_memory_max'] = new_max
                logger.info(f"🎯 TUNING: Memory threshold {humanize.naturalsize(old_max)} → {humanize.naturalsize(new_max)} (mem: {mem.percent:.0f}% HIGH)")
                self.record_tuning_action(f"MemMax→{humanize.naturalsize(new_max)}", current_files_per_sec, current_mb_per_sec)
                return
            
            # Decision tree based on bottleneck indicators
            if staged_files > 200:
                # Backlog at staging - upload is slow
                if len(self.upload_workers) < MAX_UPLOAD and not self.is_blacklisted("Upload"):
                    self.spawn_upload_workers(1)
                    self.record_tuning_action("Upload+1", current_files_per_sec, current_mb_per_sec)
            elif compress_idle_pct > 80 or worker_ratio > 1.5:
                # Compress workers are starved - need more hash workers
                if len(self.hash_workers) < MAX_HASH and not self.is_blacklisted("Hash"):
                    self.spawn_hash_workers(1)
                    self.record_tuning_action("Hash+1", current_files_per_sec, current_mb_per_sec)
                elif worker_ratio > 2 and len(self.compress_workers) > 2:
                    # Too many compress workers, remove one
                    self.remove_compress_worker()
                    self.record_tuning_action("Compress-1", current_files_per_sec, current_mb_per_sec)
                elif len(self.hash_workers) > 2:
                    # Check for disk thrashing and adjust semaphores
                    is_thrashing, thrash_ratio = self.detect_disk_thrashing()
                    if is_thrashing and thrash_ratio > 10 and self.thresholds['disk_io_semaphores'] > 1:
                        # Severe thrashing - reduce semaphores
                        self.adjust_disk_semaphores(-1)
                        self.record_tuning_action(f"Semaphores-1→{self.thresholds['disk_io_semaphores']} (thrashing)", current_files_per_sec, current_mb_per_sec)
                    elif not is_thrashing and self.thresholds['disk_io_semaphores'] < len(self.hash_workers):
                        # No thrashing and could use more concurrent reads
                        self.adjust_disk_semaphores(1)
                        self.record_tuning_action(f"Semaphores+1→{self.thresholds['disk_io_semaphores']}", current_files_per_sec, current_mb_per_sec)
            else:
                # Try more compress workers
                if len(self.compress_workers) < MAX_COMPRESS and worker_ratio < 1.5 and not self.is_blacklisted("Compress"):
                    self.spawn_compress_workers(1)
                    self.record_tuning_action("Compress+1", current_files_per_sec, current_mb_per_sec)
                    
    def record_tuning_action(self, action: str, current_files_per_sec: float, current_mb_per_sec: float):
        """Record what tuning action we took."""
        self.last_tuning_action = {
            'action': action,
            'time': time.time(),
            'config': (len(self.hash_workers), len(self.compress_workers), len(self.upload_workers))
        }
        self.last_throughput_before_tuning = current_files_per_sec
        self.last_mb_per_sec_before_tuning = current_mb_per_sec
        logger.info(f"🎯 TUNING: {action} (baseline: {current_files_per_sec:.1f} f/s | {current_mb_per_sec:.1f} MB/s)")
        
        # Write to persistent log
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "baseline_files_per_sec": current_files_per_sec,
            "baseline_mb_per_sec": current_mb_per_sec,
            "workers": {
                "hash": len(self.hash_workers),
                "compress": len(self.compress_workers),
                "upload": len(self.upload_workers)
            }
        }
        try:
            with open(self.tuning_log_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write tuning log: {e}")
    
    def is_blacklisted(self, action_type: str) -> bool:
        """Check if an action type is currently blacklisted."""
        if action_type in self.action_blacklist:
            if time.time() < self.action_blacklist[action_type]:
                return True
            else:
                # Expired, remove from blacklist
                del self.action_blacklist[action_type]
        return False
    
    def get_compress_idle_percentage(self) -> float:
        """Calculate percentage of time compress workers are idle."""
        if not self.compress_stats:
            return 0
        
        total_wait = sum(s.get('wait_time_ms', 0) for s in self.compress_stats)
        total_work = sum(s.get('work_time_ms', 0) for s in self.compress_stats)
        total_time = total_wait + total_work
        
        if total_time > 0:
            return (total_wait / total_time) * 100
        return 0
    
    def adjust_disk_semaphores(self, delta: int):
        """Adjust the number of disk I/O semaphores."""
        global disk_io_semaphore
        
        old_count = self.thresholds['disk_io_semaphores']
        new_count = max(1, min(8, old_count + delta))  # Between 1 and 8
        
        if new_count != old_count:
            self.thresholds['disk_io_semaphores'] = new_count
            # Note: We can't actually change the semaphore count at runtime
            # Workers will need to restart to use the new value
            # For now, just log the change
            logger.info(f"Disk I/O semaphores: {old_count} → {new_count}")
            # TODO: Implement worker restart to apply new semaphore count
    
    def detect_disk_thrashing(self) -> tuple[bool, float]:
        """Detect if disk is thrashing based on read latencies."""
        global disk_read_latencies
        
        if len(disk_read_latencies) < 20:
            return False, 0.0
        
        # Calculate p50 and p95
        import numpy as np
        sorted_latencies = sorted(disk_read_latencies)
        p50 = np.percentile(sorted_latencies, 50)
        p95 = np.percentile(sorted_latencies, 95)
        
        # High ratio means high variability = likely thrashing
        if p50 > 0:
            thrashing_ratio = p95 / p50
            
            # Log if concerning
            if thrashing_ratio > 10:
                logger.warning(f"Disk thrashing detected! P50: {p50:.1f}ms, P95: {p95:.1f}ms, ratio: {thrashing_ratio:.1f}")
                return True, thrashing_ratio
            elif thrashing_ratio > 5:
                logger.info(f"Disk latency variability high. P50: {p50:.1f}ms, P95: {p95:.1f}ms, ratio: {thrashing_ratio:.1f}")
                return True, thrashing_ratio
        
        return False, thrashing_ratio if p50 > 0 else 0.0
    
    def get_memory_pressure_action(self) -> Optional[str]:
        """Determine if memory adjustments are needed based on pressure."""
        mem = psutil.virtual_memory()
        current_max = self.thresholds['shared_memory_max']
        
        # Get process-specific memory usage
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)  # All child processes
        self_usage = resource.getrusage(resource.RUSAGE_SELF)  # This process
        
        # On macOS ru_maxrss is in bytes, on Linux it's in KB
        if sys.platform == 'darwin':
            process_rss_mb = (usage.ru_maxrss + self_usage.ru_maxrss) / 1024 / 1024
        else:
            process_rss_mb = (usage.ru_maxrss + self_usage.ru_maxrss) / 1024
        
        # Track shared memory actually in use
        shm_in_use = sum(s.get('shm_metrics', {}).get('total_bytes', 0) for s in self.hash_stats)
        
        # Calculate our process family's impact on system
        our_memory_pct = (process_rss_mb * 1024 * 1024) / mem.total * 100
        
        logger.debug(f"Memory: System {mem.percent:.0f}%, Our processes {our_memory_pct:.1f}% ({process_rss_mb:.0f}MB), SHM {shm_in_use/1_000_000:.0f}MB")
        
        if mem.percent < 50 and our_memory_pct < 20:
            # Very low pressure and we're not using much
            if current_max < 2_000_000_000 and shm_in_use > current_max * 0.8:
                return "increase_aggressive"  # Files are hitting the limit
        elif mem.percent < 70 and our_memory_pct < 30:
            # Low pressure - can increase if needed
            if current_max < 1_500_000_000 and shm_in_use > current_max * 0.9:
                return "increase_moderate"
        elif mem.percent > 85 or our_memory_pct > 40:
            # High pressure or we're using too much
            return "decrease"
        elif mem.percent > 90 or our_memory_pct > 50:
            # Critical - decrease immediately
            return "decrease_urgent"
        
        return None
                                                       
    def remove_hash_worker(self):
        """Remove a hash worker."""
        if self.hash_workers:
            worker = self.hash_workers.pop(0)
            self.hash_stats.pop(0)
            worker.stop_flag.set()
            
    def remove_compress_worker(self):
        """Remove a compress worker."""
        if self.compress_workers:
            worker = self.compress_workers.pop(0)
            self.compress_stats.pop(0)
            worker.stop_flag.set()
            
    def remove_upload_worker(self):
        """Remove an upload worker."""
        if self.upload_workers:
            worker = self.upload_workers.pop(0)
            worker.stop_flag.set()
            
    def run_maintenance(self):
        """Run periodic maintenance tasks."""
        logger.info("🔧 MAINTENANCE: Running queue cleanup...")
        conn = get_db_connection()
        
        try:
            with conn.cursor() as cur:
                # Reset stale claims
                cur.execute("""
                    UPDATE work_queue 
                    SET claimed_at = NULL, claimed_by = NULL
                    WHERE claimed_at < NOW() - INTERVAL '%s minutes'
                    RETURNING pth
                """, (self.stale_claim_minutes,))
                
                reset_count = cur.rowcount
                if reset_count > 0:
                    logger.info(f"  Reset {reset_count} stale claims")
                
                # Remove completed files from queue
                cur.execute("""
                    DELETE FROM work_queue
                    WHERE pth IN (
                        SELECT wq.pth
                        FROM work_queue wq
                        JOIN fs ON fs.pth = wq.pth
                        WHERE fs.blobid IS NOT NULL
                           OR fs.last_missing_at IS NOT NULL
                           OR fs.cantfind = true
                    )
                """)
                
                removed_count = cur.rowcount
                if removed_count > 0:
                    logger.info(f"  Removed {removed_count} completed files from queue")
                
                # Vacuum analyze work_queue (less frequently)
                if hasattr(self, 'vacuum_counter'):
                    self.vacuum_counter += 1
                else:
                    self.vacuum_counter = 1
                    
                if self.vacuum_counter % 12 == 0:  # Every hour
                    old_isolation = conn.isolation_level
                    conn.set_isolation_level(0)  # VACUUM requires autocommit
                    cur.execute("VACUUM ANALYZE work_queue")
                    conn.set_isolation_level(old_isolation)
                    logger.info("  Vacuumed work_queue table")
                
                conn.commit()
                
        except psycopg2.Error as e:
            logger.error(f"Maintenance error: {e}")
            conn.rollback()
        finally:
            return_db_connection(conn)
    
    def collect_system_metrics(self):
        """Collect system I/O and resource metrics."""
        current_time = time.time()
        time_delta = current_time - self.last_io_time
        
        # Disk I/O
        disk_io = psutil.disk_io_counters()
        disk_read_mbps = (disk_io.read_bytes - self.last_disk_io.read_bytes) / time_delta / 1_000_000
        disk_write_mbps = (disk_io.write_bytes - self.last_disk_io.write_bytes) / time_delta / 1_000_000
        
        # Network I/O
        net_io = psutil.net_io_counters()
        net_upload_mbps = (net_io.bytes_sent - self.last_net_io.bytes_sent) / time_delta / 1_000_000
        
        # Update last values
        self.last_disk_io = disk_io
        self.last_net_io = net_io
        self.last_io_time = current_time
        
        # CPU per core
        cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
        
        # Memory details
        mem = psutil.virtual_memory()
        
        # Shared memory tracking
        shm_bytes = 0
        shm_segments = 0
        for stats in self.hash_stats:
            if 'shm_metrics' in stats:
                shm_bytes += stats['shm_metrics'].get('total_bytes', 0)
                shm_segments += stats['shm_metrics'].get('active_segments', 0)
        
        # Process memory
        process = psutil.Process()
        process_mem = process.memory_info()
        
        return {
            'disk_read_mbps': disk_read_mbps,
            'disk_write_mbps': disk_write_mbps,
            'net_upload_mbps': net_upload_mbps,
            'cpu_per_core': cpu_per_core,
            'cpu_max_core': max(cpu_per_core) if cpu_per_core else 0,
            'mem_total_gb': mem.total / 1_000_000_000,
            'mem_used_gb': mem.used / 1_000_000_000,
            'mem_available_gb': mem.available / 1_000_000_000,
            'mem_percent': mem.percent,
            'shm_bytes_mb': shm_bytes / 1_000_000,
            'shm_segments': shm_segments,
            'process_rss_mb': process_mem.rss / 1_000_000,
            'process_vms_mb': process_mem.vms / 1_000_000
        }
    
    def calculate_worker_efficiency(self, worker_stats: list, worker_type: str) -> dict:
        """Calculate worker efficiency metrics."""
        if not worker_stats:
            return {}
            
        if worker_type == 'hash':
            total_read = sum(s.get('read_time_ms', 0) for s in worker_stats)
            total_hash = sum(s.get('hash_time_ms', 0) for s in worker_stats)
            total_dedup = sum(s.get('dedup_time_ms', 0) for s in worker_stats)
            total_time = total_read + total_hash + total_dedup
            
            if total_time > 0:
                return {
                    'read_pct': (total_read / total_time) * 100,
                    'hash_pct': (total_hash / total_time) * 100,
                    'dedup_pct': (total_dedup / total_time) * 100,
                    'bytes_per_sec': sum(s.get('bytes_read', 0) for s in worker_stats) / max(1, total_time/1000)
                }
        
        elif worker_type == 'compress':
            total_wait = sum(s.get('wait_time_ms', 0) for s in worker_stats)
            total_work = sum(s.get('work_time_ms', 0) for s in worker_stats)
            total_time = total_wait + total_work
            
            if total_time > 0:
                return {
                    'idle_pct': (total_wait / total_time) * 100,
                    'work_pct': (total_work / total_time) * 100,
                    'items': sum(s.get('items_processed', 0) for s in worker_stats)
                }
        
        return {}
    
    def calculate_db_stats(self) -> dict:
        """Calculate database latency statistics."""
        import numpy as np
        
        stats = {}
        for query_type in ['claim', 'dedup']:
            if db_latencies[query_type]:
                stats[query_type] = {
                    'p50': np.percentile(db_latencies[query_type], 50),
                    'p95': np.percentile(db_latencies[query_type], 95),
                    'p99': np.percentile(db_latencies[query_type], 99)
                }
        return stats
    
    def print_detailed_metrics(self, hash_eff: dict, compress_eff: dict, sys_metrics: dict, db_stats: dict):
        """Print detailed metrics dashboard."""
        logger.info("-" * 80)
        logger.info("BOTTLENECK ANALYSIS:")
        
        if hash_eff:
            logger.info(
                f"├─ Hash Workers: {hash_eff.get('read_pct', 0):.0f}% reading, "
                f"{hash_eff.get('hash_pct', 0):.0f}% hashing, "
                f"{hash_eff.get('dedup_pct', 0):.0f}% DB"
            )
            
        if compress_eff:
            logger.info(
                f"├─ Compress Workers: {compress_eff.get('idle_pct', 0):.0f}% idle, "
                f"{compress_eff.get('work_pct', 0):.0f}% working"
            )
            
        logger.info(
            f"├─ I/O: Read={sys_metrics['disk_read_mbps']:.1f}MB/s "
            f"Write={sys_metrics['disk_write_mbps']:.1f}MB/s "
            f"Upload={sys_metrics['net_upload_mbps']:.1f}MB/s"
        )
        
        logger.info(
            f"├─ Memory: {sys_metrics['mem_used_gb']:.1f}GB/{sys_metrics['mem_total_gb']:.1f}GB "
            f"({sys_metrics['mem_percent']:.0f}%) | "
            f"Shared: {sys_metrics['shm_bytes_mb']:.1f}MB/{sys_metrics['shm_segments']:.0f} segs | "
            f"Process: {sys_metrics['process_rss_mb']:.0f}MB"
        )
        
        logger.info(
            f"├─ CPU: Max core={sys_metrics['cpu_max_core']:.0f}% "
        )
        
        if db_stats:
            if 'claim' in db_stats:
                logger.info(
                    f"├─ DB Claim: p50={db_stats['claim']['p50']:.0f}ms "
                    f"p95={db_stats['claim']['p95']:.0f}ms"
                )
            if 'dedup' in db_stats:
                logger.info(
                    f"└─ DB Dedup: p50={db_stats['dedup']['p50']:.0f}ms "
                    f"p95={db_stats['dedup']['p95']:.0f}ms"
                )
        
        # Diagnosis
        diagnosis = self.diagnose_bottleneck(hash_eff, compress_eff, sys_metrics)
        logger.info(f"DIAGNOSIS: {diagnosis}")
        logger.info("-" * 80)
    
    def diagnose_bottleneck(self, hash_eff: dict, compress_eff: dict, sys_metrics: dict) -> str:
        """Diagnose the system bottleneck based on metrics."""
        if sys_metrics.get('mem_percent', 0) > 90:
            return "MEMORY CRITICAL - reduce workers or batch size"
        elif sys_metrics.get('shm_bytes_mb', 0) > 3.5:  # macOS limit is 4MB
            return "Shared memory near limit - process smaller files differently"
        elif compress_eff.get('idle_pct', 0) > 60:
            return "Add hash workers (compress workers starved)"
        elif hash_eff.get('read_pct', 0) > 70:
            return "I/O bound - disk read is bottleneck"
        elif hash_eff.get('dedup_pct', 0) > 30:
            return "Database is slow - optimize queries"
        elif sys_metrics['cpu_max_core'] > 90:
            return "CPU bound - at capacity"
        elif sys_metrics['net_upload_mbps'] < 1 and len(list(Path(STAGING_PATH).glob("*/*/*"))) > 50:
            return "Network upload is slow"
        elif sys_metrics.get('mem_percent', 0) > 80:
            return "Memory pressure high - monitor closely"
        else:
            return "System balanced - monitor for changes"
            
    def print_stats(self):
        """Print current statistics with detailed metrics."""
        elapsed = time.time() - self.metrics['start_time']
        
        # Collect worker stats from shared dicts
        total_processed = sum(s.get('files_processed', 0) for s in self.hash_stats)
        total_dedup = sum(s.get('dedup_hits', 0) for s in self.hash_stats)
        total_compressed = sum(s.get('files_compressed', 0) for s in self.compress_stats)
        total_bytes = sum(s.get('bytes_hashed', 0) for s in self.hash_stats)
        
        # Get queue sizes
        try:
            compress_queue_size = self.compress_queue.qsize()
        except NotImplementedError:
            compress_queue_size = -1  # Use -1 as sentinel for macOS
            
        # Count staged files
        staged_files = len(list(Path(STAGING_PATH).glob("*/*/*")))
        
        # Get remaining work from database
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM work_queue WHERE claimed_at IS NULL")
                remaining_work = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM work_queue WHERE claimed_at IS NOT NULL")
                claimed_work = cur.fetchone()[0]
        except:
            remaining_work = 0
            claimed_work = 0
        finally:
            return_db_connection(conn)
            
        # Calculate rates
        if elapsed > 0:
            hash_rate = total_processed / elapsed
            compress_rate = total_compressed / elapsed
            throughput_mb = total_bytes / elapsed / 1_000_000
        else:
            hash_rate = compress_rate = throughput_mb = 0
            
        # Calculate ETA
        total_remaining = remaining_work + claimed_work
        if hash_rate > 0 and total_remaining > 0:
            eta_seconds = total_remaining / hash_rate
            eta_str = humanize.naturaldelta(eta_seconds)
        else:
            eta_str = "unknown"
            
        # Worker status line (contention indicators)
        queue_indicator = ""
        if compress_queue_size == -1:  # macOS
            queue_indicator = "[?]"
        elif compress_queue_size > 80:
            queue_indicator = "[HIGH]"
        elif compress_queue_size > 50:
            queue_indicator = "[MED]"
        else:
            queue_indicator = "[LOW]"
            
        stage_indicator = ""
        if staged_files > 500:
            stage_indicator = "[BACKLOG]"
        elif staged_files > 200:
            stage_indicator = "[BUSY]"
        else:
            stage_indicator = "[OK]"
            
        # Calculate contention indicators
        cpu_pct = psutil.cpu_percent(interval=0.1)
        mem_pct = psutil.virtual_memory().percent
        
        # Contention/pressure indicators - adjusted for macOS
        bottleneck = ""
        if compress_queue_size == -1:
            # macOS: infer from worker ratio and CPU
            if len(self.compress_workers) < len(self.hash_workers) and cpu_pct < 50:
                bottleneck = "[C-SLOW]"  # Likely compress bottleneck
            elif staged_files > 500:
                bottleneck = "[U-SLOW]"
            else:
                bottleneck = "[OK]"
        elif compress_queue_size > 70:
            bottleneck = "[C-SLOW]"
        elif staged_files > 500:
            bottleneck = "[U-SLOW]"
        elif claimed_work < len(self.hash_workers) * 2:
            bottleneck = "[DB-LAG]"
        elif cpu_pct > 90:
            bottleneck = "[CPU-MAX]"
        elif mem_pct > 85:
            bottleneck = "[MEM-HIGH]"
        else:
            bottleneck = "[OK]"
            
        # Collect detailed metrics
        sys_metrics = self.collect_system_metrics()
        
        # Calculate worker efficiency
        hash_efficiency = self.calculate_worker_efficiency(self.hash_stats, 'hash')
        compress_efficiency = self.calculate_worker_efficiency(self.compress_stats, 'compress')
        
        # Database latency percentiles
        db_stats = self.calculate_db_stats()
        
        # Print compact stats
        if elapsed > 0:
            logger.info(
                f"[{elapsed:>4.0f}s] H:{len(self.hash_workers)} C:{len(self.compress_workers)} U:{len(self.upload_workers)} | "
                f"{hash_rate:>5.1f}f/s {throughput_mb:>5.1f}MB/s | "
                f"Done:{total_processed:>6,} Queue:{total_remaining:>7,} | "
                f"{bottleneck:>9}"
            )
            
            # Every 30 seconds, show detailed metrics
            if int(elapsed) % 30 == 0 and elapsed > 0:
                self.print_detailed_metrics(hash_efficiency, compress_efficiency, sys_metrics, db_stats)
        
    def emergency_upload(self, remaining_files: list):
        """Upload any files that didn't get uploaded during normal shutdown."""
        if not remaining_files:
            return
            
        staging_path = Path(STAGING_PATH)
        
        # Create manifest for rsync
        manifest_path = Path("/tmp/emergency_manifest.txt")
        rel_paths = [str(f.relative_to(staging_path)) for f in remaining_files]
        manifest_path.write_text('\n'.join(rel_paths))
        
        # Batch rsync
        try:
            result = subprocess.run([
                "rsync",
                "-av",
                "--files-from", str(manifest_path),
                "--relative",
                "--remove-source-files",  # Delete after upload
                "-e", f"ssh -p {SSH_PORT} -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=60",
                STAGING_PATH,
                f"{UPLOAD_HOST}:{UPLOAD_PATH}/"
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info(f"Emergency upload successful: {len(remaining_files)} files")
                
                # Mark files as uploaded in database
                conn = get_db_connection()
                try:
                    with conn.cursor() as cur:
                        for file_path in remaining_files:
                            # Extract original path from staging path structure
                            # e.g., staging/12/34/1234...json.lz4 -> extract from JSON
                            try:
                                with lz4.frame.open(file_path, 'rb') as f:
                                    metadata = json.load(f)
                                    original_path = metadata['pth']
                                    
                                cur.execute("""
                                    UPDATE work_queue
                                    SET uploaded_at = NOW()
                                    WHERE pth = %s AND uploaded_at IS NULL
                                """, (original_path,))
                            except Exception as e:
                                logger.error(f"Failed to process {file_path}: {e}")
                                continue
                        conn.commit()
                        logger.info(f"Updated database for {len(remaining_files)} emergency uploads")
                except Exception as e:
                    logger.error(f"Failed to update DB after emergency upload: {e}")
                    conn.rollback()
                finally:
                    return_db_connection(conn)
            else:
                logger.error(f"Emergency upload failed: {result.stderr}")
        except Exception as e:
            logger.error(f"Emergency upload error: {e}")
        finally:
            # Clean up manifest
            if manifest_path.exists():
                manifest_path.unlink()
    
    def shutdown(self):
        logger.info("\n" + "="*60)
        logger.info("Initiating graceful shutdown...")
        logger.info("="*60)
        
        # Collect final stats before shutdown from shared dicts
        total_processed = sum(s.get('files_processed', 0) for s in self.hash_stats)
        total_dedup = sum(s.get('dedup_hits', 0) for s in self.hash_stats)
        total_compressed = sum(s.get('files_compressed', 0) for s in self.compress_stats)
        total_bytes = sum(s.get('bytes_hashed', 0) for s in self.hash_stats)
        
        # Stop hash workers first (stop claiming new work)
        logger.info("Stopping hash workers...")
        for worker in self.hash_workers:
            worker.stop_flag.set()
            
        # Stop DB worker (let it finish pending operations)
        if self.db_worker:
            logger.info("Stopping DB worker...")
            self.db_worker.stop_flag.set()
            
        # Give hash workers time to finish current files
        time.sleep(2)
        
        # Stop compress workers (let them finish queue)
        logger.info("Stopping compress workers...")
        for worker in self.compress_workers:
            worker.stop_flag.set()
            
        # Stop upload workers last (let them upload remaining)
        logger.info("Stopping upload workers...")
        for worker in self.upload_workers:
            worker.stop_flag.set()
            
        # Wait for workers with progress updates
        logger.info("Waiting for workers to complete...")
        
        # Wait for hash and compress workers first
        for worker in self.hash_workers + self.compress_workers:
            worker.join(timeout=10)
            if worker.is_alive():
                logger.warning(f"Force terminating {worker.worker_id}")
                worker.terminate()
                worker.join(timeout=2)
                
        # Wait for DB worker
        if self.db_worker:
            self.db_worker.join(timeout=10)
            if self.db_worker.is_alive():
                logger.warning("Force terminating DB worker")
                self.db_worker.terminate()
                self.db_worker.join(timeout=2)
        
        # Give upload workers more time to finish
        logger.info("Waiting for upload workers to finish...")
        for worker in self.upload_workers:
            worker.join(timeout=30)  # More time for uploads
            if worker.is_alive():
                logger.warning(f"Force terminating {worker.worker_id}")
                worker.terminate()
                worker.join(timeout=2)
            
        # Check for any remaining staged files and upload them
        logger.info("Checking for remaining staged files...")
        remaining_files = list(Path(STAGING_PATH).glob("*/*/*"))
        if remaining_files:
            logger.info(f"Found {len(remaining_files)} un-uploaded files, performing final upload...")
            self.emergency_upload(remaining_files)
        else:
            logger.info("All files uploaded successfully")
            
        # Print final statistics
        elapsed = time.time() - self.metrics['start_time']
        logger.info("\n" + "="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        logger.info(f"Total runtime: {humanize.naturaldelta(elapsed)}")
        logger.info(f"Files processed: {total_processed:,}")
        logger.info(f"Dedup hits: {total_dedup:,} ({total_dedup/max(1,total_processed)*100:.1f}%)")
        logger.info(f"Files compressed: {total_compressed:,}")
        logger.info(f"Total data hashed: {humanize.naturalsize(total_bytes)}")
        logger.info(f"Processing rate: {total_processed/max(1,elapsed):.1f} files/sec")
        logger.info("="*60)
        
        # Close database pool
        if connection_pool:
            connection_pool.closeall()
                
        logger.info("Orchestrator shutdown complete")


def main():
    """Main entry point."""
    import argparse
    global verbose_mode
    
    parser = argparse.ArgumentParser(description="Auto-tuning blob processor")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    
    verbose_mode = args.verbose
    
    # Configure logging
    logger.remove()
    log_level = "DEBUG" if args.verbose else "INFO"
    
    if args.verbose:
        # Verbose mode: include module and line number
        logger.add(
            sys.stdout, 
            level=log_level,
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {name}:{line} | <level>{message}</level>"
        )
    else:
        # Normal mode: clean output
        logger.add(
            sys.stdout, 
            level=log_level,
            format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
        )
    
    logger.info("\n" + "="*60)
    logger.info("PBNAS AUTO-TUNING BLOB PROCESSOR")
    logger.info("Press Ctrl+C to gracefully shutdown")
    logger.info("="*60 + "\n")
    
    orchestrator = AutoTuningOrchestrator()
    
    try:
        orchestrator.start()
    except KeyboardInterrupt:
        logger.info("\nReceived keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if not shutdown_flag.is_set():
            shutdown_flag.set()
            orchestrator.shutdown()


if __name__ == "__main__":
    main()