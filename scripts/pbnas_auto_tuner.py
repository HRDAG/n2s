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

# Shutdown flag
shutdown_flag = mp.Event()


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
    logger.debug("Initialized database connection pool")


def get_db_connection():
    """Get connection from pool."""
    if connection_pool is None:
        init_connection_pool()
    return connection_pool.getconn()


def return_db_connection(conn):
    """Return connection to pool."""
    if connection_pool:
        connection_pool.putconn(conn)


def claim_work(worker_id: str) -> Optional[str]:
    """Claim a file from work_queue."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE work_queue
                SET claimed_at = NOW(), claimed_by = %s
                WHERE pth = (
                    SELECT pth FROM work_queue TABLESAMPLE BERNOULLI(0.1)
                    WHERE claimed_at IS NULL
                    LIMIT 1
                )
                AND claimed_at IS NULL
                RETURNING pth
            """, (worker_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            if result:
                return result[0]
            return None
                
    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return None
    finally:
        return_db_connection(conn)


def check_blob_exists(blob_id: str) -> bool:
    """Check if blob already exists in database."""
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


def update_fs_table(path: str, blob_id: str, is_missing: bool = False):
    """Update fs table with blobid or missing status."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if is_missing:
                cur.execute("""
                    UPDATE fs 
                    SET last_missing_at = NOW()
                    WHERE pth = %s
                """, (path,))
            else:
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


class HashWorker(mp.Process):
    """Read files, hash them, check dedup, pass to compress if needed."""
    
    def __init__(self, worker_id: str, compress_queue: mp.Queue, thresholds: dict, stats: dict):
        super().__init__()
        self.worker_id = worker_id
        self.compress_queue = compress_queue
        self.thresholds = thresholds
        self.stop_flag = mp.Event()
        self.stats = stats  # Shared manager dict
        
    def run(self):
        """Main worker loop."""
        logger.info(f"HashWorker {self.worker_id} started")
        init_connection_pool()
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            # Claim work
            path = claim_work(self.worker_id)
            if not path:
                logger.debug(f"HashWorker {self.worker_id} no work available")
                time.sleep(1)
                continue
            
            logger.debug(f"HashWorker {self.worker_id} claimed: {path[:50]}...")
                
            try:
                self.process_file(path)
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                
        logger.info(f"HashWorker {self.worker_id} stopping...")
        self.cleanup()
        logger.info(f"HashWorker {self.worker_id} stopped")
        
    def process_file(self, path: str):
        """Process a single file."""
        file_path = Path("/Volumes") / path
        
        # Check existence
        if not file_path.exists():
            logger.debug(f"File not found: {path}")
            update_fs_table(path, None, is_missing=True)
            remove_from_queue(path)
            return
            
        if not file_path.is_file():
            logger.debug(f"Skipping non-file: {path}")
            remove_from_queue(path)
            return
            
        # Get file size
        size = file_path.stat().st_size
        self.stats['bytes_hashed'] = self.stats.get('bytes_hashed', 0) + size
        
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
        # Read file
        data = file_path.read_bytes()
        
        # Hash
        blob_id = blake3.blake3(data).hexdigest()
        
        # Check dedup
        if check_blob_exists(blob_id):
            update_fs_table(path, blob_id)
            remove_from_queue(path)
            self.stats['dedup_hits'] = self.stats.get('dedup_hits', 0) + 1
            return
            
        # Pass to compress via shared memory
        try:
            shm = shared_memory.SharedMemory(create=True, size=size)
            shm.buf[:size] = data
            
            self.compress_queue.put({
                'path': path,
                'blob_id': blob_id,
                'shm_name': shm.name,
                'size': size,
                'method': 'shared_memory'
            }, timeout=30)
            
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
        # Stream hash
        hasher = blake3.blake3()
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
        # Stream hash
        hasher = blake3.blake3()
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
        
    def run(self):
        """Main worker loop."""
        logger.info(f"CompressWorker {self.worker_id} started")
        init_connection_pool()
        self.active_shm = set()  # Track active shared memory segments
        
        while not self.stop_flag.is_set() and not shutdown_flag.is_set():
            try:
                item = self.compress_queue.get(timeout=1)
                self.process_item(item)
            except Empty:
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
            # Read from disk
            file_path = Path("/Volumes") / path
            if not file_path.exists():
                logger.warning(f"File disappeared: {path}")
                remove_from_queue(path)
                return
            data = file_path.read_bytes()
            
        elif method == 'stream':
            # Stream compress (TODO: implement streaming)
            file_path = Path("/Volumes") / path
            if not file_path.exists():
                logger.warning(f"File disappeared: {path}")
                remove_from_queue(path)
                return
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
        
        logger.trace(f"Staged blob: {blob_id[:16]}...")
        
    def cleanup(self):
        """Clean up any remaining shared memory segments."""
        for shm_name in self.active_shm:
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                shm.close()
                shm.unlink()
                logger.debug(f"Cleaned up shared memory: {shm_name}")
            except Exception:
                pass


class UploadWorker(mp.Process):
    """Batch upload staged blobs."""
    
    def __init__(self, worker_id: str, thresholds: dict):
        super().__init__()
        self.worker_id = worker_id
        self.thresholds = thresholds
        self.stop_flag = mp.Event()
        self.pending = []
        self.last_upload = time.time()
        
    def run(self):
        """Main worker loop."""
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
        """Collect staged files."""
        staging_path = Path(STAGING_PATH)
        
        for blob_path in staging_path.glob("*/*/*"):
            if blob_path.is_file():
                # Get relative path for rsync
                rel_path = blob_path.relative_to(staging_path)
                self.pending.append(str(rel_path))
                
                # Don't collect too many at once
                if len(self.pending) >= self.thresholds.get('batch_size', 100) * 2:
                    break
                    
    def upload_batch(self):
        """Upload batch of blobs."""
        if not self.pending:
            return
            
        start = time.time()
        
        # Create manifest for rsync
        manifest_path = Path(f"/tmp/manifest_{self.worker_id}.txt")
        manifest_path.write_text('\n'.join(self.pending))
        
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
            else:
                logger.error(f"Rsync failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("Rsync timeout")
        except Exception as e:
            logger.error(f"Upload error: {e}")
            
        self.pending.clear()
        self.last_upload = time.time()
        manifest_path.unlink(missing_ok=True)


class AutoTuningOrchestrator:
    """Main orchestrator with auto-tuning."""
    
    def __init__(self):
        # Create queues
        global compress_queue
        compress_queue = mp.Queue(maxsize=100)
        self.compress_queue = compress_queue
        
        # Worker pools
        self.hash_workers = []
        self.compress_workers = []
        self.upload_workers = []
        
        # Shared worker stats
        self.hash_stats = []
        self.compress_stats = []
        
        # Tunable thresholds
        self.manager = mp.Manager()
        self.thresholds = self.manager.dict({
            'shared_memory_max': 10_000_000,  # 10MB
            'reread_threshold': 50_000_000,   # 50MB
            'batch_size': 100,
            'batch_wait': 5.0,
        })
        
        # Performance metrics
        self.metrics = {
            'start_time': time.time(),
            'files_processed': 0,
            'dedup_hits': 0,
            'last_tune': time.time(),
        }
        
        # Tuning parameters
        self.tune_interval = 30  # seconds
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
        
        # Start initial workers
        self.spawn_hash_workers(2)
        self.spawn_compress_workers(2)
        self.spawn_upload_workers(1)
        
        # Main loop
        while not shutdown_flag.is_set():
            time.sleep(5)
            
            # Tune periodically
            if time.time() - self.metrics['last_tune'] > self.tune_interval:
                self.tune()
                self.metrics['last_tune'] = time.time()
                
            # Print stats
            self.print_stats()
            
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
            
        logger.info(f"Spawned {count} hash workers (total: {len(self.hash_workers)})")
        
    def spawn_compress_workers(self, count: int):
        """Spawn compress workers."""
        for i in range(count):
            worker_id = f"compress_{len(self.compress_workers)}"
            stats = self.manager.dict()  # Create shared dict for this worker
            worker = CompressWorker(worker_id, self.compress_queue, stats)
            worker.start()
            self.compress_workers.append(worker)
            self.compress_stats.append(stats)
            
        logger.info(f"Spawned {count} compress workers (total: {len(self.compress_workers)})")
        
    def spawn_upload_workers(self, count: int):
        """Spawn upload workers."""
        for i in range(count):
            worker_id = f"upload_{len(self.upload_workers)}"
            worker = UploadWorker(worker_id, self.thresholds)
            worker.start()
            self.upload_workers.append(worker)
            
        logger.info(f"Spawned {count} upload workers (total: {len(self.upload_workers)})")
        
    def tune(self):
        """Auto-tune worker counts and thresholds."""
        # Get metrics
        try:
            compress_queue_size = self.compress_queue.qsize()
        except NotImplementedError:
            # macOS doesn't support qsize, use approximation
            compress_queue_size = 50  # Assume medium load
        staged_files = len(list(Path(STAGING_PATH).glob("*/*/*")))
        
        logger.info(f"Tuning: compress_queue={compress_queue_size}, staged={staged_files}")
        
        # Tune hash workers
        if compress_queue_size < 10 and len(self.hash_workers) < self.max_workers:
            self.spawn_hash_workers(1)
        elif compress_queue_size > 80 and len(self.hash_workers) > self.min_workers:
            self.remove_hash_worker()
            
        # Tune compress workers
        if compress_queue_size > 50 and len(self.compress_workers) < self.max_workers:
            if psutil.cpu_percent(interval=1) < 70:
                self.spawn_compress_workers(1)
        elif compress_queue_size < 5 and len(self.compress_workers) > self.min_workers:
            self.remove_compress_worker()
            
        # Tune upload workers
        if staged_files > 1000 and len(self.upload_workers) < 3:
            self.spawn_upload_workers(1)
            self.thresholds['batch_size'] = min(500, int(self.thresholds['batch_size'] * 1.5))
        elif staged_files < 100 and len(self.upload_workers) > 1:
            self.remove_upload_worker()
            
        # Tune memory thresholds
        mem_available = psutil.virtual_memory().available
        if mem_available > 20_000_000_000:  # 20GB
            self.thresholds['shared_memory_max'] = min(100_000_000, 
                                                       int(self.thresholds['shared_memory_max'] * 1.2))
        elif mem_available < 5_000_000_000:  # 5GB
            self.thresholds['shared_memory_max'] = max(1_000_000,
                                                       int(self.thresholds['shared_memory_max'] * 0.8))
                                                       
    def remove_hash_worker(self):
        """Remove a hash worker."""
        if self.hash_workers:
            worker = self.hash_workers.pop(0)
            self.hash_stats.pop(0)
            worker.stop_flag.set()
            logger.info(f"Removing hash worker (remaining: {len(self.hash_workers)})")
            
    def remove_compress_worker(self):
        """Remove a compress worker."""
        if self.compress_workers:
            worker = self.compress_workers.pop(0)
            self.compress_stats.pop(0)
            worker.stop_flag.set()
            logger.info(f"Removing compress worker (remaining: {len(self.compress_workers)})")
            
    def remove_upload_worker(self):
        """Remove an upload worker."""
        if self.upload_workers:
            worker = self.upload_workers.pop(0)
            worker.stop_flag.set()
            logger.info(f"Removing upload worker (remaining: {len(self.upload_workers)})")
            
    def print_stats(self):
        """Print current statistics."""
        elapsed = time.time() - self.metrics['start_time']
        
        # Collect worker stats from shared dicts
        total_processed = sum(s.get('files_processed', 0) for s in self.hash_stats)
        total_dedup = sum(s.get('dedup_hits', 0) for s in self.hash_stats)
        total_compressed = sum(s.get('files_compressed', 0) for s in self.compress_stats)
        
        # qsize() doesn't work on macOS, use approximation
        try:
            queue_size = self.compress_queue.qsize()
        except NotImplementedError:
            queue_size = "?"
        
        logger.info(
            f"[{elapsed:.0f}s] Workers: H={len(self.hash_workers)} "
            f"C={len(self.compress_workers)} U={len(self.upload_workers)} | "
            f"Processed: {total_processed} | Dedup: {total_dedup} | "
            f"Compressed: {total_compressed} | Queue: {queue_size}"
        )
        
    def shutdown(self):
        """Shutdown all workers gracefully."""
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
        all_workers = self.hash_workers + self.compress_workers + self.upload_workers
        
        for i, worker in enumerate(all_workers):
            logger.debug(f"Waiting for {worker.worker_id}...")
            worker.join(timeout=10)
            if worker.is_alive():
                logger.warning(f"Force terminating {worker.worker_id}")
                worker.terminate()
                worker.join(timeout=2)
            
        # Clean up staging directory
        logger.info("Cleaning up staging directory...")
        staged_count = 0
        for blob_path in Path(STAGING_PATH).glob("*/*/*"):
            if blob_path.is_file():
                staged_count += 1
        
        if staged_count > 0:
            logger.warning(f"Found {staged_count} un-uploaded files in staging")
            
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
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    
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