#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "blake3",
#   "lz4",
#   "humanize",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025-09-03
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/pbnas_orchestrator.py

"""
Orchestrator for parallel blob processing with specialized worker pools.

Architecture:
- Reader workers: Handle USB I/O (limited to prevent thrashing)
- Processor workers: Blake3 hashing + LZ4 compression (CPU bound)
- Uploader workers: Transfer blobs to snowball (network bound)
- DB worker: Single worker for all database operations (prevents contention)
"""

import multiprocessing as mp
import os
import queue
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import humanize
import psycopg2
import psycopg2.pool
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

UPLOAD_HOST = "snowball"
UPLOAD_PATH = "/n2s/block_storage"
SSH_PORT = "2222"

# Worker limits
MAX_READERS_PER_DRIVE = 2  # Allow 2 concurrent USB readers with semaphore
MAX_PROCESSORS = mp.cpu_count() * 2  # Allow oversubscription
MAX_UPLOADERS = 10  # Limit SSH connections
MAX_QUEUE_SIZE = 2000  # Larger queue for better throughput

# Performance tuning
BATCH_SIZE_DB = 50  # Smaller batches for faster feedback
BATCH_SIZE_SMALL_FILES = 50  # Batch small file processing
LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10MB - use shared memory
SMALL_FILE_THRESHOLD = 100 * 1024  # 100KB - batch process

@dataclass
class WorkItem:
    """Work item passed between workers."""
    path: str
    size: int
    blob_id: Optional[str] = None
    data: Optional[bytes] = None  # For small files
    shm_name: Optional[str] = None  # For large files via shared memory
    error: Optional[str] = None
    start_time: float = 0.0
    read_time: float = 0.0
    process_time: float = 0.0
    upload_time: float = 0.0


class ReaderWorker(mp.Process):
    """Reads files from USB drives with controlled access."""
    
    def __init__(self, work_queue: mp.Queue, output_queue: mp.Queue, 
                 usb_semaphore: mp.Semaphore, worker_id: int):
        super().__init__()
        self.work_queue = work_queue
        self.output_queue = output_queue
        self.usb_semaphore = usb_semaphore
        self.worker_id = worker_id
        self.running = mp.Event()
        self.running.set()
        
    def run(self):
        """Main reader loop."""
        logger.info(f"Reader-{self.worker_id} started")
        
        while self.running.is_set():
            try:
                # Get work with short timeout to check running flag
                item = self.work_queue.get(timeout=0.5)
                if item is None:  # Poison pill
                    break
                    
                item.start_time = time.time()
                
                # Read file with USB semaphore protection
                with self.usb_semaphore:
                    read_start = time.time()
                    try:
                        file_path = Path("/Volumes") / Path(item.path)
                        
                        # Check if file exists and is regular file
                        if not file_path.exists():
                            item.error = "File not found"
                            self.output_queue.put(item)
                            continue
                            
                        if file_path.is_dir():
                            item.error = "Is a directory"
                            self.output_queue.put(item)
                            continue
                        
                        # Get actual size
                        item.size = file_path.stat().st_size
                        
                        # Read file based on size
                        if item.size < SMALL_FILE_THRESHOLD:
                            # Small file - read into memory
                            with open(file_path, 'rb') as f:
                                item.data = f.read()
                        elif item.size < LARGE_FILE_THRESHOLD:
                            # Medium file - read into memory
                            with open(file_path, 'rb') as f:
                                item.data = f.read()
                        else:
                            # Large file - use shared memory (TODO)
                            # For now, just read into memory
                            with open(file_path, 'rb') as f:
                                item.data = f.read()
                                
                        item.read_time = time.time() - read_start
                        
                    except PermissionError:
                        item.error = "Permission denied"
                    except Exception as e:
                        item.error = str(e)
                        
                # Pass to processor queue
                self.output_queue.put(item)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Reader-{self.worker_id} error: {e}")
                
        logger.info(f"Reader-{self.worker_id} stopped")
        
    def stop(self):
        """Signal worker to stop."""
        self.running.clear()


class ProcessorWorker(mp.Process):
    """Handles blake3 hashing and lz4 compression."""
    
    def __init__(self, input_queue: mp.Queue, output_queue: mp.Queue, worker_id: int):
        super().__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.worker_id = worker_id
        self.running = mp.Event()
        self.running.set()
        
    def run(self):
        """Main processor loop."""
        import blake3
        import lz4.frame
        import base64
        import json
        
        logger.info(f"Processor-{self.worker_id} started")
        
        while self.running.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                if item is None:  # Poison pill
                    break
                    
                if item.error:
                    # Pass through errors
                    self.output_queue.put(item)
                    continue
                    
                process_start = time.time()
                
                # Create blob
                hasher = blake3.blake3()
                frames = []
                
                # Process in chunks
                CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks
                offset = 0
                
                while offset < len(item.data):
                    chunk = item.data[offset:offset + CHUNK_SIZE]
                    hasher.update(chunk)
                    
                    # Compress chunk
                    compressed = lz4.frame.compress(chunk)
                    b64_frame = base64.b64encode(compressed).decode('ascii')
                    frames.append(b64_frame)
                    
                    offset += CHUNK_SIZE
                
                # Generate blob ID
                item.blob_id = hasher.hexdigest()
                
                # Create blob JSON
                blob_content = {
                    "content": {
                        "encoding": "lz4-multiframe",
                        "frames": frames
                    },
                    "metadata": {
                        "size": item.size,
                        "mtime": time.time(),
                        "filetype": "unknown",  # Could add magic detection
                        "encryption": False
                    }
                }
                
                # Write blob to temp file
                blob_path = f"/tmp/{item.blob_id}"
                with open(blob_path, 'w') as f:
                    json.dump(blob_content, f)
                
                # Clear data from memory
                item.data = None
                item.process_time = time.time() - process_start
                
                # Pass to uploader
                self.output_queue.put(item)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Processor-{self.worker_id} error: {e}")
                if 'item' in locals():
                    item.error = f"Processing failed: {e}"
                    self.output_queue.put(item)
                    
        logger.info(f"Processor-{self.worker_id} stopped")
        
    def stop(self):
        """Signal worker to stop."""
        self.running.clear()


class UploaderWorker(mp.Process):
    """Handles blob uploads to snowball."""
    
    def __init__(self, input_queue: mp.Queue, db_queue: mp.Queue, worker_id: int):
        super().__init__()
        self.input_queue = input_queue
        self.db_queue = db_queue
        self.worker_id = worker_id
        self.running = mp.Event()
        self.running.set()
        
    def run(self):
        """Main uploader loop."""
        import subprocess
        import os
        
        logger.info(f"Uploader-{self.worker_id} started")
        
        while self.running.is_set():
            try:
                item = self.input_queue.get(timeout=0.5)
                if item is None:  # Poison pill
                    break
                    
                if item.error:
                    # Pass through to DB for error recording
                    self.db_queue.put(item)
                    continue
                    
                upload_start = time.time()
                
                # Check if blob already exists
                AA = item.blob_id[0:2]
                BB = item.blob_id[2:4]
                remote_path = f"{UPLOAD_HOST}:{UPLOAD_PATH}/{AA}/{BB}/{item.blob_id}"
                
                # Setup SSH options with connection multiplexing
                pid = os.getpid()
                control_path = f"/tmp/ssh-mux-{pid}-%r@%h:%p"
                ssh_opts = f"ssh -p {SSH_PORT} -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=60 -o ControlMaster=auto -o ControlPath={control_path} -o ControlPersist=600"
                
                # Check existence
                check_result = subprocess.run(
                    ["ssh", "-p", SSH_PORT, UPLOAD_HOST, f"test -f {UPLOAD_PATH}/{AA}/{BB}/{item.blob_id} && echo EXISTS"],
                    capture_output=True, text=True
                )
                
                if "EXISTS" in check_result.stdout:
                    # Don't log - too verbose
                    item.upload_time = 0.0
                else:
                    # Upload blob (directories already exist)
                    blob_path = f"/tmp/{item.blob_id}"
                    
                    # Upload file with proper SSH options
                    result = subprocess.run(
                        ["rsync", "-W", "--no-perms", "--no-owner", "--no-group", "--no-times",
                         "-e", ssh_opts, blob_path, remote_path],
                        capture_output=True, text=True, timeout=300
                    )
                    
                    if result.returncode != 0:
                        item.error = f"Upload failed: {result.stderr}"
                    else:
                        item.upload_time = time.time() - upload_start
                        # Don't log individual uploads - batch logging handles it
                
                # Clean up temp file
                try:
                    os.unlink(f"/tmp/{item.blob_id}")
                except:
                    pass
                    
                # Send to DB worker
                self.db_queue.put(item)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Uploader-{self.worker_id} error: {e}")
                if 'item' in locals():
                    item.error = f"Upload failed: {e}"
                    self.db_queue.put(item)
                    
        logger.info(f"Uploader-{self.worker_id} stopped")
        
    def stop(self):
        """Signal worker to stop."""
        self.running.clear()


class DatabaseWorker(mp.Process):
    """Single worker for all database operations."""
    
    def __init__(self, db_queue: mp.Queue, stats_dict):
        super().__init__()
        self.db_queue = db_queue
        self.stats = stats_dict
        self.running = mp.Event()
        self.running.set()
        self.batch = []
        
    def run(self):
        """Main database loop."""
        logger.info("Database worker started")
        
        # Create connection with timezone
        conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} options='-c timezone=America/Los_Angeles'"
        conn = psycopg2.connect(conn_string)
        conn.autocommit = False
        
        last_batch_time = time.time()
        
        while self.running.is_set():
            try:
                # Get items with timeout for batching
                try:
                    item = self.db_queue.get(timeout=0.2)
                    if item is None:  # Poison pill
                        break
                    self.batch.append(item)
                except queue.Empty:
                    pass
                
                # Process batch if full or timeout
                if len(self.batch) >= BATCH_SIZE_DB or \
                   (len(self.batch) > 0 and time.time() - last_batch_time > 5.0):
                    self._process_batch(conn)
                    last_batch_time = time.time()
                    
            except Exception as e:
                logger.error(f"Database worker error: {e}")
                conn.rollback()
                
        # Process remaining batch
        if self.batch:
            self._process_batch(conn)
            
        conn.close()
        logger.info("Database worker stopped")
        
    def _process_batch(self, conn):
        """Process a batch of items."""
        if not self.batch:
            return
            
        try:
            with conn.cursor() as cur:
                for item in self.batch:
                    if item.error:
                        # Handle errors
                        if "not found" in item.error.lower():
                            cur.execute(
                                "UPDATE fs SET is_missing = TRUE WHERE pth = %s",
                                (item.path,)
                            )
                        # Remove from queue
                        cur.execute(
                            "DELETE FROM work_queue WHERE pth = %s",
                            (item.path,)
                        )
                    else:
                        # Update fs table (column is 'blobid' not 'blob_id')
                        cur.execute(
                            "UPDATE fs SET blobid = %s, uploaded = NOW() WHERE pth = %s",
                            (item.blob_id, item.path)
                        )
                        # Remove from queue
                        cur.execute(
                            "DELETE FROM work_queue WHERE pth = %s",
                            (item.path,)
                        )
                        
            conn.commit()
            
            # Compact batch logging - count success/fail
            success = sum(1 for item in self.batch if not item.error)
            failed = len(self.batch) - success
            
            # Update global stats
            self.stats['files_completed'] += success
            self.stats['files_failed'] += failed
            
            # Calculate average times
            if success > 0:
                avg_read = sum(item.read_time for item in self.batch if not item.error) / success
                avg_proc = sum(item.process_time for item in self.batch if not item.error) / success
                avg_upload = sum(item.upload_time for item in self.batch if not item.error) / success
                logger.info(f"Batch: {success} ok, {failed} fail | r:{avg_read:.1f} p:{avg_proc:.1f} u:{avg_upload:.1f}s")
            elif failed > 0:
                logger.warning(f"Batch: {failed} failed")
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            conn.rollback()
        finally:
            self.batch.clear()
            
    def stop(self):
        """Signal worker to stop."""
        self.running.clear()


class Orchestrator:
    """Main orchestrator controlling all worker pools."""
    
    def __init__(self):
        self.manager = mp.Manager()
        
        # Queues
        self.work_queue = mp.Queue(maxsize=MAX_QUEUE_SIZE)
        self.read_queue = mp.Queue(maxsize=MAX_QUEUE_SIZE)
        self.process_queue = mp.Queue(maxsize=MAX_QUEUE_SIZE)
        self.db_queue = mp.Queue(maxsize=MAX_QUEUE_SIZE)
        
        # USB access control - allow 2 concurrent readers
        self.usb_semaphore = mp.Semaphore(MAX_READERS_PER_DRIVE)
        
        # Worker pools
        self.readers: List[ReaderWorker] = []
        self.processors: List[ProcessorWorker] = []
        self.uploaders: List[UploaderWorker] = []
        self.db_worker: Optional[DatabaseWorker] = None
        
        # Statistics
        self.stats = self.manager.dict({
            'start_time': time.time(),
            'files_queued': 0,
            'files_completed': 0,
            'files_failed': 0,
            'bytes_processed': 0
        })
        
        # Control
        self.running = True
        self.shutdown_signal = None
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        # Don't log from signal handler - causes deadlock with loguru
        self.running = False
        self.shutdown_signal = signum
        
    def start_workers(self, num_readers=2, num_processors=8, num_uploaders=4):
        """Start all worker pools."""
        
        # Start readers
        for i in range(num_readers):
            worker = ReaderWorker(self.work_queue, self.read_queue, 
                                 self.usb_semaphore, i)
            worker.start()
            self.readers.append(worker)
            
        # Start processors
        for i in range(num_processors):
            worker = ProcessorWorker(self.read_queue, self.process_queue, i)
            worker.start()
            self.processors.append(worker)
            
        # Start uploaders
        for i in range(num_uploaders):
            worker = UploaderWorker(self.process_queue, self.db_queue, i)
            worker.start()
            self.uploaders.append(worker)
            
        # Start single DB worker with stats access
        self.db_worker = DatabaseWorker(self.db_queue, self.stats)
        self.db_worker.start()
        
        logger.info(f"Started {num_readers} readers, {num_processors} processors, "
                   f"{num_uploaders} uploaders, 1 DB worker")
        
    def stop_workers(self):
        """Stop all workers gracefully."""
        import time
        logger.info("Stopping all workers...")
        
        # Signal workers to stop
        for worker in self.readers:
            worker.stop()
        for worker in self.processors:
            worker.stop()
        for worker in self.uploaders:
            worker.stop()
        if self.db_worker:
            self.db_worker.stop()
        
        # Send poison pills
        for _ in self.readers:
            try:
                self.work_queue.put(None, timeout=0.1)
            except:
                pass
        for _ in self.processors:
            try:
                self.read_queue.put(None, timeout=0.1)
            except:
                pass
        for _ in self.uploaders:
            try:
                self.process_queue.put(None, timeout=0.1)
            except:
                pass
        try:
            self.db_queue.put(None, timeout=0.1)
        except:
            pass
        
        # Wait briefly then force terminate
        time.sleep(0.5)  # Give workers a moment to see the stop signal
        
        # Terminate all workers immediately
        for worker in self.readers:
            if worker.is_alive():
                worker.terminate()
        for worker in self.processors:
            if worker.is_alive():
                worker.terminate()
        for worker in self.uploaders:
            if worker.is_alive():
                worker.terminate()
        if self.db_worker and self.db_worker.is_alive():
            self.db_worker.terminate()
            
        # Wait for termination
        for worker in self.readers:
            worker.join(timeout=0.5)
        for worker in self.processors:
            worker.join(timeout=0.5)
        for worker in self.uploaders:
            worker.join(timeout=0.5)
        if self.db_worker:
            self.db_worker.join(timeout=0.5)
            
        logger.info("All workers stopped")
        
    def queue_work(self):
        """Queue work from database."""
        conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME}"
        conn = psycopg2.connect(conn_string)
        
        with conn.cursor() as cur:
            # Claim a batch of work
            worker_id = f"orchestrator_{os.getpid()}"
            cur.execute("""
                UPDATE work_queue 
                SET claimed_by = %s, claimed_at = NOW()
                WHERE pth IN (
                    SELECT pth FROM work_queue 
                    WHERE claimed_at IS NULL 
                    LIMIT 2000
                )
                RETURNING pth
            """, (worker_id,))
            
            work_items = cur.fetchall()
            conn.commit()
            
        for (path,) in work_items:
            item = WorkItem(path=path, size=0)
            self.work_queue.put(item)
            self.stats['files_queued'] += 1
            
        conn.close()
        return len(work_items)
        
    def monitor(self):
        """Monitor and display statistics."""
        last_stats_time = time.time()
        
        while self.running:
            # Check for shutdown signal
            if self.shutdown_signal:
                logger.info(f"Received signal {self.shutdown_signal}, shutting down...")
                break
                
            time.sleep(1)  # Shorter sleep for more responsive shutdown
            
            # Only print stats every 5 seconds
            if time.time() - last_stats_time < 5:
                continue
            last_stats_time = time.time()
            
            # Get queue sizes (qsize not available on macOS, so catch exception)
            try:
                work_q = self.work_queue.qsize()
                read_q = self.read_queue.qsize()
                process_q = self.process_queue.qsize()
                db_q = self.db_queue.qsize()
            except NotImplementedError:
                # macOS doesn't support qsize, use approximation
                work_q = read_q = process_q = db_q = "?"
            
            # Calculate rates
            elapsed = time.time() - self.stats['start_time']
            rate = self.stats['files_completed'] / elapsed if elapsed > 0 else 0
            
            # Check if workers are alive
            alive_readers = sum(1 for w in self.readers if w.is_alive())
            alive_processors = sum(1 for w in self.processors if w.is_alive())
            alive_uploaders = sum(1 for w in self.uploaders if w.is_alive())
            db_alive = 1 if self.db_worker and self.db_worker.is_alive() else 0
            
            # Compact status line with more info
            logger.info(
                f"Q[{work_q}/{read_q}/{process_q}/{db_q}] "
                f"Done:{self.stats['files_completed']}/{self.stats['files_queued']} "
                f"Workers[R{alive_readers}/P{alive_processors}/U{alive_uploaders}/D{db_alive}] "
                f"Rate:{rate:.1f}f/s"
            )
            
            # Queue more work if needed
            # Be aggressive about keeping the pipeline full
            should_queue = False
            pending = self.stats['files_queued'] - self.stats['files_completed'] - self.stats['files_failed']
            
            if isinstance(work_q, int):
                should_queue = work_q < 500  # Keep more in queue
            else:
                # macOS: queue more if pending work is low
                should_queue = pending < 500
                
            if should_queue:
                queued = self.queue_work()
                if queued > 0:
                    logger.info(f"Queued {queued} more files")
                    
    def run(self):
        """Main orchestrator loop."""
        logger.info("=== PBNAS Orchestrator Starting ===")
        
        # Start workers with reasonable counts for macOS
        # Reduced to avoid file descriptor limits
        self.start_workers(
            num_readers=2,  # Minimal USB readers
            num_processors=8,  # Fixed count, not based on CPU
            num_uploaders=4  # Reasonable network workers
        )
        
        # Queue initial work
        queued = self.queue_work()
        logger.info(f"Initial queue: {queued} files")
        
        # Monitor until shutdown
        self.monitor()
        
        # Cleanup
        self.stop_workers()
        logger.info("=== PBNAS Orchestrator Stopped ===")


def main():
    """Main entry point."""
    # Setup logging
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="INFO"
    )
    
    # Run orchestrator
    orchestrator = Orchestrator()
    orchestrator.run()


if __name__ == "__main__":
    main()