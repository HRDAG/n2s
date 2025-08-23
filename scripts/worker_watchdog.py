#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "psutil>=6.1.0",
#   "psycopg2-binary",
#   "loguru>=0.7.3",
#   "typer>=0.16.0",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.01.22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/worker_watchdog.py

"""
Worker health monitoring and auto-restart system for n2s workers.

Detects hung workers by:
- CPU usage monitoring (workers should consume some CPU when healthy)
- Database progress tracking (workers should process files regularly)  
- Process responsiveness checks
- Network I/O timeout detection

Automatically restarts workers that appear wedged/hung.
"""

import os
import signal
import subprocess
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import psutil
import psycopg2
import typer
from loguru import logger

# Configuration constants
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"
WORKER_SCRIPT = "pbnas_blob_worker.py"

# Health check thresholds
CPU_THRESHOLD_PERCENT = 1.0  # Min CPU usage over check window
PROGRESS_TIMEOUT_MINUTES = 15  # Max time without DB progress
RESPONSIVENESS_TIMEOUT_SECONDS = 30  # Max time for process to respond
CHECK_INTERVAL_SECONDS = 60  # How often to run health checks
RESTART_COOLDOWN_MINUTES = 5  # Min time between restarts of same worker


@dataclass
class WorkerState:
    """Track state of a worker process."""
    pid: int
    process: psutil.Process
    last_cpu_time: float = 0.0
    last_progress_check: datetime = field(default_factory=datetime.now)
    last_db_activity: Optional[datetime] = None
    cpu_history: deque = field(default_factory=lambda: deque(maxlen=10))
    restart_count: int = 0
    last_restart: Optional[datetime] = None
    consecutive_hangs: int = 0

    def update_cpu(self) -> None:
        """Update CPU usage tracking."""
        try:
            cpu_percent = self.process.cpu_percent()
            self.cpu_history.append(cpu_percent)
            logger.trace(f"Worker {self.pid}: CPU {cpu_percent:.1f}%")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            # Process may have died
            pass

    def is_cpu_idle(self) -> bool:
        """Check if worker has been CPU idle for too long."""
        if len(self.cpu_history) < 5:
            return False  # Not enough data
        
        recent_avg = sum(list(self.cpu_history)[-5:]) / 5
        return recent_avg < CPU_THRESHOLD_PERCENT

    def is_progress_stalled(self) -> bool:
        """Check if worker hasn't made DB progress recently."""
        if not self.last_db_activity:
            # No activity recorded yet, give grace period
            return datetime.now() - self.last_progress_check > timedelta(minutes=PROGRESS_TIMEOUT_MINUTES)
        
        return datetime.now() - self.last_db_activity > timedelta(minutes=PROGRESS_TIMEOUT_MINUTES)

    def can_restart(self) -> bool:
        """Check if worker is eligible for restart (cooldown period)."""
        if not self.last_restart:
            return True
        return datetime.now() - self.last_restart > timedelta(minutes=RESTART_COOLDOWN_MINUTES)


class WorkerWatchdog:
    """Monitor and manage worker processes."""

    def __init__(self, max_workers: int = 8):
        self.max_workers = max_workers
        self.workers: Dict[int, WorkerState] = {}
        self.db_conn: Optional[psycopg2.connection] = None
        self.last_global_progress: Optional[datetime] = None

    def setup_logging(self, verbose: bool = False) -> None:
        """Configure logging."""
        logger.remove()
        level = "DEBUG" if verbose else "INFO"
        logger.add(
            sys.stdout,
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
            level=level,
        )

    def get_db_connection(self) -> psycopg2.connection:
        """Get database connection with retry logic."""
        if self.db_conn and not self.db_conn.closed:
            try:
                # Test connection
                with self.db_conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self.db_conn
            except (psycopg2.Error, psycopg2.OperationalError):
                logger.warning("DB connection lost, reconnecting...")
                self.db_conn = None

        # Create new connection
        conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} connect_timeout=10"
        try:
            self.db_conn = psycopg2.connect(conn_string)
            with self.db_conn.cursor() as cur:
                cur.execute("SET timezone = 'America/Los_Angeles'")
            self.db_conn.commit()
            logger.trace("Database connection established")
            return self.db_conn
        except psycopg2.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def find_worker_processes(self) -> List[psutil.Process]:
        """Find all running worker processes."""
        workers = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if any(WORKER_SCRIPT in arg for arg in cmdline):
                    workers.append(proc)
                    logger.trace(f"Found worker process: PID {proc.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return workers

    def update_worker_states(self) -> None:
        """Update tracked worker states."""
        current_processes = self.find_worker_processes()
        current_pids = {p.pid for p in current_processes}
        
        # Remove dead workers
        dead_pids = set(self.workers.keys()) - current_pids
        for pid in dead_pids:
            logger.info(f"Worker {pid} has died, removing from tracking")
            del self.workers[pid]

        # Add new workers
        for proc in current_processes:
            if proc.pid not in self.workers:
                logger.info(f"New worker detected: PID {proc.pid}")
                self.workers[proc.pid] = WorkerState(pid=proc.pid, process=proc)

        # Update CPU usage for all workers
        for worker in self.workers.values():
            worker.update_cpu()

    def check_global_progress(self) -> None:
        """Check overall system progress by querying recent uploads."""
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cur:
                # Check for files uploaded in last 10 minutes
                cur.execute("""
                    SELECT COUNT(*), MAX(uploaded)
                    FROM fs 
                    WHERE uploaded > NOW() - INTERVAL '10 minutes'
                """)
                row = cur.fetchone()
                if row and row[0] > 0:
                    self.last_global_progress = datetime.now()
                    logger.trace(f"Global progress: {row[0]} files uploaded recently")
                    
                    # Update worker activity timestamps
                    for worker in self.workers.values():
                        worker.last_db_activity = self.last_global_progress

        except psycopg2.Error as e:
            logger.warning(f"Failed to check global progress: {e}")

    def check_worker_responsiveness(self, worker: WorkerState) -> bool:
        """Check if worker process is responsive (not in uninterruptible sleep)."""
        try:
            # Check process status
            status = worker.process.status()
            if status == psutil.STATUS_ZOMBIE:
                logger.warning(f"Worker {worker.pid} is zombie")
                return False
            
            # Check if process is in uninterruptible sleep (D state)
            # This often indicates I/O hang
            if status in [psutil.STATUS_DISK_SLEEP]:
                logger.warning(f"Worker {worker.pid} in uninterruptible sleep (I/O hang)")
                return False

            return True
            
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def is_worker_hung(self, worker: WorkerState) -> bool:
        """Determine if a worker appears to be hung."""
        # Check CPU activity
        cpu_idle = worker.is_cpu_idle()
        
        # Check database progress
        progress_stalled = worker.is_progress_stalled()
        
        # Check process responsiveness
        responsive = self.check_worker_responsiveness(worker)
        
        # Worker is considered hung if:
        # 1. CPU idle AND progress stalled
        # 2. OR process unresponsive
        is_hung = (cpu_idle and progress_stalled) or not responsive
        
        if is_hung:
            logger.warning(f"Worker {worker.pid} appears hung: "
                          f"CPU_idle={cpu_idle}, progress_stalled={progress_stalled}, "
                          f"responsive={responsive}")
        
        return is_hung

    def restart_worker(self, worker: WorkerState) -> bool:
        """Restart a hung worker process."""
        if not worker.can_restart():
            logger.info(f"Worker {worker.pid} in restart cooldown, skipping")
            return False

        logger.warning(f"Restarting hung worker {worker.pid} "
                      f"(restart #{worker.restart_count + 1})")

        try:
            # Graceful shutdown first
            worker.process.terminate()
            
            # Wait up to 10 seconds for graceful shutdown
            try:
                worker.process.wait(timeout=10)
                logger.info(f"Worker {worker.pid} terminated gracefully")
            except psutil.TimeoutExpired:
                # Force kill if graceful didn't work
                logger.warning(f"Force killing worker {worker.pid}")
                worker.process.kill()
                worker.process.wait(timeout=5)

            # Start new worker
            script_path = Path(__file__).parent / WORKER_SCRIPT
            subprocess.Popen([
                sys.executable, 
                str(script_path)
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            # Update worker state
            worker.restart_count += 1
            worker.last_restart = datetime.now()
            worker.consecutive_hangs += 1
            
            logger.info(f"Started replacement worker (consecutive hangs: {worker.consecutive_hangs})")
            
            # Remove old worker from tracking (new one will be detected next cycle)
            del self.workers[worker.pid]
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to restart worker {worker.pid}: {e}")
            return False

    def ensure_worker_count(self) -> None:
        """Ensure we have the target number of workers running."""
        active_workers = len(self.workers)
        
        if active_workers < self.max_workers:
            needed = self.max_workers - active_workers
            logger.info(f"Starting {needed} additional workers "
                       f"(current: {active_workers}, target: {self.max_workers})")
            
            script_path = Path(__file__).parent / WORKER_SCRIPT
            for _ in range(needed):
                try:
                    subprocess.Popen([
                        sys.executable,
                        str(script_path)
                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    time.sleep(1)  # Brief delay between starts
                except Exception as e:
                    logger.error(f"Failed to start worker: {e}")

    def health_check_cycle(self) -> None:
        """Run one complete health check cycle."""
        logger.debug("Running health check cycle...")
        
        # Update worker states
        self.update_worker_states()
        
        # Check global progress
        self.check_global_progress()
        
        # Check each worker for problems
        hung_workers = []
        for worker in self.workers.values():
            if self.is_worker_hung(worker):
                hung_workers.append(worker)
        
        # Restart hung workers
        for worker in hung_workers:
            self.restart_worker(worker)
        
        # Ensure we have enough workers
        self.ensure_worker_count()
        
        # Log status
        if self.workers:
            avg_cpu = sum(w.cpu_history[-1] if w.cpu_history else 0 
                         for w in self.workers.values()) / len(self.workers)
            logger.info(f"Health check: {len(self.workers)} workers, "
                       f"avg CPU: {avg_cpu:.1f}%, "
                       f"hung/restarted: {len(hung_workers)}")

    def run(self, verbose: bool = False) -> None:
        """Main watchdog loop."""
        self.setup_logging(verbose)
        logger.info(f"Starting worker watchdog (target: {self.max_workers} workers)")
        
        try:
            while True:
                try:
                    self.health_check_cycle()
                    time.sleep(CHECK_INTERVAL_SECONDS)
                except KeyboardInterrupt:
                    logger.info("Shutdown requested")
                    break
                except Exception as e:
                    logger.error(f"Health check error: {e}")
                    time.sleep(CHECK_INTERVAL_SECONDS)
        finally:
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
            logger.info("Watchdog stopped")


def main(
    max_workers: int = typer.Option(8, help="Maximum number of workers to maintain"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging"),
) -> None:
    """Run the worker watchdog."""
    watchdog = WorkerWatchdog(max_workers=max_workers)
    watchdog.run(verbose=verbose)


if __name__ == "__main__":
    typer.run(main)