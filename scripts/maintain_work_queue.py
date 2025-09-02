#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "humanize",
# ]
# ///

"""
Maintenance script for the work queue table.

Key functions:
1. Reset stale claims (workers that died)
2. Add newly discovered files to queue
3. Remove completed files
4. Show queue statistics
"""

import sys
import time
import argparse
from datetime import datetime, timedelta

import psycopg2
import humanize
from loguru import logger

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"

# Default stale claim timeout (minutes)
DEFAULT_STALE_MINUTES = 30


def setup_logging(verbose=False):
    """Configure loguru for console output."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
        level="DEBUG" if verbose else "INFO",
    )


def get_connection():
    """Create database connection."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME}"
    return psycopg2.connect(conn_string)


def reset_stale_claims(conn, minutes=DEFAULT_STALE_MINUTES):
    """Reset claims older than specified minutes."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE work_queue 
            SET claimed_at = NULL, claimed_by = NULL
            WHERE claimed_at < NOW() - INTERVAL '%s minutes'
            RETURNING pth, claimed_by, claimed_at
        """, (minutes,))
        
        reset_rows = cur.fetchall()
        conn.commit()
        
        if reset_rows:
            logger.info(f"Reset {len(reset_rows)} stale claims (older than {minutes} minutes)")
            for pth, worker, claimed_at in reset_rows[:5]:  # Show first 5
                age = datetime.now(claimed_at.tzinfo) - claimed_at
                logger.debug(f"  Reset: {pth[:80]}... (claimed by {worker} {humanize.naturaldelta(age)} ago)")
            if len(reset_rows) > 5:
                logger.debug(f"  ... and {len(reset_rows) - 5} more")
        else:
            logger.info(f"No stale claims found (threshold: {minutes} minutes)")
        
        return len(reset_rows)


def add_new_files(conn, dry_run=False):
    """Add newly discovered files to the work queue."""
    with conn.cursor() as cur:
        # Find files that need processing but aren't in queue
        cur.execute("""
            SELECT COUNT(*)
            FROM fs
            WHERE main = true
              AND blobid IS NULL
              AND last_missing_at IS NULL
              AND pth NOT LIKE '%/'
              AND pth NOT LIKE '%/status'
              AND pth NOT LIKE '%/.git'
              AND pth NOT LIKE '%/.svn'
              AND NOT EXISTS (
                  SELECT 1 FROM work_queue wq WHERE wq.pth = fs.pth
              )
        """)
        
        new_count = cur.fetchone()[0]
        
        if new_count > 0:
            if dry_run:
                logger.info(f"Would add {new_count:,} new files to queue (dry run)")
            else:
                logger.info(f"Adding {new_count:,} new files to work queue...")
                cur.execute("""
                    INSERT INTO work_queue (pth)
                    SELECT pth 
                    FROM fs
                    WHERE main = true
                      AND blobid IS NULL
                      AND last_missing_at IS NULL
                      AND pth NOT LIKE '%/'
                      AND pth NOT LIKE '%/status'
                      AND pth NOT LIKE '%/.git'
                      AND pth NOT LIKE '%/.svn'
                      AND NOT EXISTS (
                          SELECT 1 FROM work_queue wq WHERE wq.pth = fs.pth
                      )
                    ON CONFLICT (pth) DO NOTHING
                """)
                
                added = cur.rowcount
                conn.commit()
                logger.info(f"Added {added:,} new files to queue")
        else:
            logger.info("No new files to add to queue")
        
        return new_count


def remove_completed_files(conn, dry_run=False):
    """Remove files from queue that have been processed."""
    with conn.cursor() as cur:
        # Find files in queue that have been processed
        cur.execute("""
            SELECT COUNT(*)
            FROM work_queue wq
            JOIN fs ON fs.pth = wq.pth
            WHERE fs.blobid IS NOT NULL
               OR fs.last_missing_at IS NOT NULL
        """)
        
        completed_count = cur.fetchone()[0]
        
        if completed_count > 0:
            if dry_run:
                logger.info(f"Would remove {completed_count:,} completed files from queue (dry run)")
            else:
                logger.info(f"Removing {completed_count:,} completed files from queue...")
                cur.execute("""
                    DELETE FROM work_queue
                    WHERE pth IN (
                        SELECT wq.pth
                        FROM work_queue wq
                        JOIN fs ON fs.pth = wq.pth
                        WHERE fs.blobid IS NOT NULL
                           OR fs.last_missing_at IS NOT NULL
                    )
                """)
                
                removed = cur.rowcount
                conn.commit()
                logger.info(f"Removed {removed:,} completed files from queue")
        else:
            logger.info("No completed files to remove from queue")
        
        return completed_count


def get_queue_stats(conn):
    """Get detailed queue statistics."""
    with conn.cursor() as cur:
        # Queue stats
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE claimed_at IS NULL) as unclaimed,
                COUNT(*) FILTER (WHERE claimed_at IS NOT NULL) as claimed,
                MIN(claimed_at) as oldest_claim,
                MAX(claimed_at) as newest_claim
            FROM work_queue
        """)
        queue_stats = cur.fetchone()
        
        # Worker stats
        cur.execute("""
            SELECT claimed_by, COUNT(*) as claims
            FROM work_queue
            WHERE claimed_at IS NOT NULL
            GROUP BY claimed_by
            ORDER BY claims DESC
        """)
        worker_stats = cur.fetchall()
        
        # Processing stats from fs table
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE blobid IS NOT NULL) as completed,
                COUNT(*) FILTER (WHERE blobid IS NULL AND last_missing_at IS NULL) as pending,
                COUNT(*) FILTER (WHERE last_missing_at IS NOT NULL) as missing
            FROM fs
            WHERE main = true
              AND pth NOT LIKE '%/'
              AND pth NOT LIKE '%/status'
              AND pth NOT LIKE '%/.git'
              AND pth NOT LIKE '%/.svn'
        """)
        fs_stats = cur.fetchone()
        
        return queue_stats, worker_stats, fs_stats


def print_stats(queue_stats, worker_stats, fs_stats):
    """Print formatted statistics."""
    total, unclaimed, claimed, oldest_claim, newest_claim = queue_stats
    completed, pending, missing = fs_stats
    
    logger.info(f"""
{'='*60}
Work Queue Statistics
{'='*60}
Queue Status:
  Total in queue:    {total:,}
  Unclaimed:         {unclaimed:,}
  Currently claimed: {claimed:,}
  
Overall Progress:
  Completed files:   {completed:,}
  Pending files:     {pending:,}
  Missing files:     {missing:,}
  
  Progress:          {completed / (completed + pending + missing) * 100:.1f}%
""")
    
    if claimed > 0 and oldest_claim:
        age = datetime.now(oldest_claim.tzinfo) - oldest_claim
        logger.info(f"  Oldest claim:      {humanize.naturaldelta(age)} ago")
    
    if worker_stats:
        logger.info("\nActive Workers:")
        for worker, claims in worker_stats[:5]:
            logger.info(f"  {worker}: {claims} claims")
        if len(worker_stats) > 5:
            logger.info(f"  ... and {len(worker_stats) - 5} more workers")
    
    logger.info("=" * 60)


def vacuum_queue(conn):
    """Run VACUUM ANALYZE on work_queue table."""
    logger.info("Running VACUUM ANALYZE on work_queue...")
    old_isolation = conn.isolation_level
    conn.set_isolation_level(0)  # VACUUM requires autocommit
    
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE work_queue")
    
    conn.set_isolation_level(old_isolation)
    logger.info("Vacuum complete")


def continuous_maintenance(conn, interval_minutes=5, stale_minutes=30):
    """Run continuous maintenance loop."""
    logger.info(f"Starting continuous maintenance (interval: {interval_minutes} min)")
    
    while True:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Maintenance cycle at {datetime.now():%Y-%m-%d %H:%M:%S}")
            
            # Reset stale claims
            reset_stale_claims(conn, stale_minutes)
            
            # Add new files
            add_new_files(conn)
            
            # Remove completed files
            remove_completed_files(conn)
            
            # Show stats
            queue_stats, worker_stats, fs_stats = get_queue_stats(conn)
            print_stats(queue_stats, worker_stats, fs_stats)
            
            # Vacuum periodically (every 10 cycles)
            if hasattr(continuous_maintenance, 'cycle_count'):
                continuous_maintenance.cycle_count += 1
            else:
                continuous_maintenance.cycle_count = 1
            
            if continuous_maintenance.cycle_count % 10 == 0:
                vacuum_queue(conn)
            
            logger.info(f"Next maintenance in {interval_minutes} minutes...")
            time.sleep(interval_minutes * 60)
            
        except KeyboardInterrupt:
            logger.info("Stopping continuous maintenance...")
            break
        except Exception as e:
            logger.error(f"Maintenance error: {e}")
            logger.info(f"Retrying in {interval_minutes} minutes...")
            time.sleep(interval_minutes * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Maintain the work queue table")
    parser.add_argument("--reset-stale", action="store_true", 
                        help="Reset stale claims")
    parser.add_argument("--add-new", action="store_true", 
                        help="Add new files to queue")
    parser.add_argument("--remove-completed", action="store_true", 
                        help="Remove completed files from queue")
    parser.add_argument("--stats", action="store_true", 
                        help="Show queue statistics")
    parser.add_argument("--vacuum", action="store_true", 
                        help="Run VACUUM ANALYZE on work_queue")
    parser.add_argument("--all", action="store_true",
                        help="Run all maintenance tasks (reset-stale, add-new, remove-completed, vacuum, stats)")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuous maintenance loop")
    parser.add_argument("--stale-minutes", type=int, default=DEFAULT_STALE_MINUTES,
                        help=f"Minutes before claim is considered stale (default: {DEFAULT_STALE_MINUTES})")
    parser.add_argument("--interval", type=int, default=5,
                        help="Minutes between maintenance cycles in continuous mode (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be done without making changes")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    # If --all is specified, enable all maintenance tasks
    if args.all:
        args.reset_stale = True
        args.add_new = True
        args.remove_completed = True
        args.vacuum = True
        args.stats = True
    
    # If no specific action, default to showing stats
    if not any([args.reset_stale, args.add_new, args.remove_completed, 
                args.stats, args.vacuum, args.continuous, args.all]):
        args.stats = True
    
    conn = get_connection()
    
    try:
        if args.continuous:
            continuous_maintenance(conn, args.interval, args.stale_minutes)
        else:
            if args.all:
                logger.info(f"\n{'='*60}")
                logger.info("Running full maintenance cycle")
                logger.info(f"{'='*60}\n")
            
            # Execute maintenance tasks in logical order
            if args.reset_stale:
                logger.info("Step 1: Resetting stale claims...")
                reset_stale_claims(conn, args.stale_minutes)
                if args.all:
                    logger.info("")
            
            if args.add_new:
                logger.info("Step 2: Adding new files to queue...")
                add_new_files(conn, args.dry_run)
                if args.all:
                    logger.info("")
            
            if args.remove_completed:
                logger.info("Step 3: Removing completed files...")
                remove_completed_files(conn, args.dry_run)
                if args.all:
                    logger.info("")
            
            if args.vacuum:
                logger.info("Step 4: Optimizing queue table...")
                vacuum_queue(conn)
                if args.all:
                    logger.info("")
            
            if args.stats:
                if args.all:
                    logger.info("Step 5: Final statistics...")
                queue_stats, worker_stats, fs_stats = get_queue_stats(conn)
                print_stats(queue_stats, worker_stats, fs_stats)
                
    finally:
        conn.close()


if __name__ == "__main__":
    main()
