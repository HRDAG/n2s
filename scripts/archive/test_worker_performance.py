#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "colorama",
#   "humanize",
# ]
# ///

"""
Test version of the blob worker to demonstrate claim performance.
Skips actual uploads to focus on claim timing.
"""

import time
import sys
from typing import Optional
import signal

import humanize
import psycopg2
from loguru import logger
from psycopg2 import pool

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"

# Pool configuration
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 10

# Create a global connection pool
connection_pool = None

# Track statistics
stats = {
    'claims': 0,
    'total_claim_time': 0.0,
    'min_claim_time': float('inf'),
    'max_claim_time': 0.0,
    'start_time': time.time(),
}

# Control flag
should_continue = True

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global should_continue
    logger.info(f"Received signal {signum}, shutting down...")
    should_continue = False


def setup_logging():
    """Configure loguru for console output."""
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True,
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


def claim_work_tablesample(worker_id: str) -> Optional[str]:
    """Claim using TABLESAMPLE - fast but may miss work as table shrinks."""
    claim_start = time.time()
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE work_queue
                SET claimed_at = NOW(), claimed_by = %s
                WHERE pth = (
                    SELECT pth
                    FROM work_queue TABLESAMPLE BERNOULLI(0.1)
                    WHERE claimed_at IS NULL
                    LIMIT 1
                )
                AND claimed_at IS NULL
                RETURNING pth
            """, (worker_id,))
            
            result = cur.fetchone()
            conn.commit()
            
            claim_time = time.time() - claim_start
            return (result[0] if result else None, claim_time)
                
    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return (None, time.time() - claim_start)
    finally:
        return_db_connection(conn)


def claim_work_offset(worker_id: str, offset: int) -> Optional[str]:
    """Claim using deterministic offset - consistent performance."""
    claim_start = time.time()
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE work_queue
                SET claimed_at = NOW(), claimed_by = %s
                WHERE pth = (
                    SELECT pth
                    FROM work_queue
                    WHERE claimed_at IS NULL
                    ORDER BY pth
                    OFFSET %s
                    LIMIT 1
                )
                AND claimed_at IS NULL
                RETURNING pth
            """, (worker_id, offset))
            
            result = cur.fetchone()
            conn.commit()
            
            claim_time = time.time() - claim_start
            return (result[0] if result else None, claim_time)
                
    except psycopg2.Error as e:
        logger.error(f"Failed to claim work: {e}")
        conn.rollback()
        return (None, time.time() - claim_start)
    finally:
        return_db_connection(conn)


def release_claim(pth: str):
    """Release a claimed file (simulate processing completion)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM work_queue WHERE pth = %s", (pth,))
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Failed to release claim: {e}")
        conn.rollback()
    finally:
        return_db_connection(conn)


def test_claim_performance(method: str = "tablesample", duration: int = 30):
    """Test claim performance for specified duration."""
    logger.info(f"Testing {method} claim method for {duration} seconds...")
    
    worker_id = f"test_worker_{method}"
    test_start = time.time()
    claims = []
    empty_claims = 0
    
    # For offset method, use random starting points
    import random
    offset = random.randint(0, 10000)
    
    while (time.time() - test_start) < duration and should_continue:
        if method == "tablesample":
            result, claim_time = claim_work_tablesample(worker_id)
        else:
            result, claim_time = claim_work_offset(worker_id, offset)
            offset = (offset + random.randint(100, 1000)) % 100000
        
        claims.append(claim_time * 1000)  # Convert to ms
        
        if result:
            # Simulate quick processing
            time.sleep(0.01)
            release_claim(result)
        else:
            empty_claims += 1
            if empty_claims > 10:
                logger.warning("Too many empty claims, may be out of work")
                break
    
    # Calculate statistics
    if claims:
        avg_claim = sum(claims) / len(claims)
        min_claim = min(claims)
        max_claim = max(claims)
        p50 = sorted(claims)[len(claims)//2]
        p95 = sorted(claims)[int(len(claims)*0.95)] if len(claims) > 20 else max_claim
        
        logger.info(f"\n{method.upper()} Method Results:")
        logger.info(f"  Claims: {len(claims)}")
        logger.info(f"  Avg: {avg_claim:.1f}ms")
        logger.info(f"  Min: {min_claim:.1f}ms")
        logger.info(f"  Max: {max_claim:.1f}ms")
        logger.info(f"  P50: {p50:.1f}ms")
        logger.info(f"  P95: {p95:.1f}ms")
        logger.info(f"  Empty: {empty_claims}")
        
        return {
            'method': method,
            'claims': len(claims),
            'avg_ms': avg_claim,
            'min_ms': min_claim,
            'max_ms': max_claim,
            'p50_ms': p50,
            'p95_ms': p95,
            'empty': empty_claims
        }
    else:
        logger.warning(f"No successful claims with {method} method")
        return None


def get_queue_stats():
    """Get current queue statistics."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE claimed_at IS NULL) as unclaimed,
                    COUNT(*) FILTER (WHERE claimed_at IS NOT NULL) as claimed
                FROM work_queue
            """)
            stats = cur.fetchone()
            return stats
    finally:
        return_db_connection(conn)


def main():
    """Main test runner."""
    setup_logging()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize connection pool
    init_connection_pool()
    
    logger.info("=== Work Queue Claim Performance Test ===")
    
    # Get initial stats
    total, unclaimed, claimed = get_queue_stats()
    logger.info(f"Queue status: {total:,} total, {unclaimed:,} unclaimed, {claimed:,} claimed")
    
    if unclaimed < 100:
        logger.warning("Not enough unclaimed work for meaningful test")
        return
    
    # Test both methods
    results = []
    
    # Test TABLESAMPLE method
    logger.info("\n--- Testing TABLESAMPLE method ---")
    result = test_claim_performance("tablesample", duration=10)
    if result:
        results.append(result)
    
    # Brief pause
    time.sleep(2)
    
    # Test OFFSET method
    logger.info("\n--- Testing OFFSET method ---")
    result = test_claim_performance("offset", duration=10)
    if result:
        results.append(result)
    
    # Compare results
    if len(results) == 2:
        logger.info("\n=== COMPARISON ===")
        logger.info(f"{'Method':<12} {'Avg(ms)':<10} {'P50(ms)':<10} {'P95(ms)':<10} {'Claims/sec':<12}")
        logger.info("-" * 54)
        for r in results:
            claims_per_sec = 1000.0 / r['avg_ms'] if r['avg_ms'] > 0 else 0
            logger.info(
                f"{r['method']:<12} "
                f"{r['avg_ms']:<10.1f} "
                f"{r['p50_ms']:<10.1f} "
                f"{r['p95_ms']:<10.1f} "
                f"{claims_per_sec:<12.1f}"
            )
    
    # Final stats
    total, unclaimed, claimed = get_queue_stats()
    logger.info(f"\nFinal queue: {total:,} total, {unclaimed:,} unclaimed, {claimed:,} claimed")
    
    # Clean up
    if connection_pool:
        connection_pool.closeall()
        logger.info("Closed all database connections")


if __name__ == "__main__":
    main()
