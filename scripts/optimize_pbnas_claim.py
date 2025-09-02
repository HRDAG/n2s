#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "typer",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.02
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/optimize_pbnas_claim.py

"""
Optimize database for pbnas_blob_worker claim query performance.

This script:
1. Checks table statistics and bloat
2. Creates optimal indexes for the claim query
3. Runs VACUUM ANALYZE to clean up and update statistics
4. Tests query performance before and after

All operations are non-destructive and can be safely run on production.
"""

import psycopg2
import sys
import time
from typing import Optional, Tuple
import typer

app = typer.Typer()

# Configuration
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


def get_connection():
    """Create database connection."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} connect_timeout=10"
    return psycopg2.connect(conn_string)


def check_table_stats(conn) -> dict:
    """Check table statistics and bloat."""
    with conn.cursor() as cur:
        # Get table size and tuple counts
        cur.execute("""
            SELECT 
                pg_size_pretty(pg_relation_size('fs')) as table_size,
                pg_size_pretty(pg_total_relation_size('fs')) as total_size,
                n_dead_tup as dead_tuples,
                n_live_tup as live_tuples,
                n_mod_since_analyze as mods_since_analyze,
                round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_percentage,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE relname = 'fs';
        """)
        
        result = cur.fetchone()
        
        # Count eligible files for claim
        cur.execute("""
            SELECT COUNT(*) 
            FROM fs
            WHERE main = true
              AND blobid IS NULL
              AND last_missing_at IS NULL
              AND processing_started IS NULL
              AND pth NOT LIKE '%/'
              AND pth NOT LIKE '%/status'
              AND pth NOT LIKE '%/.git'
              AND pth NOT LIKE '%/.svn';
        """)
        eligible_count = cur.fetchone()[0]
        
        return {
            'table_size': result[0],
            'total_size': result[1],
            'dead_tuples': result[2],
            'live_tuples': result[3],
            'mods_since_analyze': result[4],
            'dead_percentage': result[5] or 0,
            'last_vacuum': result[6],
            'last_autovacuum': result[7],
            'last_analyze': result[8],
            'last_autoanalyze': result[9],
            'eligible_files': eligible_count
        }


def print_stats(stats: dict, label: str = "Current"):
    """Pretty print table statistics."""
    print(f"\n{label} Table Statistics:")
    print("=" * 60)
    print(f"  Table size (data only): {stats['table_size']}")
    print(f"  Total size (with indexes): {stats['total_size']}")
    print(f"  Live tuples: {stats['live_tuples']:,}")
    print(f"  Dead tuples: {stats['dead_tuples']:,}")
    print(f"  Dead percentage: {stats['dead_percentage']:.2f}%")
    print(f"  Modifications since analyze: {stats['mods_since_analyze']:,}")
    print(f"  Eligible files for processing: {stats['eligible_files']:,}")
    print(f"\nMaintenance history:")
    print(f"  Last manual vacuum: {stats['last_vacuum'] or 'Never'}")
    print(f"  Last auto vacuum: {stats['last_autovacuum'] or 'Never'}")
    print(f"  Last manual analyze: {stats['last_analyze'] or 'Never'}")
    print(f"  Last auto analyze: {stats['last_autoanalyze'] or 'Never'}")
    
    if stats['dead_percentage'] > 20:
        print("\n⚠️  WARNING: High dead tuple percentage! VACUUM recommended.")
    if stats['mods_since_analyze'] > 100000:
        print("⚠️  WARNING: Many modifications since last ANALYZE! Statistics may be stale.")


def test_claim_query(conn, limit: int = 2000) -> Tuple[float, Optional[str]]:
    """Test the claim query performance and return timing."""
    with conn.cursor() as cur:
        start = time.time()
        
        cur.execute(f"""
            WITH candidates AS (
              SELECT pth
              FROM fs
              WHERE main = true
                AND blobid IS NULL
                AND last_missing_at IS NULL
                AND processing_started IS NULL
                AND pth NOT LIKE '%/'
                AND pth NOT LIKE '%/status'
                AND pth NOT LIKE '%/.git'
                AND pth NOT LIKE '%/.svn'
              LIMIT {limit}
            )
            SELECT pth FROM candidates
            ORDER BY RANDOM()
            LIMIT 1;
        """)
        
        result = cur.fetchone()
        elapsed = time.time() - start
        
        return elapsed, result[0] if result else None


def create_indexes(conn, dry_run: bool = False):
    """Create optimal indexes for claim query."""
    indexes = [
        {
            'name': 'idx_fs_claim_candidates',
            'definition': """
                CREATE INDEX CONCURRENTLY idx_fs_claim_candidates
                ON fs(pth)
                WHERE main = true 
                  AND blobid IS NULL 
                  AND last_missing_at IS NULL 
                  AND processing_started IS NULL
                  AND pth NOT LIKE '%/'
                  AND pth NOT LIKE '%/status'
                  AND pth NOT LIKE '%/.git%'
                  AND pth NOT LIKE '%/.svn%'
            """,
            'description': 'Partial index for claim query candidates'
        },
        {
            'name': 'idx_fs_processing_started',
            'definition': """
                CREATE INDEX CONCURRENTLY idx_fs_processing_started
                ON fs(processing_started)
                WHERE processing_started IS NOT NULL
            """,
            'description': 'Index for stale processing cleanup'
        }
    ]
    
    with conn.cursor() as cur:
        for idx in indexes:
            # Check if index exists
            cur.execute("""
                SELECT 1 FROM pg_indexes 
                WHERE schemaname = 'public' 
                  AND tablename = 'fs' 
                  AND indexname = %s
            """, (idx['name'],))
            
            if cur.fetchone():
                print(f"✓ Index {idx['name']} already exists")
            else:
                print(f"\nCreating index: {idx['description']}")
                print(f"  Name: {idx['name']}")
                
                if dry_run:
                    print("  [DRY RUN - would execute]:")
                    print(f"  {idx['definition']}")
                else:
                    print("  Creating (this may take a while)...")
                    try:
                        cur.execute(idx['definition'])
                        conn.commit()
                        print(f"  ✓ Index {idx['name']} created successfully")
                    except psycopg2.Error as e:
                        print(f"  ✗ Failed to create index: {e}")
                        conn.rollback()


def run_vacuum_analyze(conn, full: bool = False):
    """Run VACUUM ANALYZE on fs table."""
    old_isolation = conn.isolation_level
    conn.set_isolation_level(0)  # AUTOCOMMIT mode required for VACUUM
    
    try:
        with conn.cursor() as cur:
            if full:
                print("\nRunning VACUUM FULL ANALYZE on fs table...")
                print("⚠️  This will lock the table and may take a long time!")
                cur.execute("VACUUM FULL ANALYZE fs;")
            else:
                print("\nRunning VACUUM ANALYZE on fs table...")
                cur.execute("VACUUM ANALYZE fs;")
            
            print("✓ VACUUM ANALYZE complete!")
    finally:
        conn.set_isolation_level(old_isolation)


@app.command()
def main(
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be done without making changes"),
    skip_vacuum: bool = typer.Option(False, "--skip-vacuum", help="Skip VACUUM ANALYZE"),
    full_vacuum: bool = typer.Option(False, "--full-vacuum", help="Use VACUUM FULL (locks table!)"),
    test_only: bool = typer.Option(False, "--test-only", help="Only test current performance")
):
    """
    Optimize PostgreSQL database for pbnas_blob_worker claim query performance.
    
    Safe to run on production - all operations are non-destructive.
    """
    print("PostgreSQL Claim Query Optimizer for pbnas_blob_worker")
    print("=" * 60)
    
    conn = get_connection()
    
    try:
        # Get initial stats
        print("\nChecking current table state...")
        initial_stats = check_table_stats(conn)
        print_stats(initial_stats, "Initial")
        
        # Test current performance
        print("\n\nTesting current claim query performance...")
        print("-" * 60)
        
        # Test with current limit (2000)
        time_2000, path_2000 = test_claim_query(conn, 2000)
        print(f"  With LIMIT 2000: {time_2000*1000:.2f}ms")
        if path_2000:
            print(f"    Sample path: ...{path_2000[-50:]}")
        
        # Test with proposed limit (100)
        time_100, path_100 = test_claim_query(conn, 100)
        print(f"  With LIMIT 100:  {time_100*1000:.2f}ms")
        if path_100:
            print(f"    Sample path: ...{path_100[-50:]}")
        
        speedup = time_2000 / time_100 if time_100 > 0 else 0
        print(f"\n  Potential speedup from reducing LIMIT: {speedup:.1f}x")
        
        if test_only:
            print("\nTest complete (--test-only mode)")
            return
        
        # Create indexes
        print("\n\nIndex Optimization")
        print("-" * 60)
        create_indexes(conn, dry_run)
        
        # Run VACUUM ANALYZE
        if not skip_vacuum and not dry_run:
            run_vacuum_analyze(conn, full_vacuum)
            
            # Get post-vacuum stats
            print("\nChecking table state after maintenance...")
            final_stats = check_table_stats(conn)
            print_stats(final_stats, "After Maintenance")
            
            # Test performance again
            print("\n\nTesting claim query performance after optimization...")
            print("-" * 60)
            time_after, _ = test_claim_query(conn, 2000)
            print(f"  With LIMIT 2000: {time_after*1000:.2f}ms")
            
            time_after_100, _ = test_claim_query(conn, 100)
            print(f"  With LIMIT 100:  {time_after_100*1000:.2f}ms")
            
            print(f"\nImprovement:")
            print(f"  LIMIT 2000: {time_2000*1000:.2f}ms → {time_after*1000:.2f}ms ({time_2000/time_after:.1f}x faster)")
            print(f"  LIMIT 100:  {time_100*1000:.2f}ms → {time_after_100*1000:.2f}ms ({time_100/time_after_100:.1f}x faster)")
        
        # Recommendations
        print("\n\nRECOMMENDATIONS")
        print("=" * 60)
        print("1. Update pbnas_blob_worker.py to use LIMIT 100 instead of 2000")
        print("   This alone should give you ~10x speedup")
        print("\n2. Schedule regular VACUUM ANALYZE (weekly or daily)")
        print("   Add to crontab: 0 3 * * 0 psql -h snowball -d pbnas -c 'VACUUM ANALYZE fs;'")
        print("\n3. Monitor dead tuple percentage")
        print("   If consistently >20%, consider more frequent autovacuum")
        
        if initial_stats['eligible_files'] < 1000:
            print(f"\n⚠️  Only {initial_stats['eligible_files']} files left to process!")
            print("   Claim queries may slow down as the pool shrinks.")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    app()
