#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/fix_timemachine_apostrophes.py

"""
Fix Time Machine backup paths with "xt's MacBook Pro" apostrophe issue.
These are deep nested paths that need special handling.
"""

from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def main(dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing Time Machine backup apostrophe issues")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get paths containing "xt's MacBook Pro" (with straight apostrophe in DB)
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        AND pth LIKE E'%xt\\'s MacBook Pro%'
        ORDER BY pth
    """
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Processing {len(results)} Time Machine backup records")
    
    stats = {'found': 0, 'not_found': 0, 'updated': 0}
    updates = []
    
    for (db_path,) in results:
        # Simple replacement: straight apostrophe to curly apostrophe
        # We know from checking that the disk has "xt's" with U+2019
        fixed_path = db_path.replace("xt's MacBook Pro", "xt's MacBook Pro")
        
        # Verify the path exists
        full_path = Path('/Volumes') / fixed_path
        
        if full_path.exists():
            stats['found'] += 1
            updates.append((fixed_path, db_path))
            logger.success(f"Fixed: {db_path[:100]}...")
            
            # Batch updates for efficiency
            if not dry_run and len(updates) >= 50:
                try:
                    cur.executemany("""
                        UPDATE fs 
                        SET pth = %s, cantfind = false
                        WHERE pth = %s
                    """, updates)
                    conn.commit()
                    stats['updated'] += len(updates)
                    logger.info(f"Updated {len(updates)} records")
                except psycopg2.Error as e:
                    logger.error(f"Update failed: {e}")
                    conn.rollback()
                updates = []
        else:
            stats['not_found'] += 1
            if stats['not_found'] <= 5:
                logger.warning(f"Still not found: {db_path[:100]}...")
    
    # Final update batch
    if not dry_run and updates:
        try:
            cur.executemany("""
                UPDATE fs 
                SET pth = %s, cantfind = false
                WHERE pth = %s
            """, updates)
            conn.commit()
            stats['updated'] += len(updates)
        except psycopg2.Error as e:
            logger.error(f"Final update failed: {e}")
            conn.rollback()
    
    logger.info("=" * 60)
    logger.info(f"✓ Fixed: {stats['found']} files")
    logger.info(f"✗ Not found: {stats['not_found']} files")
    if not dry_run:
        logger.info(f"✓ Updated: {stats['updated']} records")
    else:
        logger.info(f"DRY RUN - Would update {stats['found']} records")
    
    # Success rate
    total = len(results)
    if total > 0:
        success_rate = stats['found'] / total * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    
    args = parser.parse_args()
    main(dry_run=not args.execute)