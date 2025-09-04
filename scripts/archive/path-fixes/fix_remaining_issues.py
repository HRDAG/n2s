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
# scripts/fix_remaining_issues.py

"""
Fix remaining path issues including:
1. Backslash escaping in filenames
2. xt's MacBook Pro apostrophe
3. Any other edge cases
"""

import unicodedata
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

def fix_path_variations(db_path: str) -> Optional[str]:
    """Try various fixes for path issues."""
    
    # Fix 1: Handle backslash-escaped quotes (e.g., "About \Convert\ Scripts" -> 'About "Convert" Scripts')
    if '\\' in db_path:
        # Replace backslash-escaped quotes with actual quotes
        fixed = db_path.replace('\\"', '"').replace('\\', '"')
        full_path = Path('/Volumes') / fixed
        if full_path.exists():
            return fixed
        
        # Also try just removing backslashes
        fixed = db_path.replace('\\', '')
        full_path = Path('/Volumes') / fixed
        if full_path.exists():
            return fixed
    
    # Fix 2: xt's MacBook Pro - try with curly apostrophe
    if "xt's MacBook Pro" in db_path:
        # The database has curly apostrophe but let's verify both ways
        straight_apostrophe = db_path.replace("xt's", "xt's")  # to straight
        curly_apostrophe = db_path.replace("xt's", "xt's")     # to curly
        
        for variant in [straight_apostrophe, curly_apostrophe]:
            if variant != db_path:
                full_path = Path('/Volumes') / variant
                if full_path.exists():
                    return variant
    
    # Fix 3: Try path as-is
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    return None

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing remaining path issues")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get all remaining cantfind records
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY 
            -- Prioritize non-dictionary files (those are legitimately missing)
            CASE 
                WHEN pth NOT LIKE '%InfoPlist.strings%' AND pth NOT LIKE '%Localizable.strings%'
                THEN 0 
                ELSE 1 
            END,
            pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Processing {len(results)} cantfind records")
    
    stats = {
        'found': 0, 
        'not_found': 0, 
        'updated': 0,
        'backslash_fixed': 0,
        'apostrophe_fixed': 0,
        'infoplist_missing': 0,
        'other_missing': 0
    }
    updates = []
    
    for (db_path,) in results:
        actual_path = fix_path_variations(db_path)
        
        if actual_path and actual_path != db_path:
            stats['found'] += 1
            updates.append((actual_path, db_path))
            
            # Track what type of fix
            if '\\' in db_path and '\\' not in actual_path:
                stats['backslash_fixed'] += 1
                logger.success(f"Fixed backslash escaping: {db_path[:80]}...")
            elif "xt's" in db_path or "xt's" in actual_path:
                stats['apostrophe_fixed'] += 1
                logger.success(f"Fixed apostrophe: {db_path[:80]}...")
            else:
                logger.success(f"Fixed: {db_path[:80]}...")
            
            # Batch updates
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
        elif actual_path == db_path:
            # Path exists as-is
            if not dry_run:
                cur.execute("""
                    UPDATE fs 
                    SET cantfind = false
                    WHERE pth = %s
                """, (db_path,))
            stats['found'] += 1
        else:
            stats['not_found'] += 1
            # Track what's missing
            if 'InfoPlist.strings' in db_path or 'Localizable.strings' in db_path:
                stats['infoplist_missing'] += 1
            else:
                stats['other_missing'] += 1
                if stats['other_missing'] <= 10:
                    logger.warning(f"Still missing: {db_path[:100]}...")
    
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
    logger.info("RESULTS:")
    logger.info(f"✓ Fixed: {stats['found']} files")
    if stats['backslash_fixed'] > 0:
        logger.info(f"  - Backslash escaping fixed: {stats['backslash_fixed']}")
    if stats['apostrophe_fixed'] > 0:
        logger.info(f"  - Apostrophe issues fixed: {stats['apostrophe_fixed']}")
    
    logger.info(f"✗ Still missing: {stats['not_found']} files")
    logger.info(f"  - InfoPlist.strings files (likely deleted): {stats['infoplist_missing']}")
    logger.info(f"  - Other files: {stats['other_missing']}")
    
    if not dry_run:
        logger.info(f"✓ Updated: {stats['updated']} records")
    else:
        logger.info(f"DRY RUN - Would update {stats['found']} records")
    
    # Success rate
    total = len(results)
    if total > 0:
        success_rate = stats['found'] / total * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
    
    # Final summary
    logger.info("")
    logger.info("FINAL SUMMARY:")
    logger.info(f"The {stats['infoplist_missing']} InfoPlist.strings files appear to be genuinely missing")
    logger.info("from the Time Machine backups (incomplete dictionary bundles).")
    if stats['other_missing'] > 0:
        logger.info(f"There are {stats['other_missing']} other files that couldn't be found.")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--execute", action="store_true")
    
    args = parser.parse_args()
    main(limit=args.limit, dry_run=not args.execute)