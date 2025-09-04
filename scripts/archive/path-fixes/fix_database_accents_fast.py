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
# Original date: 2025.09.03
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/fix_database_accents_fast.py

"""
Fast version - no filesystem scanning, just targeted lookups.

For each cantfind=true record:
1. Build path piece by piece
2. At each directory level, list contents and find accent match
3. Update DB when found
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional
import os

def remove_all_accents(text: str) -> str:
    """Remove ALL diacritics/accents from any character."""
    nfd = unicodedata.normalize('NFD', text)
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    return without_accents

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def find_accented_match(parent_dir: Path, target_name_no_accents: str) -> Optional[str]:
    """
    In a given directory, find a file/dir whose accent-stripped name matches target.
    
    This is FAST because it only lists one directory, not recursive.
    """
    if not parent_dir.exists():
        return None
    
    try:
        for item in parent_dir.iterdir():
            # Check if removing accents from this item matches our target
            if remove_all_accents(item.name) == target_name_no_accents:
                return item.name
            # Also try case-insensitive
            if remove_all_accents(item.name.lower()) == target_name_no_accents.lower():
                return item.name
    except (PermissionError, OSError) as e:
        logger.debug(f"Cannot read {parent_dir}: {e}")
    
    return None

def build_accented_path(db_path: str) -> Optional[str]:
    """
    Build the actual accented path by checking each directory level.
    This is MUCH faster than scanning the entire filesystem.
    
    Example:
    DB has: archives-2019/music/espanol/Jose/Cancion.mp3
    
    Process:
    1. Check /Volumes/archives-2019 - exists
    2. List /Volumes/archives-2019/, find item where remove_accents(name) == "music"
    3. List /Volumes/archives-2019/music/, find item where remove_accents(name) == "espanol"
       -> Found "español"
    4. Continue building path with actual names from disk
    
    Result: archives-2019/music/español/José/Canción.mp3
    """
    
    # Quick check - if exact path exists, we're done
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    # Build path piece by piece
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    
    for i, part in enumerate(parts):
        # Find the actual name (possibly accented) for this part
        actual_name = find_accented_match(current_path, part)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            # Couldn't find this part - path doesn't exist
            logger.debug(f"Part '{part}' not found in {current_path}")
            return None
    
    # Reconstruct the path
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function - FAST version."""
    
    logger.info("Fast database accent fix - no filesystem scanning")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Processing {len(results)} cantfind records")
    
    stats = {
        'found_accented': 0,
        'already_correct': 0,
        'not_found': 0,
        'updated': 0,
        'errors': 0
    }
    
    updates = []  # Batch updates for efficiency
    
    for (db_path,) in results:
        try:
            actual_path = build_accented_path(db_path)
            
            if actual_path:
                if actual_path == db_path:
                    stats['already_correct'] += 1
                    logger.debug(f"Already correct: {db_path}")
                else:
                    stats['found_accented'] += 1
                    updates.append((actual_path, db_path))
                    logger.success(f"Found accented version!")
                    logger.success(f"  DB:   {db_path}")
                    logger.success(f"  Disk: {actual_path}")
                    
                    # Batch updates every 50 records
                    if not dry_run and len(updates) >= 50:
                        try:
                            cur.executemany("""
                                UPDATE fs 
                                SET pth = %s, cantfind = false                                WHERE pth = %s
                            """, updates)
                            conn.commit()
                            stats['updated'] += len(updates)
                            logger.info(f"Batch updated {len(updates)} records (total: {stats['updated']})")
                        except psycopg2.Error as e:
                            logger.error(f"Batch update failed: {e}")
                            conn.rollback()
                            # Try individual updates
                            for new_path, old_path in updates:
                                try:
                                    cur.execute("""
                                        UPDATE fs 
                                        SET pth = %s, cantfind = false                                        WHERE pth = %s
                                    """, (new_path, old_path))
                                    conn.commit()
                                    stats['updated'] += 1
                                except psycopg2.Error as e2:
                                    logger.error(f"Individual update failed for {old_path}: {e2}")
                                    conn.rollback()
                        updates = []
            else:
                stats['not_found'] += 1
                if stats['not_found'] <= 5:  # Only show first few
                    logger.debug(f"Not found: {db_path}")
        
        except Exception as e:
            logger.error(f"Error processing {db_path}: {e}")
            stats['errors'] += 1
    
    # Final batch update
    if not dry_run and updates:
        try:
            cur.executemany("""
                UPDATE fs 
                SET pth = %s, cantfind = false                WHERE pth = %s
            """, updates)
            conn.commit()
            stats['updated'] += len(updates)
        except psycopg2.Error as e:
            logger.error(f"Final batch update failed: {e}")
            conn.rollback()
            # Try individual updates
            for new_path, old_path in updates:
                try:
                    cur.execute("""
                        UPDATE fs 
                        SET pth = %s, cantfind = false                        WHERE pth = %s
                    """, (new_path, old_path))
                    conn.commit()
                    stats['updated'] += 1
                except psycopg2.Error as e2:
                    logger.error(f"Individual update failed for {old_path}: {e2}")
                    conn.rollback()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info(f"✓ Found with accents: {stats['found_accented']} files")
    logger.info(f"✓ Already correct: {stats['already_correct']} files")
    logger.info(f"✗ Still not found: {stats['not_found']} files")
    if stats['errors']:
        logger.warning(f"⚠ Errors: {stats['errors']} files")
    
    if not dry_run:
        logger.info(f"\n✓ Updated database: {stats['updated']} records")
    else:
        logger.info(f"\nDRY RUN - Would update {stats['found_accented']} records")
        if stats['found_accented'] > 0:
            logger.info("Run with --execute to update database")
    
    # Show success rate
    total = len(results)
    if total > 0:
        success_rate = (stats['found_accented'] + stats['already_correct']) / total * 100
        logger.info(f"\nSuccess rate: {success_rate:.1f}%")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="FAST fix for database accent mismatches - no scanning"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Limit number of files to process"
    )
    parser.add_argument(
        "--execute", 
        action="store_true", 
        help="Actually update database (default is dry run)"
    )
    
    args = parser.parse_args()
    
    main(limit=args.limit, dry_run=not args.execute)