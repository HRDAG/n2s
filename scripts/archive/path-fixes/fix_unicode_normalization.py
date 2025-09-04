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
# scripts/fix_unicode_normalization.py

"""
Fix Unicode normalization issues in Time Machine backups.

The problem:
- Database has composed Unicode (NFC): "française", "Española"
- Disk has decomposed Unicode (NFD): appears as "francÌ§aise", "EspanÌola"

Solution:
- Convert database paths to NFD (decomposed) form to match disk
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional, Dict, Set

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def to_nfd(text: str) -> str:
    """Convert text to NFD (decomposed) Unicode form."""
    return unicodedata.normalize('NFD', text)

def to_nfc(text: str) -> str:
    """Convert text to NFC (composed) Unicode form."""
    return unicodedata.normalize('NFC', text)

def find_with_normalization(parent_dir: Path, target_name: str, cache: Dict[str, Set[str]]) -> Optional[str]:
    """
    Try to find file with different Unicode normalizations.
    """
    if not parent_dir.exists():
        return None
    
    # Use cache if available
    cache_key = str(parent_dir)
    if cache_key in cache:
        dir_contents = cache[cache_key]
    else:
        try:
            dir_contents = {item.name for item in parent_dir.iterdir()}
            cache[cache_key] = dir_contents
        except (PermissionError, OSError):
            return None
    
    # 1. Exact match
    if target_name in dir_contents:
        return target_name
    
    # 2. Try NFD (decomposed) form - most common issue with Time Machine
    target_nfd = to_nfd(target_name)
    if target_nfd != target_name and target_nfd in dir_contents:
        logger.debug(f"Found with NFD: {target_name} -> {target_nfd}")
        return target_nfd
    
    # 3. Try NFC (composed) form - less common
    target_nfc = to_nfc(target_name)
    if target_nfc != target_name and target_nfc in dir_contents:
        logger.debug(f"Found with NFC: {target_name} -> {target_nfc}")
        return target_nfc
    
    # 4. Check if any file matches when both are normalized the same way
    # This handles cases where both have accents but in different forms
    for disk_name in dir_contents:
        # Compare both as NFC
        if to_nfc(disk_name) == to_nfc(target_name):
            return disk_name
        # Compare both as NFD
        if to_nfd(disk_name) == to_nfd(target_name):
            return disk_name
    
    return None

def build_path_with_normalization(db_path: str) -> Optional[str]:
    """Build path fixing Unicode normalization issues."""
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    cache = {}  # Cache directory contents
    
    for part in parts:
        # Special handling for the "xt's MacBook Pro" part
        if "MacBook Pro" in part and "xt" in part:
            # Try with the curly apostrophe
            part_with_curly = part.replace("'", "\u2019")
            actual_name = find_with_normalization(current_path, part_with_curly, cache)
            if not actual_name:
                actual_name = find_with_normalization(current_path, part, cache)
        else:
            actual_name = find_with_normalization(current_path, part, cache)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            return None
    
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing Unicode normalization issues (NFC/NFD)")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records, prioritize Time Machine backups with dictionary files
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY 
            -- Prioritize Time Machine dictionary files
            CASE 
                WHEN pth LIKE '%timemachine%Dictionary%' OR pth LIKE '%Backups.backupdb%Dictionary%'
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
    
    stats = {'found': 0, 'not_found': 0, 'updated': 0, 'normalization_fixed': 0}
    updates = []
    
    for (db_path,) in results:
        actual_path = build_path_with_normalization(db_path)
        
        if actual_path and actual_path != db_path:
            stats['found'] += 1
            updates.append((actual_path, db_path))
            
            # Check if this was a normalization issue
            if to_nfc(actual_path) == to_nfc(db_path) or to_nfd(actual_path) == to_nfd(db_path):
                stats['normalization_fixed'] += 1
                logger.success(f"Fixed normalization: {db_path[:80]}...")
                logger.success(f"                  -> {actual_path[:80]}...")
            else:
                logger.success(f"Fixed: {db_path[:80]}...")
                logger.success(f"   -> {actual_path[:80]}...")
            
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
        elif actual_path == db_path:
            # Path exists as-is, just mark as found
            if not dry_run:
                cur.execute("""
                    UPDATE fs 
                    SET cantfind = false
                    WHERE pth = %s
                """, (db_path,))
            stats['found'] += 1
        else:
            stats['not_found'] += 1
            if stats['not_found'] <= 5:
                logger.debug(f"Not found: {db_path[:100]}...")
    
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
    logger.info(f"  - Unicode normalization fixes: {stats['normalization_fixed']}")
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
    parser.add_argument("--limit", type=int)
    parser.add_argument("--execute", action="store_true")
    
    args = parser.parse_args()
    main(limit=args.limit, dry_run=not args.execute)