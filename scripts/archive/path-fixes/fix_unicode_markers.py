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
# scripts/fix_unicode_markers.py

"""
Fix Unicode directional markers and other invisible characters in paths.

Common patterns:
- Phone numbers with U+202A (LEFT-TO-RIGHT EMBEDDING) and U+202C (POP DIRECTIONAL FORMATTING)
- Other invisible Unicode control characters
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional, Dict, Set
import re

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

# Unicode control characters that are often invisible
UNICODE_MARKERS = {
    '\u202A': 'LEFT-TO-RIGHT EMBEDDING',
    '\u202B': 'RIGHT-TO-LEFT EMBEDDING', 
    '\u202C': 'POP DIRECTIONAL FORMATTING',
    '\u202D': 'LEFT-TO-RIGHT OVERRIDE',
    '\u202E': 'RIGHT-TO-LEFT OVERRIDE',
    '\u200B': 'ZERO WIDTH SPACE',
    '\u200C': 'ZERO WIDTH NON-JOINER',
    '\u200D': 'ZERO WIDTH JOINER',
    '\u200E': 'LEFT-TO-RIGHT MARK',
    '\u200F': 'RIGHT-TO-LEFT MARK',
    '\uFEFF': 'ZERO WIDTH NO-BREAK SPACE',
}

def remove_unicode_markers(text: str) -> str:
    """Remove all Unicode directional and invisible markers."""
    result = text
    for marker in UNICODE_MARKERS:
        result = result.replace(marker, '')
    return result

def has_unicode_markers(text: str) -> bool:
    """Check if text contains any Unicode markers."""
    return any(marker in text for marker in UNICODE_MARKERS)

def find_with_unicode_variations(parent_dir: Path, target_name: str, cache: Dict[str, Set[str]]) -> Optional[str]:
    """
    Try to find file with Unicode marker variations.
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
    
    # 2. DB has no markers, disk has markers (common with phone numbers)
    # Try adding markers around phone number patterns
    phone_pattern = r'(\+?\d[\d\s\-\(\)]+)'
    if re.search(phone_pattern, target_name):
        # Check if any disk name without markers matches our target
        for disk_name in dir_contents:
            if remove_unicode_markers(disk_name) == target_name:
                logger.debug(f"Found with markers: {repr(disk_name)}")
                return disk_name
    
    # 3. DB has markers, disk doesn't (less common)
    if has_unicode_markers(target_name):
        target_no_markers = remove_unicode_markers(target_name)
        if target_no_markers in dir_contents:
            return target_no_markers
    
    # 4. Both have markers but different ones
    target_no_markers = remove_unicode_markers(target_name)
    for disk_name in dir_contents:
        if remove_unicode_markers(disk_name) == target_no_markers:
            return disk_name
    
    return None

def build_path_with_unicode_fixes(db_path: str) -> Optional[str]:
    """Build path fixing Unicode marker issues."""
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    cache = {}  # Cache directory contents
    
    for part in parts:
        actual_name = find_with_unicode_variations(current_path, part, cache)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            return None
    
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing Unicode directional marker issues")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records, prioritize those with phone numbers (Messages app)
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY 
            -- Prioritize paths with phone numbers (Messages app)
            CASE 
                WHEN pth ~ 'Messages.*\\+?[0-9][0-9\\s\\-\\(\\)]+' 
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
    
    stats = {'found': 0, 'not_found': 0, 'updated': 0, 'with_markers': 0}
    updates = []
    
    for (db_path,) in results:
        actual_path = build_path_with_unicode_fixes(db_path)
        
        if actual_path and actual_path != db_path:
            stats['found'] += 1
            updates.append((actual_path, db_path))
            
            # Check if we added Unicode markers
            if has_unicode_markers(actual_path) and not has_unicode_markers(db_path):
                stats['with_markers'] += 1
                logger.success(f"Fixed (added markers): {db_path}")
                logger.success(f"                    -> {repr(actual_path)}")
            elif has_unicode_markers(db_path) and not has_unicode_markers(actual_path):
                logger.success(f"Fixed (removed markers): {repr(db_path)}")
                logger.success(f"                      -> {actual_path}")
            else:
                logger.success(f"Fixed: {db_path}")
                logger.success(f"   -> {actual_path}")
            
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
    logger.info(f"  - With Unicode markers added: {stats['with_markers']}")
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