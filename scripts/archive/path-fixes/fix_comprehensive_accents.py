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
# scripts/fix_comprehensive_accents.py

"""
Comprehensive fix for accent/character mismatches.
Does CHARACTER-BY-CHARACTER accent removal, not word replacements.
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys

def remove_all_accents(text):
    """Remove ALL diacritics/accents from any character."""
    # NFD = Canonical Decomposition - separates base chars from combining marks
    nfd = unicodedata.normalize('NFD', text)
    # Remove all combining marks (Mn = Mark, nonspacing)
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    return without_accents

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host='henwen',
        database='n2s',
        user='pball',
        options="-c TimeZone=America/Los_Angeles"
    )

def find_file_on_disk(db_path):
    """Try to find the actual file on disk with various transformations."""
    full_path = Path('/Volumes') / db_path
    
    # 1. Try exact match
    if full_path.exists():
        return full_path, 'exact'
    
    # 2. Try removing ALL accents from entire path
    no_accent_path = remove_all_accents(db_path)
    full_no_accent = Path('/Volumes') / no_accent_path
    if full_no_accent.exists():
        return full_no_accent, 'no_accent_full'
    
    # 3. Try parent directory with accent-free filename
    parent = full_path.parent
    if parent.exists():
        target_name_no_accent = remove_all_accents(full_path.name)
        
        # List directory and compare accent-free versions
        try:
            for item in parent.iterdir():
                if remove_all_accents(item.name) == target_name_no_accent:
                    return item, 'no_accent_filename'
        except PermissionError:
            pass
    
    # 4. Try accent-free parent with original filename
    parent_no_accent = Path('/Volumes') / remove_all_accents(str(parent.relative_to('/Volumes')))
    if parent_no_accent.exists():
        test_path = parent_no_accent / full_path.name
        if test_path.exists():
            return test_path, 'no_accent_parent'
        
        # Also try accent-free filename in accent-free parent
        test_path2 = parent_no_accent / remove_all_accents(full_path.name)
        if test_path2.exists():
            return test_path2, 'no_accent_both'
    
    return None, None

def main(limit=None, dry_run=True):
    """Main processing function."""
    logger.info("Starting comprehensive accent fix")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get all cantfind files with non-ASCII characters
    query = """
        SELECT pth, st_size
        FROM fs 
        WHERE cantfind = true
        AND pth ~ '[^[:ascii:]]'
        ORDER BY pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Found {len(results)} files with non-ASCII characters")
    
    stats = {
        'exact': 0,
        'no_accent_full': 0,
        'no_accent_filename': 0,
        'no_accent_parent': 0,
        'no_accent_both': 0,
        'not_found': 0,
        'updated': 0
    }
    
    for db_path, size in results:
        actual_path, method = find_file_on_disk(db_path)
        
        if actual_path:
            stats[method] += 1
            logger.success(f"Found via {method}: {db_path}")
            
            if not dry_run:
                # Update database with the actual path
                actual_rel_path = str(actual_path.relative_to('/Volumes'))
                
                # Check if we need to update the path
                if actual_rel_path != db_path:
                    cur.execute("""
                        UPDATE fs 
                        SET pth = %s, cantfind = false, last_found_at = NOW()
                        WHERE pth = %s
                    """, (actual_rel_path, db_path))
                else:
                    # Just mark as found
                    cur.execute("""
                        UPDATE fs 
                        SET cantfind = false, last_found_at = NOW()
                        WHERE pth = %s
                    """, (db_path,))
                
                stats['updated'] += 1
                
                if stats['updated'] % 10 == 0:
                    conn.commit()
                    logger.info(f"Committed {stats['updated']} updates")
        else:
            stats['not_found'] += 1
            logger.warning(f"Still missing: {db_path}")
    
    if not dry_run:
        conn.commit()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    total_found = sum(v for k, v in stats.items() if k not in ['not_found', 'updated'])
    logger.info(f"Found: {total_found} files")
    logger.info(f"  - Exact match: {stats['exact']}")
    logger.info(f"  - Full path no accents: {stats['no_accent_full']}")
    logger.info(f"  - Filename no accents: {stats['no_accent_filename']}")  
    logger.info(f"  - Parent no accents: {stats['no_accent_parent']}")
    logger.info(f"  - Both no accents: {stats['no_accent_both']}")
    logger.info(f"Not found: {stats['not_found']}")
    
    if not dry_run:
        logger.info(f"Updated in database: {stats['updated']}")
    else:
        logger.info("DRY RUN - no database updates made")
        logger.info(f"Would update {total_found} records")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix accent mismatches comprehensively")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--execute", action="store_true", help="Actually update database (default is dry run)")
    
    args = parser.parse_args()
    
    main(limit=args.limit, dry_run=not args.execute)