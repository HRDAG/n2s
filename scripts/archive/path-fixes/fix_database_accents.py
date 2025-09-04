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
# scripts/fix_database_accents.py

"""
Fix database paths to match accented filenames on disk.

The problem:
- Database has paths WITHOUT accents (musica/Jose/Cancion.mp3)
- Filesystem has paths WITH accents (música/José/Canción.mp3)
- We need to UPDATE the database to store the correct accented paths

Strategy:
1. Get cantfind=true records from database
2. Search filesystem for files whose accent-stripped version matches the DB path
3. Update database with the actual accented path from filesystem
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional, List, Tuple

def remove_all_accents(text: str) -> str:
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

def find_accented_version_on_disk(db_path: str) -> Optional[Path]:
    """
    Given a non-accented path from DB, find the actual accented version on disk.
    
    Args:
        db_path: Path from database (likely without accents)
    
    Returns:
        Actual path on disk (with accents) if found, None otherwise
    """
    # Start with the full path
    base_path = Path('/Volumes')
    full_path = base_path / db_path
    
    # 1. Check if exact path exists (maybe it's already correct)
    if full_path.exists():
        return full_path
    
    # 2. Search for file where accent-stripped version matches our DB path
    # We need to search directory by directory, building up the path
    
    path_parts = Path(db_path).parts
    current_path = base_path
    actual_parts = []
    
    for part in path_parts:
        # Check if current path exists
        if not current_path.exists():
            return None
        
        # Look for a child that matches when accents are removed
        found = False
        try:
            for child in current_path.iterdir():
                if remove_all_accents(child.name) == part:
                    # Found a match!
                    current_path = child
                    actual_parts.append(child.name)
                    found = True
                    break
                # Also try case-insensitive match
                elif remove_all_accents(child.name.lower()) == part.lower():
                    current_path = child
                    actual_parts.append(child.name)
                    found = True
                    break
        except PermissionError:
            logger.warning(f"Permission denied: {current_path}")
            return None
        
        if not found:
            # This part doesn't exist, even with accents
            return None
    
    # We found the complete path
    return current_path

def scan_directory_for_accented_files(base_dir: Path, limit: int = 100) -> List[Tuple[str, str]]:
    """
    Scan a directory and create mapping of non-accented to accented paths.
    This helps us understand the pattern of what we're looking for.
    
    Returns:
        List of (non_accented_path, accented_path) tuples
    """
    mappings = []
    count = 0
    
    try:
        for item in base_dir.rglob('*'):
            if count >= limit:
                break
            
            # Check if path has any non-ASCII characters (likely accents)
            path_str = str(item.relative_to('/Volumes'))
            if any(ord(c) > 127 for c in path_str):
                non_accented = remove_all_accents(path_str)
                if non_accented != path_str:
                    mappings.append((non_accented, path_str))
                    count += 1
    except Exception as e:
        logger.error(f"Error scanning {base_dir}: {e}")
    
    return mappings

def main(limit: Optional[int] = None, dry_run: bool = True, scan_only: bool = False):
    """Main processing function."""
    
    if scan_only:
        # Just scan to understand the accent patterns
        logger.info("Scanning filesystem for accent patterns...")
        base_dirs = [
            Path('/Volumes/archives-2019'),
            Path('/Volumes/backup'),
        ]
        
        all_mappings = []
        for base_dir in base_dirs:
            if base_dir.exists():
                logger.info(f"Scanning {base_dir}...")
                mappings = scan_directory_for_accented_files(base_dir, limit=50)
                all_mappings.extend(mappings)
        
        logger.info(f"\nFound {len(all_mappings)} files with accents:")
        for non_acc, acc in all_mappings[:20]:
            logger.info(f"  DB would have: {non_acc}")
            logger.info(f"  Disk has:      {acc}")
            logger.info("")
        
        return
    
    logger.info("Starting database accent fix")
    logger.info("Finding files where DB has no accents but disk has accents")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records - these likely have accent issues
    query = """
        SELECT pth, st_size
        FROM fs 
        WHERE cantfind = true
        ORDER BY pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Checking {len(results)} cantfind records")
    
    stats = {
        'found_accented': 0,
        'already_correct': 0,
        'not_found': 0,
        'updated': 0
    }
    
    found_mappings = []
    
    for db_path, size in results:
        actual_path = find_accented_version_on_disk(db_path)
        
        if actual_path:
            actual_rel_path = str(actual_path.relative_to('/Volumes'))
            
            if actual_rel_path == db_path:
                stats['already_correct'] += 1
                logger.info(f"Already correct: {db_path}")
            else:
                stats['found_accented'] += 1
                found_mappings.append((db_path, actual_rel_path))
                logger.success(f"Found accented version!")
                logger.success(f"  DB has:   {db_path}")
                logger.success(f"  Disk has: {actual_rel_path}")
                
                if not dry_run:
                    # Update database with the correct accented path
                    cur.execute("""
                        UPDATE fs 
                        SET pth = %s, cantfind = false, last_found_at = NOW()
                        WHERE pth = %s
                    """, (actual_rel_path, db_path))
                    
                    stats['updated'] += 1
                    
                    if stats['updated'] % 10 == 0:
                        conn.commit()
                        logger.info(f"Committed {stats['updated']} updates")
        else:
            stats['not_found'] += 1
            # Only log first few not found to avoid spam
            if stats['not_found'] <= 10:
                logger.warning(f"Not found even with accents: {db_path}")
    
    if not dry_run:
        conn.commit()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info(f"Found with accents: {stats['found_accented']} files")
    logger.info(f"Already correct: {stats['already_correct']} files")
    logger.info(f"Still not found: {stats['not_found']} files")
    
    if not dry_run:
        logger.info(f"Updated in database: {stats['updated']} records")
    else:
        logger.info("DRY RUN - no database updates made")
        logger.info(f"Would update {stats['found_accented']} records to have accented paths")
    
    # Show some examples of what we found
    if found_mappings:
        logger.info("\nExample mappings found:")
        for db_path, disk_path in found_mappings[:5]:
            logger.info(f"\n  DB (no accents):  {db_path}")
            logger.info(f"  Disk (accented):  {disk_path}")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fix database to match accented paths on disk"
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
    parser.add_argument(
        "--scan", 
        action="store_true", 
        help="Just scan filesystem to show accent patterns"
    )
    
    args = parser.parse_args()
    
    if args.scan:
        main(scan_only=True)
    else:
        main(limit=args.limit, dry_run=not args.execute)