#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
#   "loguru",
#   "python-Levenshtein",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.03
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/fix_partial_accents.py

"""
Fix partial accent mismatches where DB has SOME accents but not all.

Examples:
- DB: "José Alfredo Jimenez" (has é but missing é in Jiménez)
- Disk: "José Alfredo Jiménez"

- DB: "Juan Ernesto Mendez Rodríguez" (has í but missing é in Méndez)
- Disk: "Juan Ernesto Méndez Rodríguez"

Strategy: For each directory, create accent mappings and try all combinations.
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
import Levenshtein
from typing import Optional, Dict, List, Tuple
import itertools

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

def generate_accent_variants(word: str) -> List[str]:
    """Generate all possible accent variants of a word."""
    # Map of base characters to their accented versions
    accent_map = {
        'a': ['a', 'á'],
        'e': ['e', 'é'],
        'i': ['i', 'í'],
        'o': ['o', 'ó'],
        'u': ['u', 'ú', 'ü'],
        'n': ['n', 'ñ'],
        'A': ['A', 'Á'],
        'E': ['E', 'É'],
        'I': ['I', 'Í'],
        'O': ['O', 'Ó'],
        'U': ['U', 'Ú', 'Ü'],
        'N': ['N', 'Ñ'],
    }
    
    # Build list of character options
    char_options = []
    for char in word:
        base_char = remove_all_accents(char)
        if base_char in accent_map:
            char_options.append(accent_map[base_char])
        else:
            char_options.append([char])
    
    # Generate all combinations
    variants = []
    for combo in itertools.product(*char_options):
        variants.append(''.join(combo))
    
    return variants

def find_best_match_in_directory(parent_dir: Path, target_name: str) -> Optional[Tuple[str, float]]:
    """
    Find the best matching file/dir in a directory using fuzzy matching.
    Returns (actual_name, similarity_score) or None.
    """
    if not parent_dir.exists():
        return None
    
    best_match = None
    best_score = 0.0
    
    try:
        # First try exact match
        for item in parent_dir.iterdir():
            if item.name == target_name:
                return (item.name, 1.0)
        
        # Generate variants of the target name
        # Split on common separators to handle names like "José Alfredo Jiménez"
        parts = target_name.replace('-', ' ').replace('_', ' ').split()
        
        # For each part, try with and without accents
        part_variants = []
        for part in parts:
            # Check if this part might need accent variations
            base = remove_all_accents(part)
            if base != part:
                # This part has some accents, try without
                part_variants.append([part, base])
            else:
                # This part has no accents, try common accent additions
                variants = generate_accent_variants(part)
                if len(variants) > 1:
                    part_variants.append(variants[:5])  # Limit to avoid explosion
                else:
                    part_variants.append([part])
        
        # Try combinations
        for combo in itertools.product(*part_variants):
            test_name = target_name
            for i, original_part in enumerate(parts):
                if original_part in test_name:
                    test_name = test_name.replace(original_part, combo[i], 1)
            
            # Check if this variant exists
            for item in parent_dir.iterdir():
                if item.name == test_name:
                    return (item.name, 0.95)
                
                # Also try Levenshtein distance for close matches
                similarity = 1.0 - (Levenshtein.distance(item.name, test_name) / max(len(item.name), len(test_name)))
                if similarity > best_score and similarity > 0.85:
                    best_match = item.name
                    best_score = similarity
    
    except (PermissionError, OSError) as e:
        logger.debug(f"Cannot read {parent_dir}: {e}")
        return None
    
    if best_match and best_score > 0.85:
        return (best_match, best_score)
    
    return None

def build_fuzzy_path(db_path: str) -> Optional[str]:
    """
    Build the actual path using fuzzy matching for partial accents.
    """
    # Quick check - if exact path exists, we're done
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    # Build path piece by piece with fuzzy matching
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    
    for part in parts:
        result = find_best_match_in_directory(current_path, part)
        
        if result:
            actual_name, score = result
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
            
            if score < 1.0:
                logger.debug(f"Fuzzy matched '{part}' -> '{actual_name}' (score: {score:.2f})")
        else:
            # Couldn't find this part
            logger.debug(f"Part '{part}' not found in {current_path}")
            return None
    
    # Reconstruct the path
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing partial accent mismatches")
    logger.info("Looking for files where DB has SOME accents but not all")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records that have at least one accent (partial accents likely)
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        AND pth ~ '[áéíóúñÁÉÍÓÚÑ]'
        ORDER BY pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Processing {len(results)} records with partial accents")
    
    stats = {
        'found_fuzzy': 0,
        'already_correct': 0,
        'not_found': 0,
        'updated': 0,
        'errors': 0
    }
    
    updates = []
    
    for (db_path,) in results:
        try:
            actual_path = build_fuzzy_path(db_path)
            
            if actual_path:
                if actual_path == db_path:
                    stats['already_correct'] += 1
                    logger.debug(f"Already correct: {db_path}")
                else:
                    stats['found_fuzzy'] += 1
                    updates.append((actual_path, db_path))
                    logger.success(f"Found fuzzy match!")
                    logger.success(f"  DB:   {db_path}")
                    logger.success(f"  Disk: {actual_path}")
                    
                    # Show the specific differences
                    db_parts = db_path.split('/')
                    actual_parts = actual_path.split('/')
                    for i, (db_part, actual_part) in enumerate(zip(db_parts, actual_parts)):
                        if db_part != actual_part:
                            logger.info(f"    Part {i}: '{db_part}' -> '{actual_part}'")
                    
                    # Batch updates
                    if not dry_run and len(updates) >= 25:
                        try:
                            cur.executemany("""
                                UPDATE fs 
                                SET pth = %s, cantfind = false
                                WHERE pth = %s
                            """, updates)
                            conn.commit()
                            stats['updated'] += len(updates)
                            logger.info(f"Batch updated {len(updates)} records (total: {stats['updated']})")
                        except psycopg2.Error as e:
                            logger.error(f"Batch update failed: {e}")
                            conn.rollback()
                        updates = []
            else:
                stats['not_found'] += 1
                if stats['not_found'] <= 5:
                    logger.debug(f"Not found: {db_path}")
        
        except Exception as e:
            logger.error(f"Error processing {db_path}: {e}")
            stats['errors'] += 1
    
    # Final batch update
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
            logger.error(f"Final batch update failed: {e}")
            conn.rollback()
    
    # Print summary
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info(f"✓ Found with fuzzy matching: {stats['found_fuzzy']} files")
    logger.info(f"✓ Already correct: {stats['already_correct']} files")
    logger.info(f"✗ Still not found: {stats['not_found']} files")
    if stats['errors']:
        logger.warning(f"⚠ Errors: {stats['errors']} files")
    
    if not dry_run:
        logger.info(f"\n✓ Updated database: {stats['updated']} records")
    else:
        logger.info(f"\nDRY RUN - Would update {stats['found_fuzzy']} records")
        if stats['found_fuzzy'] > 0:
            logger.info("Run with --execute to update database")
    
    # Show success rate
    total = len(results)
    if total > 0:
        success_rate = (stats['found_fuzzy'] + stats['already_correct']) / total * 100
        logger.info(f"\nSuccess rate: {success_rate:.1f}%")
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fix partial accent mismatches in database"
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