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
# scripts/fix_apostrophes_and_special_chars.py

"""
Fix missing apostrophes, curly quotes, and other special character issues.

Common patterns:
- DB: "Dont" -> Disk: "Don't" 
- DB: "Patricks" -> Disk: "Patrick's"
- DB: straight quotes -> Disk: curly quotes
- Various apostrophe types: ' ' ʼ
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

# Different types of apostrophes and quotes
APOSTROPHE_VARIANTS = [
    "'",      # U+0027 APOSTROPHE
    "\u2019", # U+2019 RIGHT SINGLE QUOTATION MARK (curly apostrophe)
    "\u2018", # U+2018 LEFT SINGLE QUOTATION MARK
    "\u02BC", # U+02BC MODIFIER LETTER APOSTROPHE
    "\u00B4", # U+00B4 ACUTE ACCENT
    "`",      # U+0060 GRAVE ACCENT
    "\u201B", # U+201B SINGLE HIGH-REVERSED-9 QUOTATION MARK
]
QUOTE_VARIANTS = ['"', "\u201C", "\u201D", "\u201E", "\u201F", "\u2033", "\u2036"]  # Various quote types

def normalize_quotes_apostrophes(text: str) -> str:
    """Normalize all quote and apostrophe variants to standard ASCII."""
    result = text
    for variant in APOSTROPHE_VARIANTS:
        result = result.replace(variant, "'")
    for variant in QUOTE_VARIANTS:
        result = result.replace(variant, '"')
    return result

def add_common_apostrophes(text: str) -> list[str]:
    """Add apostrophes to common contractions and possessives."""
    variations = [text]
    
    # Common contractions
    contractions = {
        r'\bDont\b': "Don't",
        r'\bdont\b': "don't",
        r'\bWont\b': "Won't",
        r'\bwont\b': "won't",
        r'\bCant\b': "Can't",
        r'\bcant\b': "can't",
        r'\bIts\b': "It's",  # when likely contraction
        r'\bYoure\b': "You're",
        r'\byoure\b': "you're",
        r'\bTheyre\b': "They're",
        r'\btheyre\b': "they're",
        r'\bWeve\b': "We've",
        r'\bweve\b': "we've",
        r'\bIve\b': "I've",
        r'\bYouve\b': "You've",
        r'\byouve\b': "you've",
        r'\bIll\b': "I'll",
        r'\bWell\b': "We'll",
        r'\bwell\b': "we'll",  # careful with word "well"
        r'\bShell\b': "She'll",
        r'\bHell\b': "He'll",
        r'\bItll\b': "It'll",
        r'\bTheyll\b': "They'll",
        r'\btheyll\b': "they'll",
        r'\bId\b': "I'd",
        r'\bWed\b': "We'd",
        r'\bYoud\b': "You'd",
        r'\byoud\b': "you'd",
        r'\bShed\b': "She'd",
        r'\bHed\b': "He'd",
        r'\bTheyd\b': "They'd",
        r'\btheyd\b': "they'd",
        r'\bIm\b': "I'm",
        r'\bWhats\b': "What's",
        r'\bwhats\b': "what's",
        r'\bThats\b': "That's",
        r'\bthats\b': "that's",
        r'\bHeres\b': "Here's",
        r'\bheres\b': "here's",
        r'\bTheres\b': "There's",
        r'\btheres\b': "there's",
        r'\bWheres\b': "Where's",
        r'\bwheres\b': "where's",
        r'\bHows\b': "How's",
        r'\bhows\b': "how's",
        r'\bLets\b': "Let's",
        r'\blets\b': "let's",
        r'\bWhos\b': "Who's",
        r'\bwhos\b': "who's",
        r'\bCouldnt\b': "Couldn't",
        r'\bcouldnt\b': "couldn't",
        r'\bWouldnt\b': "Wouldn't",
        r'\bwouldnt\b': "wouldn't",
        r'\bShouldnt\b': "Shouldn't",
        r'\bshouldnt\b': "shouldn't",
        r'\bHasnt\b': "Hasn't",
        r'\bhasnt\b': "hasn't",
        r'\bHavent\b': "Haven't",
        r'\bhavent\b': "haven't",
        r'\bDidnt\b': "Didn't",
        r'\bdidnt\b': "didn't",
        r'\bDoesnt\b': "Doesn't",
        r'\bdoesnt\b': "doesn't",
        r'\bIsnt\b': "Isn't",
        r'\bisnt\b': "isn't",
        r'\bArent\b': "Aren't",
        r'\barent\b': "aren't",
        r'\bWasnt\b': "Wasn't",
        r'\bwasnt\b': "wasn't",
        r'\bWerent\b': "Weren't",
        r'\bwerent\b': "weren't",
        r'\bLovin\b': "Lovin'",
        r'\blovin\b': "lovin'",
        r'\bCheatin\b': "Cheatin'",
        r'\bcheatin\b': "cheatin'",
        r'\bRockin\b': "Rockin'",
        r'\brockin\b': "rockin'",
        r'\bRollin\b': "Rollin'",
        r'\brollin\b': "rollin'",
        r'\bGoin\b': "Goin'",
        r'\bgoin\b': "goin'",
        r'\bComin\b': "Comin'",
        r'\bcomin\b': "comin'",
    }
    
    for pattern, replacement in contractions.items():
        if re.search(pattern, text):
            new_text = re.sub(pattern, replacement, text)
            if new_text not in variations:
                variations.append(new_text)
    
    # Common possessives (names ending in s)
    possessives = [
        (r'\bPatricks\b', "Patrick's"),
        (r'\bMothers\b', "Mother's"),
        (r'\bFathers\b', "Father's"),
        (r'\bDoctors\b', "Doctor's"),
        (r'\bDrivers\b', "Driver's"),
        (r'\bWriters\b', "Writer's"),
        (r'\bSingers\b', "Singer's"),
        (r'\bLovers\b', "Lover's"),
        (r'\bBrothers\b', "Brother's"),
        (r'\bSisters\b', "Sister's"),
    ]
    
    for pattern, replacement in possessives:
        if re.search(pattern, text, re.IGNORECASE):
            new_text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
            if new_text not in variations:
                variations.append(new_text)
    
    return variations

def find_with_apostrophe_variations(parent_dir: Path, target_name: str, cache: Dict[str, Set[str]]) -> Optional[str]:
    """
    Try to find file with various apostrophe and quote variations.
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
    
    # 2. Try normalizing quotes/apostrophes in both directions
    normalized_target = normalize_quotes_apostrophes(target_name)
    for disk_name in dir_contents:
        if normalize_quotes_apostrophes(disk_name) == normalized_target:
            return disk_name
    
    # 3. Try adding apostrophes to common contractions
    variations = add_common_apostrophes(target_name)
    for variant in variations:
        # Try each apostrophe type
        for apos in APOSTROPHE_VARIANTS:
            variant_with_apos = variant.replace("'", apos)
            if variant_with_apos in dir_contents:
                return variant_with_apos
    
    # 4. Try removing apostrophes from disk names to match DB
    target_no_apos = target_name.replace("'", "")
    for disk_name in dir_contents:
        disk_no_apos = disk_name
        for apos in APOSTROPHE_VARIANTS:
            disk_no_apos = disk_no_apos.replace(apos, "")
        if disk_no_apos == target_no_apos:
            return disk_name
    
    return None

def build_path_with_apostrophes(db_path: str) -> Optional[str]:
    """Build path fixing apostrophe issues."""
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    cache = {}  # Cache directory contents
    
    for part in parts:
        actual_name = find_with_apostrophe_variations(current_path, part, cache)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            return None
    
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fixing apostrophes and special character issues")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cantfind records, prioritize those likely to have apostrophe issues
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY 
            -- Prioritize paths with likely missing apostrophes
            CASE 
                WHEN pth ~* 'Dont|Wont|Cant|Its|Youre|Theyre|Patricks|Mothers|Fathers|Lovin|Cheatin' 
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
    
    stats = {'found': 0, 'not_found': 0, 'updated': 0}
    updates = []
    
    for (db_path,) in results:
        actual_path = build_path_with_apostrophes(db_path)
        
        if actual_path and actual_path != db_path:
            stats['found'] += 1
            updates.append((actual_path, db_path))
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