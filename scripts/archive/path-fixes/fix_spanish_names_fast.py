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
# scripts/fix_spanish_names_fast.py

"""
Fast fix for common Spanish name accent patterns.
Instead of trying all combinations, use known patterns.
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional, Dict

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

# Common Spanish name patterns where accents are often missing
COMMON_FIXES = {
    # Last names
    'Rodriguez': 'Rodríguez',
    'Hernandez': 'Hernández',
    'Jimenez': 'Jiménez',
    'Mendez': 'Méndez',
    'Martinez': 'Martínez',
    'Gonzalez': 'González',
    'Sanchez': 'Sánchez',
    'Ramirez': 'Ramírez',
    'Perez': 'Pérez',
    'Garcia': 'García',
    'Lopez': 'López',
    'Gomez': 'Gómez',
    'Diaz': 'Díaz',
    'Vazquez': 'Vázquez',
    'Munoz': 'Muñoz',
    'Alvarez': 'Álvarez',
    'Cordova': 'Córdova',
    
    # First names
    'Jose': 'José',
    'Maria': 'María',
    'Jesus': 'Jesús',
    'Angel': 'Ángel',
    'Ramon': 'Ramón',
    'Andres': 'Andrés',
    'Oscar': 'Óscar',
    'Hector': 'Héctor',
    'Ivan': 'Iván',
    'Ruben': 'Rubén',
    'Adrian': 'Adrián',
    'Sebastian': 'Sebastián',
    'Nicolas': 'Nicolás',
    'Cesar': 'César',
    
    # Common words in titles
    'Cancion': 'Canción',
    'Anos': 'Años',
    'Nino': 'Niño',
    'Nina': 'Niña',
    'Espanol': 'Español',
    'Todavia': 'Todavía',
    'Como': 'Cómo',
    'Que': 'Qué',
    'Mas': 'Más',
    'Tambien': 'También',
    'Despues': 'Después',
    'Lineas': 'Líneas',
    'Yambu': 'Yambú',
    'version': 'versión',
    'Invitacion': 'Invitación',
    
    # Handle upper case versions
    'HERNANDEZ': 'HERNÁNDEZ',
    'RODRIGUEZ': 'RODRÍGUEZ',
    'JIMENEZ': 'JIMÉNEZ',
    'MENDEZ': 'MÉNDEZ',
    'MARTINEZ': 'MARTÍNEZ',
}

def apply_common_fixes(text: str) -> str:
    """Apply common Spanish name/word fixes."""
    result = text
    for wrong, right in COMMON_FIXES.items():
        # Case-sensitive replacement for exact matches
        result = result.replace(wrong, right)
    return result

def find_with_common_fixes(parent_dir: Path, target_name: str) -> Optional[str]:
    """
    Try to find file with common Spanish accent fixes applied.
    """
    if not parent_dir.exists():
        return None
    
    try:
        # First try exact match
        for item in parent_dir.iterdir():
            if item.name == target_name:
                return item.name
        
        # Try with common fixes applied
        fixed_name = apply_common_fixes(target_name)
        if fixed_name != target_name:
            for item in parent_dir.iterdir():
                if item.name == fixed_name:
                    return item.name
        
        # Try removing all accents from disk names to match DB
        for item in parent_dir.iterdir():
            if remove_all_accents(item.name) == target_name:
                return item.name
            # Also try if DB name with fixes matches disk
            if item.name == fixed_name:
                return item.name
    
    except (PermissionError, OSError):
        pass
    
    return None

def build_path_with_fixes(db_path: str) -> Optional[str]:
    """Build path using common Spanish fixes."""
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    
    for part in parts:
        actual_name = find_with_common_fixes(current_path, part)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            return None
    
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Fast Spanish name accent fix")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Focus on records with Spanish names/words
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        AND (
            pth LIKE '%Rodriguez%' OR pth LIKE '%Rodríguez%' OR
            pth LIKE '%Hernandez%' OR pth LIKE '%Hernández%' OR
            pth LIKE '%Jimenez%' OR pth LIKE '%Jiménez%' OR
            pth LIKE '%Mendez%' OR pth LIKE '%Méndez%' OR
            pth LIKE '%Martinez%' OR pth LIKE '%Martínez%' OR
            pth LIKE '%Jose %' OR pth LIKE '%José %' OR
            pth LIKE '%Maria %' OR pth LIKE '%María %' OR
            pth LIKE '%Yambu%' OR pth LIKE '%Cancion%' OR
            pth LIKE '%Anos%' OR pth LIKE '%Todavia%'
        )
        ORDER BY pth
    """
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    
    logger.info(f"Processing {len(results)} records with Spanish names/words")
    
    stats = {'found': 0, 'not_found': 0, 'updated': 0}
    updates = []
    
    for (db_path,) in results:
        actual_path = build_path_with_fixes(db_path)
        
        if actual_path and actual_path != db_path:
            stats['found'] += 1
            updates.append((actual_path, db_path))
            logger.success(f"Fixed: {db_path}")
            logger.success(f"   -> {actual_path}")
            
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
    
    # Final update
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
    
    conn.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--execute", action="store_true")
    
    args = parser.parse_args()
    main(limit=args.limit, dry_run=not args.execute)