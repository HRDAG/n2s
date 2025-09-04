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
# scripts/fix_accents_optimized.py

"""
Optimized accent fix combining multiple strategies:
1. Common Spanish name patterns (fast)
2. Character-by-character accent removal (comprehensive)
3. Smart partial accent handling (without combinatorial explosion)
"""

import unicodedata
from pathlib import Path
import psycopg2
from loguru import logger
import sys
from typing import Optional, Dict, Set
import re

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

# Extended dictionary of common Spanish accent patterns
COMMON_ACCENT_PATTERNS = {
    # Last names
    'Rodriguez': 'Rodríguez', 'rodriguez': 'rodríguez', 'RODRIGUEZ': 'RODRÍGUEZ',
    'Hernandez': 'Hernández', 'hernandez': 'hernández', 'HERNANDEZ': 'HERNÁNDEZ',
    'Jimenez': 'Jiménez', 'jimenez': 'jiménez', 'JIMENEZ': 'JIMÉNEZ',
    'Mendez': 'Méndez', 'mendez': 'méndez', 'MENDEZ': 'MÉNDEZ',
    'Martinez': 'Martínez', 'martinez': 'martínez', 'MARTINEZ': 'MARTÍNEZ',
    'Gonzalez': 'González', 'gonzalez': 'gonzález', 'GONZALEZ': 'GONZÁLEZ',
    'Sanchez': 'Sánchez', 'sanchez': 'sánchez', 'SANCHEZ': 'SÁNCHEZ',
    'Ramirez': 'Ramírez', 'ramirez': 'ramírez', 'RAMIREZ': 'RAMÍREZ',
    'Perez': 'Pérez', 'perez': 'pérez', 'PEREZ': 'PÉREZ',
    'Garcia': 'García', 'garcia': 'garcía', 'GARCIA': 'GARCÍA',
    'Lopez': 'López', 'lopez': 'lópez', 'LOPEZ': 'LÓPEZ',
    'Gomez': 'Gómez', 'gomez': 'gómez', 'GOMEZ': 'GÓMEZ',
    'Diaz': 'Díaz', 'diaz': 'díaz', 'DIAZ': 'DÍAZ',
    'Vazquez': 'Vázquez', 'vazquez': 'vázquez', 'VAZQUEZ': 'VÁZQUEZ',
    'Munoz': 'Muñoz', 'munoz': 'muñoz', 'MUNOZ': 'MUÑOZ',
    'Alvarez': 'Álvarez', 'alvarez': 'álvarez', 'ALVAREZ': 'ÁLVAREZ',
    'Cordova': 'Córdova', 'cordova': 'córdova', 'CORDOVA': 'CÓRDOVA',
    'Fernandez': 'Fernández', 'fernandez': 'fernández', 'FERNANDEZ': 'FERNÁNDEZ',
    'Gutierrez': 'Gutiérrez', 'gutierrez': 'gutiérrez', 'GUTIERREZ': 'GUTIÉRREZ',
    
    # First names
    'Jose': 'José', 'jose': 'josé', 'JOSE': 'JOSÉ',
    'Maria': 'María', 'maria': 'maría', 'MARIA': 'MARÍA',
    'Jesus': 'Jesús', 'jesus': 'jesús', 'JESUS': 'JESÚS',
    'Angel': 'Ángel', 'angel': 'ángel', 'ANGEL': 'ÁNGEL',
    'Ramon': 'Ramón', 'ramon': 'ramón', 'RAMON': 'RAMÓN',
    'Andres': 'Andrés', 'andres': 'andrés', 'ANDRES': 'ANDRÉS',
    'Oscar': 'Óscar', 'oscar': 'óscar', 'OSCAR': 'ÓSCAR',
    'Hector': 'Héctor', 'hector': 'héctor', 'HECTOR': 'HÉCTOR',
    'Ivan': 'Iván', 'ivan': 'iván', 'IVAN': 'IVÁN',
    'Ruben': 'Rubén', 'ruben': 'rubén', 'RUBEN': 'RUBÉN',
    'Adrian': 'Adrián', 'adrian': 'adrián', 'ADRIAN': 'ADRIÁN',
    'Sebastian': 'Sebastián', 'sebastian': 'sebastián', 'SEBASTIAN': 'SEBASTIÁN',
    'Nicolas': 'Nicolás', 'nicolas': 'nicolás', 'NICOLAS': 'NICOLÁS',
    'Cesar': 'César', 'cesar': 'césar', 'CESAR': 'CÉSAR',
    'Joaquin': 'Joaquín', 'joaquin': 'joaquín', 'JOAQUIN': 'JOAQUÍN',
    'Martin': 'Martín', 'martin': 'martín', 'MARTIN': 'MARTÍN',
    'Julian': 'Julián', 'julian': 'julián', 'JULIAN': 'JULIÁN',
    'Valentin': 'Valentín', 'valentin': 'valentín', 'VALENTIN': 'VALENTÍN',
    
    # Common words in titles
    'Cancion': 'Canción', 'cancion': 'canción', 'CANCION': 'CANCIÓN',
    'Anos': 'Años', 'anos': 'años', 'ANOS': 'AÑOS',
    'Nino': 'Niño', 'nino': 'niño', 'NINO': 'NIÑO',
    'Nina': 'Niña', 'nina': 'niña', 'NINA': 'NIÑA',
    'Espanol': 'Español', 'espanol': 'español', 'ESPANOL': 'ESPAÑOL',
    'Todavia': 'Todavía', 'todavia': 'todavía', 'TODAVIA': 'TODAVÍA',
    'Como': 'Cómo', 'como': 'cómo', 'COMO': 'CÓMO',
    'Que': 'Qué', 'que': 'qué', 'QUE': 'QUÉ',
    'Mas': 'Más', 'mas': 'más', 'MAS': 'MÁS',
    'Tambien': 'También', 'tambien': 'también', 'TAMBIEN': 'TAMBIÉN',
    'Despues': 'Después', 'despues': 'después', 'DESPUES': 'DESPUÉS',
    'Lineas': 'Líneas', 'lineas': 'líneas', 'LINEAS': 'LÍNEAS',
    'Yambu': 'Yambú', 'yambu': 'yambú', 'YAMBU': 'YAMBÚ',
    'version': 'versión', 'Version': 'Versión', 'VERSION': 'VERSIÓN',
    'Invitacion': 'Invitación', 'invitacion': 'invitación', 'INVITACION': 'INVITACIÓN',
    'Corazon': 'Corazón', 'corazon': 'corazón', 'CORAZON': 'CORAZÓN',
    'Adios': 'Adiós', 'adios': 'adiós', 'ADIOS': 'ADIÓS',
    'Musica': 'Música', 'musica': 'música', 'MUSICA': 'MÚSICA',
    'Ultimo': 'Último', 'ultimo': 'último', 'ULTIMO': 'ÚLTIMO',
    'Unico': 'Único', 'unico': 'único', 'UNICO': 'ÚNICO',
}

def apply_common_patterns(text: str) -> str:
    """Apply common Spanish accent patterns word by word."""
    result = text
    # Apply patterns to whole words only using word boundaries
    for unaccented, accented in COMMON_ACCENT_PATTERNS.items():
        # Use word boundary regex to avoid partial replacements
        pattern = r'\b' + re.escape(unaccented) + r'\b'
        result = re.sub(pattern, accented, result)
    return result

def find_with_strategies(parent_dir: Path, target_name: str, cache: Dict[str, Set[str]]) -> Optional[str]:
    """
    Try multiple strategies to find a file:
    1. Exact match
    2. Common Spanish pattern fixes
    3. Remove all accents from disk names to match DB
    4. Add all accents to DB name to match disk
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
    
    # Strategy 1: Exact match
    if target_name in dir_contents:
        return target_name
    
    # Strategy 2: Apply common Spanish patterns
    with_patterns = apply_common_patterns(target_name)
    if with_patterns != target_name and with_patterns in dir_contents:
        return with_patterns
    
    # Strategy 3: DB has no accents, disk has accents
    # Remove accents from all disk names and compare
    target_no_accents = remove_all_accents(target_name)
    for disk_name in dir_contents:
        if remove_all_accents(disk_name) == target_no_accents:
            return disk_name
    
    # Strategy 4: DB has partial accents, apply patterns then check
    # This handles cases like "José Alfredo Jimenez" -> "José Alfredo Jiménez"
    if any(ord(c) > 127 for c in target_name):  # Has some accents
        # Apply patterns to potentially fix missing accents
        fixed = apply_common_patterns(target_name)
        if fixed in dir_contents:
            return fixed
        
        # Also try removing accents from DB name and matching
        for disk_name in dir_contents:
            if remove_all_accents(disk_name) == target_no_accents:
                return disk_name
    
    return None

def build_path_with_strategies(db_path: str) -> Optional[str]:
    """Build path using multiple strategies efficiently."""
    full_path = Path('/Volumes') / db_path
    if full_path.exists():
        return db_path
    
    parts = Path(db_path).parts
    current_path = Path('/Volumes')
    actual_parts = []
    cache = {}  # Cache directory contents
    
    for part in parts:
        actual_name = find_with_strategies(current_path, part, cache)
        
        if actual_name:
            actual_parts.append(actual_name)
            current_path = current_path / actual_name
        else:
            return None
    
    return str(Path(*actual_parts))

def main(limit: Optional[int] = None, dry_run: bool = True):
    """Main processing function."""
    
    logger.info("Optimized accent fix - combining all strategies")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get ALL cantfind records, prioritize those with Spanish patterns
    query = """
        SELECT pth
        FROM fs 
        WHERE cantfind = true
        ORDER BY 
            -- Prioritize paths with Spanish names/words
            CASE 
                WHEN pth ~* 'Rodriguez|Hernandez|Jimenez|Martinez|Gonzalez|Sanchez|Garcia|Lopez|Jose|Maria' 
                THEN 0 
                ELSE 1 
            END,
            -- Then paths with some accents (partial accent issues)
            CASE 
                WHEN pth ~ '[áéíóúñÁÉÍÓÚÑ]' 
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
        actual_path = build_path_with_strategies(db_path)
        
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