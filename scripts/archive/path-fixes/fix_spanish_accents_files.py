#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
"""
Script to rename files on disk to match the Spanish accented names in the database.
"""

import os
import sys
import psycopg2
from pathlib import Path

def get_files_to_rename():
    """Get list of files that need renaming from the database."""
    conn = psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )
    
    cur = conn.cursor()
    
    # Get all archives-2019 files with accents that are marked as cantfind
    cur.execute("""
        SELECT pth 
        FROM fs 
        WHERE pth LIKE 'archives-2019/%' 
          AND pth ~ '[áéíóúñÁÉÍÓÚÑ]'
          AND cantfind = true
        ORDER BY pth
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return [r[0] for r in results]

def remove_accents(text):
    """Remove Spanish accents from text to match current filenames."""
    replacements = [
        ('Canción', 'Cancion'),
        ('canción', 'cancion'),
        ('José ', 'Jose '),
        ('Rodríguez', 'Rodriguez'),
        ('Hernández', 'Hernandez'),
        ('Jesús ', 'Jesus '),
        ('Qué ', 'Que '),
        ('español', 'espanol'),
        ('Español', 'Espanol'),
        ('María ', 'Maria '),
        ('Pérez', 'Perez'),
        ('Adiós', 'Adios'),
        ('niña', 'nina'),
        ('niño', 'nino'),
        ('Niño ', 'Nino '),
        ('Cómo ', 'Como '),
        ('García', 'Garcia'),
        ('González', 'Gonzalez'),
        ('Martínez', 'Martinez'),
        ('Corazón', 'Corazon'),
        ('López', 'Lopez'),
        ('Sánchez', 'Sanchez'),
        ('Ramírez', 'Ramirez'),
    ]
    
    result = text
    for accented, plain in replacements:
        result = result.replace(accented, plain)
    return result

def rename_files(dry_run=True):
    """Rename files on disk to match database entries with accents."""
    
    files_to_rename = get_files_to_rename()
    print(f"Found {len(files_to_rename)} files to potentially rename")
    
    renamed_count = 0
    error_count = 0
    not_found_count = 0
    
    for db_path in files_to_rename:
        # The database path is relative to /Volumes/
        full_accented_path = Path('/Volumes') / db_path
        
        # Get the unaccented version
        unaccented_db_path = remove_accents(db_path)
        full_unaccented_path = Path('/Volumes') / unaccented_db_path
        
        # Only process if paths are different
        if full_accented_path == full_unaccented_path:
            continue
            
        # Check if the unaccented file exists
        if full_unaccented_path.exists():
            if dry_run:
                print(f"Would rename:\n  FROM: {full_unaccented_path}\n  TO:   {full_accented_path}")
            else:
                try:
                    # Make sure parent directory exists
                    full_accented_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Rename the file
                    os.rename(str(full_unaccented_path), str(full_accented_path))
                    print(f"Renamed: {full_unaccented_path.name} -> {full_accented_path.name}")
                    renamed_count += 1
                except Exception as e:
                    print(f"Error renaming {full_unaccented_path}: {e}")
                    error_count += 1
        else:
            # Check if the accented version already exists
            if not full_accented_path.exists():
                print(f"File not found (neither version exists): {full_unaccented_path}")
                not_found_count += 1
    
    print(f"\nSummary:")
    print(f"  Files renamed: {renamed_count}")
    print(f"  Errors: {error_count}")
    print(f"  Not found: {not_found_count}")
    
    return renamed_count, error_count, not_found_count

if __name__ == "__main__":
    # First do a dry run
    print("=== DRY RUN ===")
    rename_files(dry_run=True)
    
    response = input("\nProceed with actual renaming? (y/n): ")
    if response.lower() == 'y':
        print("\n=== ACTUAL RENAMING ===")
        renamed, errors, not_found = rename_files(dry_run=False)
        
        if renamed > 0:
            # Update the database to mark these as found
            conn = psycopg2.connect(
                host='snowball',
                database='pbnas',
                user='pball'
            )
            cur = conn.cursor()
            
            # Update cantfind status for successfully renamed files
            cur.execute("""
                UPDATE fs
                SET cantfind = false
                WHERE pth LIKE 'archives-2019/%' 
                  AND pth ~ '[áéíóúñÁÉÍÓÚÑ]'
                  AND cantfind = true
            """)
            
            updated = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            
            print(f"\nUpdated {updated} database records to cantfind=false")
