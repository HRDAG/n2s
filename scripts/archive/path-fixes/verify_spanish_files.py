#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
"""
Script to verify which Spanish-accented files actually exist on disk 
and update the database to match reality.
"""

import os
import psycopg2
from pathlib import Path

def get_spanish_accented_files():
    """Get all files with Spanish accents from the database."""
    conn = psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )
    
    cur = conn.cursor()
    
    # Get all archives-2019 files with accents
    cur.execute("""
        SELECT pth, cantfind 
        FROM fs 
        WHERE pth LIKE 'archives-2019/%' 
          AND pth ~ '[áéíóúñÁÉÍÓÚÑ]'
        ORDER BY pth
    """)
    
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return results

def check_file_variations(db_path):
    """Check if file exists with various accent combinations."""
    base_path = Path('/Volumes') / db_path
    
    # If the exact path exists, we're good
    if base_path.exists():
        return str(base_path), True
    
    # Try common variations
    variations = [
        ('Qué ', 'Que '),
        ('Cómo ', 'Como '),
        ('José ', 'Jose '),
        ('María ', 'Maria '),
        ('Jesús ', 'Jesus '),
        ('Canción', 'Cancion'),
        ('español', 'espanol'),
        ('Español', 'Espanol'),
    ]
    
    test_path = str(base_path)
    for accented, plain in variations:
        variant = test_path.replace(accented, plain)
        if Path(variant).exists():
            # Found it with this variation
            return variant, True
    
    # Didn't find any variation
    return None, False

def main():
    files = get_spanish_accented_files()
    print(f"Checking {len(files)} files with Spanish accents...")
    
    updates = []
    found_count = 0
    not_found_count = 0
    
    for db_path, cantfind in files:
        actual_path, exists = check_file_variations(db_path)
        
        if exists:
            found_count += 1
            # Extract the path relative to /Volumes/
            relative_path = str(Path(actual_path).relative_to('/Volumes'))
            
            if relative_path != db_path:
                print(f"Found with different accents:")
                print(f"  DB:   {db_path}")
                print(f"  Disk: {relative_path}")
                updates.append((relative_path, db_path))
            elif cantfind:
                print(f"Found but marked as missing: {db_path}")
                updates.append((db_path, db_path))  # Just update cantfind status
        else:
            not_found_count += 1
            if not cantfind:
                print(f"NOT found but marked as existing: {db_path}")
    
    print(f"\nSummary:")
    print(f"  Found on disk: {found_count}")
    print(f"  Not found: {not_found_count}")
    print(f"  Need DB updates: {len(updates)}")
    
    if updates:
        response = input("\nUpdate database to match disk reality? (y/n): ")
        if response.lower() == 'y':
            conn = psycopg2.connect(
                host='snowball',
                database='pbnas',
                user='pball'
            )
            cur = conn.cursor()
            
            for new_path, old_path in updates:
                if new_path == old_path:
                    # Just updating cantfind status
                    cur.execute("""
                        UPDATE fs
                        SET cantfind = false
                        WHERE pth = %s
                    """, (old_path,))
                else:
                    # Updating both path and cantfind status
                    cur.execute("""
                        UPDATE fs
                        SET pth = %s, cantfind = false
                        WHERE pth = %s
                    """, (new_path, old_path))
                    
            conn.commit()
            print(f"Updated {len(updates)} database records")
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()
