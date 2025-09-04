#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
#   "python-Levenshtein",
# ]
# ///
"""
Script to fix Unicode normalization and accent mismatches in directory paths.
Focuses on paths where the directory names have accent mismatches.
"""

import os
import psycopg2
from pathlib import Path
import unicodedata
from collections import defaultdict

def get_connection():
    """Create database connection."""
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )

def get_missing_files_with_accents(limit=None):
    """Get files marked as cantfind=true that have accented characters."""
    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT tree, pth 
        FROM fs 
        WHERE cantfind = true
          AND pth ~ '[àáâãäåæçèéêëìíîïñòóôõöøùúûüýÿĀāĂăĄąĆćĈĉĊċČčĎďĐđĒēĔĕĖėĘęĚěĜĝĞğĠġĢģĤĥĦħĨĩĪīĬĭĮįİıĴĵĶķĸĹĺĻļĽľĿŀŁłŃńŅņŇňŉŊŋŌōŎŏŐőŒœŔŕŖŗŘřŚśŜŝŞşŠšŢţŤťŦŧŨũŪūŬŭŮůŰűŲųŴŵŶŷŸŹźŻżŽž]'
        ORDER BY pth
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return results

def try_path_variations(db_path):
    """Try various accent and normalization variations of a path."""
    # Known directory-level replacements found in the data
    directory_replacements = [
        # This is a key pattern we discovered
        ('Los Anos Maravillosos De La Canción Social Vol.Ii', 
         'Los Años Maravillosos De La Cancion Social Vol.Ii'),
        # Add other common Spanish music album names
        ('Canción Social', 'Cancion Social'),
        ('Canción', 'Cancion'),
        ('canción', 'cancion'),
    ]
    
    # Common Spanish accent replacements
    accent_replacements = [
        ('á', 'a'), ('Á', 'A'),
        ('é', 'e'), ('É', 'E'),
        ('í', 'i'), ('Í', 'I'),
        ('ó', 'o'), ('Ó', 'O'),
        ('ú', 'u'), ('Ú', 'U'),
        ('ñ', 'n'), ('Ñ', 'N'),
        ('ü', 'u'), ('Ü', 'U'),
    ]
    
    # Common filename-level replacements
    filename_replacements = [
        ('José ', 'Jose '),
        ('María ', 'Maria '),
        ('Jesús ', 'Jesus '),
        ('Qué ', 'Que '),
        ('Cómo ', 'Como '),
        ('García', 'Garcia'),
        ('González', 'Gonzalez'),
        ('Martínez', 'Martinez'),
        ('Hernández', 'Hernandez'),
        ('Rodríguez', 'Rodriguez'),
        ('Pérez', 'Perez'),
        ('Sánchez', 'Sanchez'),
        ('Ramírez', 'Ramirez'),
        ('López', 'Lopez'),
    ]
    
    full_path = Path('/Volumes') / db_path
    
    # Check exact path first
    if full_path.exists():
        return str(full_path), 'exact'
    
    # Try directory-level replacements first (most effective)
    test_path = db_path
    for old, new in directory_replacements:
        if old in test_path:
            variant = test_path.replace(old, new)
            full_variant = Path('/Volumes') / variant
            if full_variant.exists():
                return str(full_variant), f'dir_replace_{old}'
    
    # Split path into directory and filename
    parts = db_path.rsplit('/', 1)
    if len(parts) == 2:
        dir_path, filename = parts
        
        # Try variations on the directory path
        dir_variants = [dir_path]
        
        # Apply directory replacements
        for old, new in directory_replacements:
            if old in dir_path:
                dir_variants.append(dir_path.replace(old, new))
        
        # Try removing all accents from directory
        no_accent_dir = dir_path
        for acc, plain in accent_replacements:
            no_accent_dir = no_accent_dir.replace(acc, plain)
        if no_accent_dir != dir_path:
            dir_variants.append(no_accent_dir)
        
        # Try filename variations
        filename_variants = [filename]
        
        # Apply filename replacements
        for old, new in filename_replacements:
            if old in filename:
                filename_variants.append(filename.replace(old, new))
        
        # Try removing accents from filename
        no_accent_file = filename
        for acc, plain in accent_replacements:
            no_accent_file = no_accent_file.replace(acc, plain)
        if no_accent_file != filename:
            filename_variants.append(no_accent_file)
        
        # Try all combinations
        for dir_var in dir_variants:
            for file_var in filename_variants:
                test_path = Path('/Volumes') / dir_var / file_var
                if test_path.exists():
                    return str(test_path), 'combined_variation'
    
    # Try NFD/NFC normalization
    nfd_path = unicodedata.normalize('NFD', db_path)
    full_nfd = Path('/Volumes') / nfd_path
    if full_nfd.exists():
        return str(full_nfd), 'nfd'
    
    nfc_path = unicodedata.normalize('NFC', db_path)
    full_nfc = Path('/Volumes') / nfc_path
    if full_nfc.exists():
        return str(full_nfc), 'nfc'
    
    # Try removing all accents
    no_accents = db_path
    for acc, plain in accent_replacements:
        no_accents = no_accents.replace(acc, plain)
    full_no_accents = Path('/Volumes') / no_accents
    if full_no_accents.exists():
        return str(full_no_accents), 'remove_all_accents'
    
    return None, None

def process_files(batch_size=500):
    """Process files with accent mismatches."""
    files = get_missing_files_with_accents(limit=batch_size)
    
    if not files:
        print("No files with accents found in cantfind=true records")
        return []
    
    print(f"Processing {len(files)} files with accented characters...")
    
    fixes = []
    by_type = defaultdict(int)
    
    for tree, db_path in files:
        actual_path, variation_type = try_path_variations(db_path)
        
        if actual_path:
            # Convert back to relative path
            fixed_path = str(Path(actual_path).relative_to('/Volumes'))
            if fixed_path != db_path:
                fixes.append((tree, db_path, fixed_path))
                by_type[variation_type] += 1
            else:
                # File exists with exact path, just needs cantfind updated
                fixes.append((tree, db_path, db_path))
                by_type['exact'] += 1
    
    # Report findings
    print(f"\nFound {len(fixes)} fixable files out of {len(files)}")
    print("\nBreakdown by fix type:")
    for fix_type, count in sorted(by_type.items()):
        print(f"  {fix_type}: {count}")
    
    # Show examples
    if fixes:
        print("\nExample fixes (first 5):")
        for i, (tree, old_path, new_path) in enumerate(fixes[:5], 1):
            if old_path != new_path:
                print(f"\n{i}. Tree: {tree}")
                print(f"   From: {old_path}")
                print(f"   To:   {new_path}")
            else:
                print(f"\n{i}. Tree: {tree}")
                print(f"   Path: {old_path} (just updating cantfind status)")
    
    return fixes

def apply_fixes(fixes):
    """Apply the fixes to the database."""
    if not fixes:
        print("No fixes to apply")
        return 0
    
    response = input(f"\nApply {len(fixes)} fixes to the database? (y/n): ")
    
    if response.lower() != 'y':
        print("Aborted")
        return 0
    
    conn = get_connection()
    cur = conn.cursor()
    
    update_count = 0
    for tree, old_path, new_path in fixes:
        if old_path != new_path:
            # Update both path and cantfind status
            cur.execute("""
                UPDATE fs
                SET pth = %s, cantfind = false
                WHERE tree = %s AND pth = %s
            """, (new_path, tree, old_path))
        else:
            # Just update cantfind status
            cur.execute("""
                UPDATE fs
                SET cantfind = false
                WHERE tree = %s AND pth = %s
            """, (tree, old_path))
        
        update_count += cur.rowcount
    
    conn.commit()
    print(f"Successfully updated {update_count} database records")
    
    # Show remaining stats
    cur.execute("""
        SELECT COUNT(*) 
        FROM fs 
        WHERE cantfind = true
    """)
    remaining = cur.fetchone()[0]
    print(f"Remaining cantfind=true records: {remaining}")
    
    cur.close()
    conn.close()
    
    return update_count

def main():
    """Process files in batches."""
    print("="*60)
    print("Fixing path-level accent mismatches")
    print("="*60)
    
    total_fixed = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        print(f"\n--- Batch {batch_num} ---")
        
        fixes = process_files(batch_size=500)
        
        if not fixes:
            print("No more fixes found")
            break
        
        fixed = apply_fixes(fixes)
        total_fixed += fixed
        
        if fixed == 0:
            # User declined to apply fixes
            break
    
    print(f"\n{'='*60}")
    print(f"Total files fixed: {total_fixed}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()