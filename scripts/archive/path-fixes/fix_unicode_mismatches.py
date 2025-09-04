#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
#   "python-Levenshtein",
# ]
# ///
"""
Script to automatically fix Unicode normalization mismatches between database and disk.
"""

import os
import psycopg2
from pathlib import Path
import unicodedata
import Levenshtein
from collections import defaultdict

def get_missing_files(tree=None, limit=None):
    """Get files marked as cantfind=true from database."""
    conn = psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )
    
    cur = conn.cursor()
    
    query = """
        SELECT tree, pth 
        FROM fs 
        WHERE cantfind = true
    """
    
    if tree:
        query += f" AND tree = '{tree}'"
    
    query += " ORDER BY pth"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    conn.close()
    
    return results

def get_directory_files(directory_path):
    """Get all files in a directory."""
    try:
        if directory_path.exists() and directory_path.is_dir():
            return {f.name: f for f in directory_path.iterdir() if f.is_file()}
    except PermissionError:
        pass
    return {}

def find_and_fix_mismatches(tree=None, batch_size=500):
    """Find and fix Unicode normalization mismatches."""
    
    missing_files = get_missing_files(tree=tree, limit=batch_size)
    print(f"Analyzing {len(missing_files)} missing files" + (f" in tree '{tree}'" if tree else "") + "...")
    
    fixes = []
    by_directory = defaultdict(list)
    
    # Group missing files by directory
    for tree, pth in missing_files:
        full_path = Path('/Volumes') / pth
        directory = full_path.parent
        filename = full_path.name
        by_directory[directory].append((tree, pth, filename))
    
    print(f"Checking {len(by_directory)} unique directories...")
    
    for directory, missing_in_dir in by_directory.items():
        # Get actual files in this directory
        actual_files = get_directory_files(directory)
        
        if not actual_files:
            continue
            
        for tree, db_path, db_filename in missing_in_dir:
            # Check for exact filename match (case-sensitive)
            if db_filename in actual_files:
                # File exists with exact name - just mark as found
                fixes.append((tree, db_path, db_path, 'exact'))
                continue
            
            # Try NFD/NFC normalization
            nfd_name = unicodedata.normalize('NFD', db_filename)
            nfc_name = unicodedata.normalize('NFC', db_filename)
            
            actual_match = None
            match_type = None
            
            if nfd_name in actual_files:
                actual_match = nfd_name
                match_type = 'nfd'
            elif nfc_name in actual_files:
                actual_match = nfc_name
                match_type = 'nfc'
            else:
                # Try fuzzy matching for very close matches (distance <= 2)
                best_match = None
                best_distance = float('inf')
                
                for actual_name in actual_files.keys():
                    distance = Levenshtein.distance(db_filename, actual_name)
                    if distance < best_distance and distance <= 2:
                        best_distance = distance
                        best_match = actual_name
                
                if best_match and best_distance <= 2:
                    actual_match = best_match
                    match_type = f'fuzzy_{best_distance}'
            
            if actual_match:
                # Found a match - prepare the fix
                new_path = str(Path(db_path).parent / actual_match)
                if new_path != db_path:
                    fixes.append((tree, db_path, new_path, match_type))
                else:
                    fixes.append((tree, db_path, db_path, 'found'))
    
    return fixes

def apply_fixes(fixes):
    """Apply the fixes to the database."""
    if not fixes:
        print("No fixes to apply.")
        return
    
    print(f"\nFound {len(fixes)} files to fix:")
    
    # Group by type
    by_type = defaultdict(int)
    for _, _, _, match_type in fixes:
        by_type[match_type] += 1
    
    for match_type, count in sorted(by_type.items()):
        print(f"  {match_type}: {count}")
    
    # Show a few examples
    print("\nExample fixes:")
    for i, (tree, old_path, new_path, match_type) in enumerate(fixes[:5]):
        if old_path != new_path:
            old_name = Path(old_path).name
            new_name = Path(new_path).name
            print(f"  [{match_type}] '{old_name}' -> '{new_name}'")
        else:
            print(f"  [{match_type}] File found, just updating cantfind status")
    
    response = input(f"\nApply these {len(fixes)} fixes to the database? (y/n): ")
    
    if response.lower() == 'y':
        conn = psycopg2.connect(
            host='snowball',
            database='pbnas',
            user='pball'
        )
        cur = conn.cursor()
        
        update_count = 0
        for tree, old_path, new_path, _ in fixes:
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
        cur.close()
        conn.close()
        
        return update_count
    
    return 0

def main():
    """Process all trees with missing files."""
    trees = ['archives-2019', 'osxgather', 'backup', 'dump-2019']
    
    total_fixed = 0
    
    for tree in trees:
        print(f"\n{'='*60}")
        print(f"Processing tree: {tree}")
        print(f"{'='*60}")
        
        # Process in batches
        batch_num = 0
        while True:
            batch_num += 1
            print(f"\nBatch {batch_num}:")
            
            fixes = find_and_fix_mismatches(tree=tree, batch_size=500)
            
            if not fixes:
                print("No more fixes found for this tree.")
                break
            
            fixed = apply_fixes(fixes)
            total_fixed += fixed
            
            if fixed == 0:
                # User declined to apply fixes
                break
    
    print(f"\n{'='*60}")
    print(f"Total files fixed: {total_fixed}")
    
    # Show final status
    conn = psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            tree,
            COUNT(*) FILTER (WHERE cantfind = true) as still_missing
        FROM fs 
        WHERE tree IN ('archives-2019', 'osxgather', 'backup', 'dump-2019')
        GROUP BY tree
        ORDER BY still_missing DESC
    """)
    
    print("\nRemaining missing files by tree:")
    for tree, count in cur.fetchall():
        print(f"  {tree}: {count}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
