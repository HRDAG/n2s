#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
#   "python-Levenshtein",
# ]
# ///
"""
Script to find files on disk that are near-matches to missing DB entries.
This helps identify normalization issues.
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

def normalize_variations(text):
    """Generate common normalization variations of a string."""
    variations = []
    
    # Original
    variations.append(text)
    
    # NFD normalization (decomposed)
    variations.append(unicodedata.normalize('NFD', text))
    
    # NFC normalization (composed)
    variations.append(unicodedata.normalize('NFC', text))
    
    # Replace curly quotes with straight
    straight = text.replace(''', "'").replace(''', "'").replace('"', '"').replace('"', '"')
    variations.append(straight)
    
    # Replace straight quotes with curly
    curly = text.replace("'", "'")
    variations.append(curly)
    
    # Various dash replacements
    dashes = text.replace('—', '-').replace('–', '-')
    variations.append(dashes)
    
    # Remove double spaces
    no_double = ' '.join(text.split())
    variations.append(no_double)
    
    return list(set(variations))

def find_near_matches():
    """Find near-matches between DB paths and disk files."""
    
    missing_files = get_missing_files(limit=100)  # Start with first 100
    print(f"Analyzing {len(missing_files)} missing files...")
    
    matches_found = []
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
            # Try exact match with variations
            for variant in normalize_variations(db_filename):
                if variant in actual_files:
                    actual_path = actual_files[variant]
                    if db_filename != variant:
                        print(f"\nFound variation match!")
                        print(f"  Tree: {tree}")
                        print(f"  DB filename:   '{db_filename}'")
                        print(f"  Disk filename: '{variant}'")
                        matches_found.append((tree, db_path, str(actual_path)))
                        break
            else:
                # No exact variant match, try fuzzy matching
                best_match = None
                best_distance = float('inf')
                
                for actual_name in actual_files.keys():
                    distance = Levenshtein.distance(db_filename, actual_name)
                    if distance < best_distance and distance <= 3:  # Max 3 character difference
                        best_distance = distance
                        best_match = actual_name
                
                if best_match and best_distance <= 2:  # Only show very close matches
                    print(f"\nFound close match (distance={best_distance})!")
                    print(f"  Tree: {tree}")
                    print(f"  DB filename:   '{db_filename}'")
                    print(f"  Disk filename: '{best_match}'")
                    
                    # Show the character differences
                    for i, (c1, c2) in enumerate(zip(db_filename, best_match)):
                        if c1 != c2:
                            print(f"    Position {i}: '{c1}' (U+{ord(c1):04X}) vs '{c2}' (U+{ord(c2):04X})")
                    
                    matches_found.append((tree, db_path, str(directory / best_match)))
    
    print(f"\n\nSummary: Found {len(matches_found)} potential matches")
    return matches_found

if __name__ == "__main__":
    matches = find_near_matches()
    
    if matches:
        response = input("\nGenerate SQL to fix these? (y/n): ")
        if response.lower() == 'y':
            print("\n-- SQL to update database with correct paths")
            print("BEGIN;")
            for tree, old_path, new_full_path in matches:
                # Convert full path back to relative path
                new_path = str(Path(new_full_path).relative_to('/Volumes'))
                print(f"UPDATE fs SET pth = '{new_path}', cantfind = false WHERE tree = '{tree}' AND pth = '{old_path}';")
            print("COMMIT;")
            print(f"\n-- This will update {len(matches)} records")
