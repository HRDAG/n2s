#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
"""
Unified script to fix directory-level character mismatches.
Handles umlauts, apostrophes, accents, and other special characters in directory names.
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

# Define directory-level replacements
DIRECTORY_REPLACEMENTS = [
    # Most impactful fixes first
    ('/SQURL/', '/SQÜRL/'),  # 32 files
    ('patricks iPhone', "patrick's iPhone"),  # 1069 files
    
    # Spanish album that we partially fixed before
    ('Los Anos Maravillosos De La Canción Social Vol.Ii', 
     'Los Años Maravillosos De La Cancion Social Vol.Ii'),
    
    # Common name replacements
    ('José ', 'Jose '),
    ('María ', 'Maria '),
    ('André ', 'Andre '),
    ('Café ', 'Cafe '),
    
    # Music artist names with special chars
    ('Björk', 'Bjork'),
    ('Mötley Crüe', 'Motley Crue'),
    ('Beyoncé', 'Beyonce'),
    ('Sinéad', 'Sinead'),
    
    # Smart quotes to straight
    (''', "'"),
    (''', "'"),
    ('"', '"'),
    ('"', '"'),
    
    # Other punctuation
    ('—', '-'),  # em dash to hyphen
    ('–', '-'),  # en dash to hyphen
    ('…', '...'),  # ellipsis
]

def try_path_with_replacements(db_path):
    """Try various character replacements on a path."""
    full_path = Path('/Volumes') / db_path
    
    # Check exact path first
    if full_path.exists():
        return db_path, 'exact'
    
    # Try each replacement pattern
    test_path = db_path
    for old, new in DIRECTORY_REPLACEMENTS:
        if old in test_path:
            variant = test_path.replace(old, new)
            full_variant = Path('/Volumes') / variant
            if full_variant.exists():
                return variant, f'replaced_{old}_with_{new}'
    
    # Try comprehensive character replacements
    # This catches cases where multiple replacements are needed
    fixed = db_path
    for old, new in DIRECTORY_REPLACEMENTS:
        fixed = fixed.replace(old, new)
    
    if fixed != db_path:
        full_fixed = Path('/Volumes') / fixed
        if full_fixed.exists():
            return fixed, 'multiple_replacements'
    
    # Try NFD/NFC normalization
    nfd_path = unicodedata.normalize('NFD', db_path)
    if nfd_path != db_path:
        full_nfd = Path('/Volumes') / nfd_path
        if full_nfd.exists():
            return nfd_path, 'nfd_normalization'
    
    nfc_path = unicodedata.normalize('NFC', db_path)
    if nfc_path != db_path:
        full_nfc = Path('/Volumes') / nfc_path
        if full_nfc.exists():
            return nfc_path, 'nfc_normalization'
    
    return None, None

def analyze_batch(batch_size=1000):
    """Analyze a batch of cantfind files for fixable issues."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Get batch of cantfind files
    cur.execute("""
        SELECT tree, pth
        FROM fs
        WHERE cantfind = true
        ORDER BY pth
        LIMIT %s
    """, (batch_size,))
    
    files = cur.fetchall()
    cur.close()
    conn.close()
    
    return files

def process_files(files):
    """Process files and find fixes."""
    fixes = []
    fix_types = defaultdict(int)
    
    for tree, db_path in files:
        fixed_path, fix_type = try_path_with_replacements(db_path)
        
        if fixed_path:
            if fixed_path != db_path:
                fixes.append((tree, db_path, fixed_path))
                fix_types[fix_type] += 1
            else:
                # File exists, just needs cantfind=false
                fixes.append((tree, db_path, db_path))
                fix_types['exact'] += 1
    
    return fixes, fix_types

def apply_fixes(fixes):
    """Apply fixes to the database."""
    if not fixes:
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
    cur.close()
    conn.close()
    
    return update_count

def main():
    """Main processing loop."""
    print("="*60)
    print("FIXING DIRECTORY-LEVEL CHARACTER MISMATCHES")
    print("="*60)
    print("\nThis script fixes:")
    print("  - SQURL -> SQÜRL (umlaut)")
    print("  - patricks iPhone -> patrick's iPhone (apostrophe)")
    print("  - Spanish/French accents in directory names")
    print("  - Smart quotes, dashes, and other punctuation")
    print()
    
    # Get initial count
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fs WHERE cantfind = true")
    initial_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"Starting with {initial_count} cantfind=true records\n")
    
    total_fixed = 0
    batch_num = 0
    
    while True:
        batch_num += 1
        print(f"--- Batch {batch_num} ---")
        
        # Get batch of files
        files = analyze_batch(batch_size=1000)
        
        if not files:
            print("No more files to process")
            break
        
        print(f"Processing {len(files)} files...")
        
        # Find fixes
        fixes, fix_types = process_files(files)
        
        if not fixes:
            print("No fixes found in this batch")
            break
        
        # Report findings
        print(f"Found {len(fixes)} fixable files")
        
        if fix_types:
            print("\nFix types:")
            for fix_type, count in sorted(fix_types.items(), key=lambda x: -x[1]):
                # Clean up the fix type for display
                display_type = fix_type.replace('replaced_', '').replace('_with_', ' → ')
                print(f"  {display_type}: {count}")
        
        # Show examples
        print("\nExample fixes (first 3):")
        for tree, old_path, new_path in fixes[:3]:
            if old_path != new_path:
                # Show just the differing parts
                old_parts = old_path.split('/')
                new_parts = new_path.split('/')
                
                for i, (old_part, new_part) in enumerate(zip(old_parts, new_parts)):
                    if old_part != new_part:
                        print(f"  {'/'.join(old_parts[:i])}/")
                        print(f"    '{old_part}' → '{new_part}'")
                        break
            else:
                print(f"  [exact match] {old_path}")
        
        # Apply fixes
        response = input(f"\nApply {len(fixes)} fixes? (y/n/q to quit): ").lower()
        
        if response == 'q':
            break
        elif response == 'y':
            updated = apply_fixes(fixes)
            total_fixed += updated
            print(f"Updated {updated} database records")
            
            # Show progress
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM fs WHERE cantfind = true")
            remaining = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            print(f"Remaining cantfind=true: {remaining}")
            print(f"Fixed so far: {total_fixed} ({total_fixed*100//initial_count}%)")
        else:
            print("Skipping this batch")
        
        print()
    
    print("="*60)
    print(f"SUMMARY")
    print(f"  Started with: {initial_count}")
    print(f"  Fixed: {total_fixed}")
    print(f"  Success rate: {total_fixed*100//initial_count if initial_count else 0}%")
    
    # Final status
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fs WHERE cantfind = true")
    final_count = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"  Remaining: {final_count}")
    print("="*60)

if __name__ == "__main__":
    main()