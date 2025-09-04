#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
"""
Comprehensive fix for all character mismatches.
Focuses on high-impact patterns that we know work.
"""

import psycopg2
from pathlib import Path
from collections import defaultdict

def get_connection():
    return psycopg2.connect(
        host='snowball',
        database='pbnas',
        user='pball'
    )

def apply_all_fixes(db_path):
    """Apply all known working fixes to a path."""
    # Track what fix was applied
    fix_applied = None
    result = db_path
    
    # 1. MOST COMMON: patricks iPhone -> patrick's iPhone (984 files!)
    if 'patricks iPhone' in result:
        result = result.replace('patricks iPhone', "patrick's iPhone")
        fix_applied = "patricks_to_apostrophe"
    
    # 2. Spanish album directory (147+ files with accents)
    if 'Los Anos Maravillosos De La Canción Social Vol.Ii' in result:
        result = result.replace(
            'Los Anos Maravillosos De La Canción Social Vol.Ii',
            'Los Años Maravillosos De La Cancion Social Vol.Ii'
        )
        fix_applied = "spanish_album_fix"
    
    # 3. SQURL -> SQÜRL
    if '/SQURL/' in result:
        result = result.replace('/SQURL/', '/SQÜRL/')
        fix_applied = "squrl_umlaut"
    
    # 4. Other common Spanish names
    replacements = [
        ('José ', 'Jose '),
        ('María ', 'Maria '),
        ('Jesús ', 'Jesus '),
        ('Qué ', 'Que '),
        ('Cómo ', 'Como '),
    ]
    
    if not fix_applied:
        for old, new in replacements:
            if old in result:
                result = result.replace(old, new)
                fix_applied = f"spanish_{old}"
                break
    
    return result, fix_applied

def main():
    print("="*60)
    print("COMPREHENSIVE CHARACTER MISMATCH FIXER")
    print("="*60)
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get ALL cantfind files at once (not in batches)
    print("\nLoading all cantfind=true files...")
    cur.execute("""
        SELECT tree, pth
        FROM fs
        WHERE cantfind = true
        ORDER BY 
            CASE 
                WHEN pth LIKE '%patricks iPhone%' THEN 1
                WHEN pth LIKE '%Canción%' THEN 2
                WHEN pth LIKE '%SQURL%' THEN 3
                ELSE 4
            END,
            pth
    """)
    
    all_files = cur.fetchall()
    print(f"Loaded {len(all_files)} files")
    
    # Process all files
    print("\nChecking for fixes...")
    fixes = []
    fix_types = defaultdict(int)
    
    for tree, db_path in all_files:
        fixed_path, fix_type = apply_all_fixes(db_path)
        
        if fixed_path != db_path:
            # Check if the fixed path exists
            full_path = Path('/Volumes') / fixed_path
            if full_path.exists():
                fixes.append((tree, db_path, fixed_path))
                fix_types[fix_type] += 1
                
                # Show progress every 100 fixes
                if len(fixes) % 100 == 0:
                    print(f"  Found {len(fixes)} fixes so far...")
    
    # Report findings
    print(f"\n" + "="*60)
    print(f"FOUND {len(fixes)} FIXABLE FILES")
    print(f"\nBreakdown by fix type:")
    for fix_type, count in sorted(fix_types.items(), key=lambda x: -x[1]):
        print(f"  {fix_type}: {count}")
    
    if fixes:
        print(f"\nExample fixes (first 5):")
        for tree, old, new in fixes[:5]:
            # Show the key difference
            if 'patricks' in old:
                print(f"  patricks → patrick's")
            elif 'Canción' in old:
                print(f"  Canción → Cancion, Anos → Años")
            elif 'SQURL' in old:
                print(f"  SQURL → SQÜRL")
            print(f"    {old[-70:]}")
    
    # Apply fixes
    if fixes:
        response = input(f"\nApply all {len(fixes)} fixes? (y/n): ")
        if response.lower() == 'y':
            print("\nApplying fixes...")
            
            update_count = 0
            # Process in chunks for performance
            chunk_size = 100
            for i in range(0, len(fixes), chunk_size):
                chunk = fixes[i:i+chunk_size]
                
                for tree, old_path, new_path in chunk:
                    cur.execute("""
                        UPDATE fs
                        SET pth = %s, cantfind = false
                        WHERE tree = %s AND pth = %s
                    """, (new_path, tree, old_path))
                    update_count += cur.rowcount
                
                conn.commit()
                print(f"  Processed {min(i+chunk_size, len(fixes))}/{len(fixes)}")
            
            print(f"\nSuccessfully updated {update_count} records")
            
            # Show final status
            cur.execute("SELECT COUNT(*) FROM fs WHERE cantfind = true")
            remaining = cur.fetchone()[0]
            print(f"Remaining cantfind=true: {remaining}")
            
            # Calculate improvement
            original = len(all_files)
            fixed_pct = (update_count * 100) // original
            print(f"Fixed {fixed_pct}% of originally missing files")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()