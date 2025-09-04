#!/usr/bin/env -S uv run --quiet --script
# /// script
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
"""
Analyze which files are truly missing vs fixable mismatches.
Separates files into:
1. In non-existent backups (genuinely deleted)
2. In existing directories but with character mismatches (fixable)
3. Other issues
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

def get_existing_backups():
    """Get list of backup directories that actually exist on disk."""
    existing = set()
    
    # Known backup locations
    paths = [
        Path('/Volumes/archives-2019/old-backups/petunia'),
        Path('/Volumes/archives-2019/old-backups/piglet'),
        Path('/Volumes/backup/henwen-backups'),
        Path('/Volumes/osxgather/legacy/backup-buffer'),
    ]
    
    for base_path in paths:
        if base_path.exists():
            for item in base_path.iterdir():
                if item.is_dir() and item.name.startswith('back-'):
                    # Store relative path from /Volumes
                    rel_path = str(item).replace('/Volumes/', '')
                    existing.add(rel_path)
    
    return existing

def main():
    print("="*60)
    print("ANALYZING TRULY MISSING vs FIXABLE FILES")
    print("="*60)
    
    # Get existing backups
    print("\nScanning for existing backup directories...")
    existing_backups = get_existing_backups()
    print(f"Found {len(existing_backups)} backup directories on disk")
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Get all cantfind files
    cur.execute("""
        SELECT tree, pth
        FROM fs
        WHERE cantfind = true
        ORDER BY pth
    """)
    all_missing = cur.fetchall()
    
    print(f"Total cantfind=true files: {len(all_missing)}")
    
    # Categorize files
    in_deleted_backups = []
    in_existing_backups = []
    patricks_iphone = []
    squrl_files = []
    other_missing = []
    
    for tree, pth in all_missing:
        # Check if it's in a backup
        if '/back-' in pth:
            # Extract backup directory
            parts = pth.split('/')
            backup_idx = next((i for i, p in enumerate(parts) if p.startswith('back-')), -1)
            if backup_idx >= 0:
                backup_path = '/'.join(parts[:backup_idx+1])
                if backup_path in existing_backups:
                    in_existing_backups.append((tree, pth))
                else:
                    in_deleted_backups.append((tree, pth))
        
        # Check for specific patterns
        if 'patricks iPhone' in pth:
            patricks_iphone.append((tree, pth))
        elif '/SQURL/' in pth:
            squrl_files.append((tree, pth))
        elif '/back-' not in pth:
            other_missing.append((tree, pth))
    
    # Report findings
    print(f"\nCATEGORIZATION:")
    print(f"  In deleted backups: {len(in_deleted_backups)} ({len(in_deleted_backups)*100//len(all_missing)}%)")
    print(f"  In existing backups: {len(in_existing_backups)} ({len(in_existing_backups)*100//len(all_missing)}%)")
    print(f"  Non-backup files: {len(other_missing)} ({len(other_missing)*100//len(all_missing)}%)")
    print(f"\nSPECIFIC PATTERNS:")
    print(f"  'patricks iPhone' files: {len(patricks_iphone)}")
    print(f"  SQURL files: {len(squrl_files)}")
    
    # Check fixability of files in existing backups
    print(f"\n" + "="*60)
    print(f"CHECKING FIXABILITY OF {len(in_existing_backups)} FILES IN EXISTING BACKUPS")
    
    fixable = []
    sample_size = min(100, len(in_existing_backups))
    
    for tree, pth in in_existing_backups[:sample_size]:
        fixed_path = pth
        
        # Try common fixes
        if 'patricks iPhone' in pth:
            fixed_path = pth.replace('patricks iPhone', "patrick's iPhone")
        elif '/SQURL/' in pth:
            fixed_path = pth.replace('/SQURL/', '/SQÜRL/')
        
        if fixed_path != pth:
            full_path = Path('/Volumes') / fixed_path
            if full_path.exists():
                fixable.append((tree, pth, fixed_path))
    
    print(f"From sample of {sample_size}:")
    print(f"  Fixable: {len(fixable)}")
    print(f"  Estimated total fixable: {len(fixable) * len(in_existing_backups) // sample_size}")
    
    if fixable:
        print(f"\nExample fixes:")
        for tree, old, new in fixable[:3]:
            print(f"  {old}")
            print(f"    → {new}")
    
    # SQL to mark genuinely deleted files
    print(f"\n" + "="*60)
    print("RECOMMENDATIONS:")
    print(f"1. Mark {len(in_deleted_backups)} files in deleted backups as permanently missing")
    print(f"2. Focus fix efforts on {len(in_existing_backups)} files in existing backups")
    print(f"3. Investigate {len(other_missing)} non-backup files separately")
    
    response = input("\nGenerate SQL to mark deleted backup files? (y/n): ")
    if response.lower() == 'y':
        print("\n-- SQL to mark files in deleted backups")
        print("-- Add a 'deleted' column first if not exists:")
        print("-- ALTER TABLE fs ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE;")
        print("\nBEGIN;")
        
        # Group by backup for cleaner SQL
        by_backup = defaultdict(list)
        for tree, pth in in_deleted_backups[:100]:  # First 100 as example
            if '/back-' in pth:
                parts = pth.split('/')
                backup_idx = next((i for i, p in enumerate(parts) if p.startswith('back-')), -1)
                if backup_idx >= 0:
                    backup = parts[backup_idx]
                    by_backup[backup].append((tree, pth))
        
        for backup, files in list(by_backup.items())[:5]:
            print(f"-- {len(files)} files from {backup}")
            print(f"UPDATE fs SET deleted = true WHERE pth LIKE '%/{backup}/%' AND cantfind = true;")
        
        print("COMMIT;")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()