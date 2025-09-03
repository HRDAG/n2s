#!/usr/bin/env python3
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/test_mass_client.py

"""Test script for mass file client and database operations."""

import blake3
from pathlib import Path
from collections import defaultdict
from loguru import logger

from n2s.clients.mass_client import MassFileClient
from n2s.service.database.operations import DatabaseManager


def process_hardlink_groups(file_entries):
    """Process file entries into hardlink groups and generate file records."""
    logger.info("Processing hardlink groups...")
    
    # Group files by (st_dev, st_inode)
    inode_groups = defaultdict(list)
    for entry in file_entries:
        key = (entry.st_dev, entry.st_inode)
        inode_groups[key].append(entry.path)
    
    # Generate file records with canonical path selection
    file_records = []
    hardlink_groups = 0
    
    for (st_dev, st_inode), paths in inode_groups.items():
        # Pick canonical path (lexicographically first)
        canonical_path = min(paths)
        
        # Generate fake file hash and file_id
        fake_content_hash = blake3.blake3(f"content_for_inode_{st_inode}".encode()).hexdigest()
        file_id = blake3.blake3(f"{canonical_path}:{fake_content_hash}".encode()).hexdigest()
        
        if len(paths) > 1:
            hardlink_groups += 1
        
        # Create records for all paths in group
        for path in paths:
            file_records.append({
                "path": path,
                "st_dev": st_dev,
                "st_inode": st_inode,
                "size": 1024 + (st_inode % 10000),  # Fake size
                "mtime": "2025-01-01 12:00:00",  # Fake mtime
                "file_hash": fake_content_hash,
                "file_id": file_id,  # Shared across hardlink group
                "is_canonical": (path == canonical_path),
                "is_symlink": False,
            })
    
    logger.info(f"Processed {len(file_records):,} file records")
    logger.info(f"Found {hardlink_groups:,} hardlink groups")
    logger.info(f"Deduplication ratio: {len(file_records) / len(inode_groups):.2f}x")
    
    return file_records


def test_mass_processing(file_count: int = 1000000):
    """Test mass file processing."""
    logger.info(f"Testing mass processing with {file_count:,} files")
    
    # Create mass client
    client = MassFileClient(
        root_path=Path("/fake/root"),
        file_count=file_count,
        hardlink_ratio=0.7,  # 70% hardlinks for realistic backup scenario
    )
    
    # Generate file entries
    logger.info("Generating file entries...")
    file_entries = client.discover_files()
    
    # Process into hardlink groups
    file_records = process_hardlink_groups(file_entries)
    
    # Test database operations
    logger.info("Testing database operations...")
    db_path = Path("/tmp/test_n2s_mass.db")
    if db_path.exists():
        db_path.unlink()
    
    db = DatabaseManager(f"sqlite:///{db_path}")
    db.create_tables()
    
    # Create changeset
    changeset_id = blake3.blake3(f"test_mass_{file_count}".encode()).hexdigest()
    content_hash = blake3.blake3("sorted_file_list".encode()).hexdigest()
    
    changeset = db.create_changeset(
        changeset_id=changeset_id,
        name=f"test_mass_{file_count}",
        content_hash=content_hash,
    )
    
    # Bulk insert files
    logger.info("Bulk inserting file records...")
    inserted_count = db.bulk_insert_files(changeset_id, file_records)
    
    # Update changeset stats
    db.update_changeset_stats(changeset_id)
    
    # Get status
    status = db.get_changeset_status(changeset_id)
    logger.info(f"Changeset status: {status}")
    
    # Show some hardlink group info
    hardlink_groups = db.get_hardlink_groups(changeset_id)
    logger.info(f"Found {len(hardlink_groups):,} hardlink groups in database")
    
    # Show a few example groups
    for i, ((st_dev, st_inode), paths) in enumerate(hardlink_groups.items()):
        if i < 3:  # Show first 3 groups
            logger.info(f"  Hardlink group {st_dev}:{st_inode} -> {len(paths)} paths")
            for path in paths[:3]:  # Show first 3 paths
                logger.info(f"    {path}")
            if len(paths) > 3:
                logger.info(f"    ... and {len(paths) - 3} more")
    
    logger.info(f"Database created at: {db_path}")
    logger.info(f"Database size: {db_path.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    import sys
    
    # Default to 1M files, or take from command line
    file_count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000000
    test_mass_processing(file_count)