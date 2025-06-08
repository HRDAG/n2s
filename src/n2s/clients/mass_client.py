# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/clients/mass_client.py

"""Mass file client for testing with large datasets."""

import os
import random
from typing import List, Generator
from pathlib import Path
from loguru import logger

from .base import BaseClient, FileEntry


class MassFileClient(BaseClient):
    """Client that can generate or process massive file lists for testing."""
    
    def __init__(
        self,
        root_path: Path,
        file_count: int = 1000000,
        hardlink_ratio: float = 0.3,
        symlink_ratio: float = 0.05,
    ):
        """Initialize mass file client.
        
        Args:
            root_path: Root directory to scan or simulate
            file_count: Number of files to generate/process
            hardlink_ratio: Fraction of files that are hardlinks
            symlink_ratio: Fraction of files that are symlinks
        """
        self.root_path = Path(root_path)
        self.file_count = file_count
        self.hardlink_ratio = hardlink_ratio
        self.symlink_ratio = symlink_ratio
        
    def discover_files(self) -> List[FileEntry]:
        """Generate massive file list for testing.
        
        This creates a realistic distribution of files with hardlinks
        simulating rsync --link-dest backup structures.
        """
        logger.info(f"Generating {self.file_count:,} file entries...")
        
        files = []
        
        # Create base inodes (unique files)
        unique_files = int(self.file_count * (1 - self.hardlink_ratio))
        hardlink_files = self.file_count - unique_files
        
        # Generate unique files
        for i in range(unique_files):
            path = f"files/batch{i//10000}/file_{i:08d}.dat"
            files.append(FileEntry(
                path=path,
                st_dev=12345,  # Simulated device
                st_inode=1000000 + i,  # Unique inode
            ))
            
            if i % 100000 == 0:
                logger.info(f"Generated {i:,} unique files...")
        
        # Generate hardlinks (same inode, different paths)
        # Simulate rsync --link-dest structure with many daily backups
        logger.info("Generating hardlink entries...")
        
        for i in range(hardlink_files):
            # Pick a random existing inode to hardlink to
            source_inode = 1000000 + random.randint(0, unique_files - 1)
            
            # Create path in different backup directory
            backup_date = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            file_num = random.randint(0, unique_files - 1)
            path = f"backups/{backup_date}/files/batch{file_num//10000}/file_{file_num:08d}.dat"
            
            files.append(FileEntry(
                path=path,
                st_dev=12345,
                st_inode=source_inode,  # Same inode = hardlink
            ))
            
            if i % 100000 == 0:
                logger.info(f"Generated {i:,} hardlink entries...")
        
        logger.info(f"Generated {len(files):,} total file entries")
        logger.info(f"  - {unique_files:,} unique files")
        logger.info(f"  - {hardlink_files:,} hardlink entries")
        
        return files
    
    def discover_files_streaming(self) -> Generator[List[FileEntry], None, None]:
        """Stream file entries in batches for memory efficiency.
        
        Yields:
            Batches of FileEntry objects
        """
        batch_size = 50000
        batch = []
        
        logger.info(f"Streaming {self.file_count:,} file entries in batches of {batch_size:,}...")
        
        for entry in self._generate_entries():
            batch.append(entry)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield final batch
        if batch:
            yield batch
    
    def _generate_entries(self) -> Generator[FileEntry, None, None]:
        """Generate file entries one at a time."""
        # Generate unique files
        unique_files = int(self.file_count * (1 - self.hardlink_ratio))
        
        for i in range(unique_files):
            path = f"files/batch{i//10000}/file_{i:08d}.dat"
            yield FileEntry(
                path=path,
                st_dev=12345,
                st_inode=1000000 + i,
            )
        
        # Generate hardlinks
        hardlink_files = self.file_count - unique_files
        for i in range(hardlink_files):
            source_inode = 1000000 + random.randint(0, unique_files - 1)
            backup_date = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            file_num = random.randint(0, unique_files - 1)
            path = f"backups/{backup_date}/files/batch{file_num//10000}/file_{file_num:08d}.dat"
            
            yield FileEntry(
                path=path,
                st_dev=12345,
                st_inode=source_inode,
            )


class RealFileClient(BaseClient):
    """Client that scans real filesystem for file entries."""
    
    def __init__(self, root_path: Path):
        """Initialize with root path to scan."""
        self.root_path = Path(root_path)
        
    def discover_files(self) -> List[FileEntry]:
        """Scan real filesystem and return file entries."""
        logger.info(f"Scanning filesystem from {self.root_path}...")
        
        files = []
        file_count = 0
        
        for root, dirs, filenames in os.walk(self.root_path):
            for filename in filenames:
                file_path = Path(root) / filename
                
                try:
                    stat = file_path.stat()
                    relative_path = file_path.relative_to(self.root_path)
                    
                    files.append(FileEntry(
                        path=str(relative_path),
                        st_dev=stat.st_dev,
                        st_inode=stat.st_ino,
                    ))
                    
                    file_count += 1
                    if file_count % 100000 == 0:
                        logger.info(f"Scanned {file_count:,} files...")
                        
                except (OSError, ValueError) as e:
                    logger.warning(f"Skipping {file_path}: {e}")
                    continue
        
        logger.info(f"Scanned {len(files):,} files from {self.root_path}")
        return files