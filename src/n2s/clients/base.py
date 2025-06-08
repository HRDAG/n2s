# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/clients/base.py

"""Base client interface for n2s."""

from abc import ABC, abstractmethod
from typing import List, Tuple, NamedTuple
from pathlib import Path


class FileEntry(NamedTuple):
    """File entry with path and inode information."""
    path: str  # Relative file path
    st_dev: int  # Device number
    st_inode: int  # Inode number


class BaseClient(ABC):
    """Abstract base class for n2s clients."""
    
    @abstractmethod
    def discover_files(self) -> List[FileEntry]:
        """Discover files and return list of (path, st_dev, st_inode) tuples.
        
        Returns:
            List of FileEntry tuples with path and inode information
        """
        pass