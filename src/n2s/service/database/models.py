# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/service/database/models.py

"""SQLAlchemy models for n2s database schema."""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Changeset(Base):
    """Changeset tracking table."""
    
    __tablename__ = "changesets"
    
    # Primary key
    changeset_id = Column(Text, primary_key=True)  # hash(name, sorted_file_list)
    
    # Identification
    name = Column(Text, nullable=False)  # Human-readable changeset name
    content_hash = Column(Text, nullable=False)  # hash(sorted_file_ids)
    
    # Timing
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    # Status tracking
    file_count = Column(Integer, default=0)  # Number of files processed
    total_size = Column(Integer, default=0)  # Total original bytes
    status = Column(Text, default="pending")  # pending, processing, completed, failed
    
    # Relationships
    files = relationship("File", back_populates="changeset", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Changeset(id={self.changeset_id}, name={self.name}, status={self.status})>"


class File(Base):
    """File records table with hardlink and symlink support."""
    
    __tablename__ = "files"
    
    # Composite primary key
    path = Column(Text, nullable=False, primary_key=True)  # Relative file path
    changeset_id = Column(Text, ForeignKey("changesets.changeset_id"), nullable=False, primary_key=True)
    
    # File system metadata
    st_dev = Column(Integer, nullable=False)  # Device number (st_dev)
    st_inode = Column(Integer, nullable=False)  # Inode number (st_ino)
    size = Column(Integer, nullable=False)  # Original file size (or symlink target length)
    mtime = Column(DateTime, nullable=False)  # File modification time
    
    # Content and storage
    file_hash = Column(Text, nullable=False)  # BLAKE3 hash of content or symlink target
    file_id = Column(Text, nullable=False)  # Storage key (shared across hardlink groups)
    
    # File type flags
    is_canonical = Column(Boolean, nullable=False)  # TRUE for canonical path in hardlink group
    is_symlink = Column(Boolean, default=False)  # TRUE for symbolic links
    
    # Upload tracking
    upload_start_tm = Column(DateTime, nullable=True)  # When upload began
    upload_finish_tm = Column(DateTime, nullable=True)  # When upload completed
    
    # Relationships
    changeset = relationship("Changeset", back_populates="files")
    
    def __repr__(self):
        return f"<File(path={self.path}, file_id={self.file_id}, canonical={self.is_canonical})>"


# Performance indexes
Index("idx_files_upload_status", File.upload_finish_tm)
Index("idx_files_file_id", File.file_id)  
Index("idx_files_hardlinks", File.st_dev, File.st_inode)
Index("idx_changesets_status", Changeset.status)