# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.08
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/n2s/service/database/operations.py

"""Database operations for n2s."""

from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime

from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, Session
from loguru import logger

from .models import Base, Changeset, File


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(self, database_url: str):
        """Initialize database manager.
        
        Args:
            database_url: SQLAlchemy database URL
                e.g., "sqlite:///path/to/manifest.db"
        """
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            # SQLite optimizations for large datasets
            connect_args={
                "timeout": 30.0,
                "check_same_thread": False,
            } if database_url.startswith("sqlite") else {},
        )
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        # Configure SQLite for performance with large datasets
        if database_url.startswith("sqlite"):
            self._configure_sqlite()
    
    def _configure_sqlite(self):
        """Configure SQLite for optimal performance."""
        with self.engine.connect() as conn:
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            # Faster writes, still safe
            conn.execute("PRAGMA synchronous=NORMAL")
            # Increase cache size (default is 2MB, set to 64MB)
            conn.execute("PRAGMA cache_size=-65536") 
            # Optimize for bulk inserts
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.commit()
        logger.info("SQLite configured for high-performance operations")
    
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created")
    
    def get_session(self) -> Session:
        """Get a database session."""
        return self.SessionLocal()
    
    def create_changeset(
        self,
        changeset_id: str,
        name: str,
        content_hash: str,
    ) -> Changeset:
        """Create a new changeset."""
        with self.get_session() as session:
            changeset = Changeset(
                changeset_id=changeset_id,
                name=name,
                content_hash=content_hash,
            )
            session.add(changeset)
            session.commit()
            session.refresh(changeset)
            logger.info(f"Created changeset: {changeset_id}")
            return changeset
    
    def bulk_insert_files(
        self,
        changeset_id: str,
        file_records: List[Dict],
        batch_size: int = 10000,
    ) -> int:
        """Bulk insert file records for performance.
        
        Args:
            changeset_id: The changeset these files belong to
            file_records: List of file record dictionaries
            batch_size: Number of records to insert per batch
            
        Returns:
            Number of records inserted
        """
        total_inserted = 0
        
        with self.get_session() as session:
            for i in range(0, len(file_records), batch_size):
                batch = file_records[i:i + batch_size]
                
                # Add changeset_id to each record
                for record in batch:
                    record["changeset_id"] = changeset_id
                
                # Bulk insert
                session.bulk_insert_mappings(File, batch)
                total_inserted += len(batch)
                
                if i % (batch_size * 10) == 0:  # Log every 100k records
                    logger.info(f"Inserted {total_inserted:,} file records...")
            
            session.commit()
            
        logger.info(f"Bulk inserted {total_inserted:,} file records for changeset {changeset_id}")
        return total_inserted
    
    def update_changeset_stats(self, changeset_id: str):
        """Update changeset file count and total size."""
        with self.get_session() as session:
            stats = session.query(
                func.count(File.path).label("file_count"),
                func.sum(File.size).label("total_size"),
            ).filter(File.changeset_id == changeset_id).first()
            
            session.query(Changeset).filter(
                Changeset.changeset_id == changeset_id
            ).update({
                "file_count": stats.file_count,
                "total_size": stats.total_size or 0,
            })
            session.commit()
            
        logger.info(f"Updated stats for changeset {changeset_id}: {stats.file_count:,} files, {stats.total_size:,} bytes")
    
    def get_canonical_files_needing_upload(self, changeset_id: str) -> List[File]:
        """Get canonical files that need to be uploaded."""
        with self.get_session() as session:
            files = session.query(File).filter(
                File.changeset_id == changeset_id,
                File.is_canonical == True,
                File.upload_finish_tm.is_(None),
            ).all()
            
        return files
    
    def mark_upload_started(self, changeset_id: str, file_id: str):
        """Mark upload as started for a file."""
        with self.get_session() as session:
            session.query(File).filter(
                File.changeset_id == changeset_id,
                File.file_id == file_id,
                File.is_canonical == True,
            ).update({"upload_start_tm": datetime.now()})
            session.commit()
    
    def mark_upload_completed(self, changeset_id: str, file_id: str):
        """Mark upload as completed for entire hardlink group."""
        with self.get_session() as session:
            # Update all files with this file_id (entire hardlink group)
            session.query(File).filter(
                File.changeset_id == changeset_id,
                File.file_id == file_id,
            ).update({"upload_finish_tm": datetime.now()})
            session.commit()
    
    def get_hardlink_groups(self, changeset_id: str) -> Dict[Tuple[int, int], List[str]]:
        """Get hardlink groups (inode -> list of paths)."""
        with self.get_session() as session:
            files = session.query(File).filter(
                File.changeset_id == changeset_id
            ).all()
            
        # Group by (st_dev, st_inode)
        groups = {}
        for file in files:
            key = (file.st_dev, file.st_inode)
            if key not in groups:
                groups[key] = []
            groups[key].append(file.path)
            
        # Only return groups with multiple paths (actual hardlinks)
        return {k: v for k, v in groups.items() if len(v) > 1}
    
    def get_changeset_status(self, changeset_id: str) -> Dict:
        """Get comprehensive status for a changeset."""
        with self.get_session() as session:
            changeset = session.query(Changeset).filter(
                Changeset.changeset_id == changeset_id
            ).first()
            
            if not changeset:
                return {}
            
            # Count upload status
            upload_stats = session.query(
                func.count(File.path).label("total_files"),
                func.count(File.upload_finish_tm).label("completed_uploads"),
                func.count(File.upload_start_tm).label("started_uploads"),
            ).filter(File.changeset_id == changeset_id).first()
            
            # Count canonical files (actual uploads needed)
            canonical_stats = session.query(
                func.count(File.path).label("canonical_files"),
                func.count(File.upload_finish_tm).label("canonical_completed"),
            ).filter(
                File.changeset_id == changeset_id,
                File.is_canonical == True,
            ).first()
            
            return {
                "changeset_id": changeset.changeset_id,
                "name": changeset.name,
                "status": changeset.status,
                "created_at": changeset.created_at,
                "file_count": changeset.file_count,
                "total_size": changeset.total_size,
                "upload_progress": {
                    "total_files": upload_stats.total_files,
                    "completed_uploads": upload_stats.completed_uploads,
                    "started_uploads": upload_stats.started_uploads,
                    "canonical_files": canonical_stats.canonical_files,
                    "canonical_completed": canonical_stats.canonical_completed,
                },
            }