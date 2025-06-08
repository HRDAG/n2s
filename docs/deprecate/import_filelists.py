#!/usr/bin/env python3
"""Import null-delimited file lists into PostgreSQL database using psycopg3."""

import os
import sys
import psycopg
from pathlib import Path
import typer
from typing import Iterator, Tuple, Optional
from io import StringIO
import logging
from typing_extensions import Annotated
import random

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File list mapping
FILE_LISTS = {
    'backup': '/var/tmp/pball/nas-snapshot-files/hrdag-nas-nas-backup-filelist.txt',
    'legacy': '/var/tmp/pball/nas-snapshot-files/hrdag-nas-nas-legacy-filelist.txt',
    'working': '/var/tmp/pball/nas-snapshot-files/hrdag-nas-nas-working-filelist.txt'
}

PREFIX_TO_STRIP = '/mnt/snapshot/'
BATCH_SIZE = 100000  # Process in batches for memory efficiency


def parse_null_delimited_file(filepath: str, dataset: str) -> Iterator[Tuple[str, str]]:
    """Parse null-delimited file and yield (trimmed_path, dataset) tuples."""
    count = 0
    skipped = 0
    
    with open(filepath, 'rb') as f:
        while True:
            # Read until null byte
            path_bytes = bytearray()
            while True:
                byte = f.read(1)
                if not byte:  # EOF
                    if path_bytes:
                        path = path_bytes.decode('utf-8', errors='replace')
                        if not path.startswith('#'):
                            yield process_path(path, dataset)
                            count += 1
                    logger.info(f"Processed {count} files from {dataset} dataset (skipped {skipped})")
                    return
                if byte == b'\0':  # Null terminator
                    break
                path_bytes.extend(byte)
            
            if path_bytes:
                try:
                    path = path_bytes.decode('utf-8', errors='replace')
                    # Skip comments
                    if not path.startswith('#'):
                        processed = process_path(path, dataset)
                        if processed[0]:  # Only yield non-empty paths
                            yield processed
                            count += 1
                        else:
                            skipped += 1
                except Exception as e:
                    logger.warning(f"Failed to process path: {e}")
                    skipped += 1
                    
                if count % 100000 == 0 and count > 0:
                    logger.info(f"Processed {count} files from {dataset} dataset...")


def process_path(path: str, dataset: str) -> Tuple[str, str]:
    """Process a single path by stripping prefix."""
    path = path.strip()
    if path.startswith(PREFIX_TO_STRIP):
        path = path[len(PREFIX_TO_STRIP):]
    return (path, dataset)


def validate_path(path: str, dataset: str) -> bool:
    """Validate that a path exists in the filesystem."""
    full_path = f"/mnt/nas/{dataset}/{path}"
    return os.path.exists(full_path)


def import_dataset_with_copy(conn, dataset: str, filepath: str, validate_fraction: float = 0.0, clean_first: bool = True):
    """Import a dataset using COPY for maximum performance."""
    logger.info(f"Starting import of {dataset} dataset from {filepath}")
    
    # Clean existing records for this dataset
    if clean_first:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM files WHERE dataset = %s", (dataset,))
            deleted = cursor.rowcount
            conn.commit()
            if deleted > 0:
                logger.info(f"Deleted {deleted} existing records for dataset {dataset}")
    
    if validate_fraction > 0:
        logger.info(f"Will validate {validate_fraction*100:.1f}% of paths")
    
    total_count = 0
    valid_count = 0
    invalid_count = 0
    validated_count = 0
    
    try:
        with conn.cursor() as cursor:
            # Create a StringIO buffer to accumulate data
            buffer = StringIO()
            buffer_count = 0
            
            for file_path, ds in parse_null_delimited_file(filepath, dataset):
                if not file_path:
                    continue
                    
                total_count += 1
                
                # Random validation based on fraction
                if validate_fraction > 0 and random.random() < validate_fraction:
                    validated_count += 1
                    if validate_path(file_path, dataset):
                        valid_count += 1
                    else:
                        invalid_count += 1
                        if invalid_count <= 10:  # Log first 10 invalid paths
                            logger.warning(f"Invalid path: /mnt/nas/{dataset}/{file_path}")
                        continue
                else:
                    valid_count += 1
                
                # Escape special characters for COPY
                escaped_path = file_path.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                buffer.write(f"{escaped_path}\t{dataset}\n")
                buffer_count += 1
                
                # Flush buffer periodically
                if buffer_count >= BATCH_SIZE:
                    buffer.seek(0)
                    with cursor.copy("COPY files (file_path, dataset) FROM STDIN") as copy:
                        copy.write(buffer.read())
                    conn.commit()
                    logger.info(f"Inserted batch of {buffer_count} records (total: {valid_count})")
                    buffer = StringIO()
                    buffer_count = 0
            
            # Insert remaining records
            if buffer_count > 0:
                buffer.seek(0)
                with cursor.copy("COPY files (file_path, dataset) FROM STDIN") as copy:
                    copy.write(buffer.read())
                conn.commit()
                logger.info(f"Inserted final batch of {buffer_count} records")
        
        logger.info(f"Completed import of {dataset} dataset:")
        logger.info(f"  Total paths: {total_count}")
        logger.info(f"  Inserted paths: {valid_count}")
        if validate_fraction > 0:
            logger.info(f"  Validated: {validated_count} ({validated_count/total_count*100:.1f}%)")
            logger.info(f"  Invalid paths found: {invalid_count}")
            
    except Exception as e:
        logger.error(f"Error importing {dataset}: {e}")
        conn.rollback()
        raise


app = typer.Typer()


@app.command()
def import_files(
    dataset: Annotated[str, typer.Option(help="Which dataset to import")] = "all",
    validate: Annotated[float, typer.Option(help="Fraction of paths to validate (0.0-1.0)", min=0.0, max=1.0)] = 0.05,
    dbname: Annotated[str, typer.Option(help="Database name")] = "zfs_to_s3",
    user: Annotated[str, typer.Option(help="Database user")] = "pball",
    host: Annotated[str, typer.Option(help="Database host")] = "localhost",
    no_clean: Annotated[bool, typer.Option(help="Don't delete existing records before import")] = False
):
    """Import file lists into PostgreSQL database."""
    # Validate dataset choice
    valid_datasets = ['backup', 'legacy', 'working', 'all']
    if dataset not in valid_datasets:
        logger.error(f"Invalid dataset: {dataset}. Must be one of: {', '.join(valid_datasets)}")
        raise typer.Exit(1)
    
    # Connect to database
    try:
        conn = psycopg.connect(
            dbname=dbname,
            user=user,
            host=host
        )
        logger.info(f"Connected to database {dbname}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise typer.Exit(1)
    
    try:
        if dataset == 'all':
            for ds, filepath in FILE_LISTS.items():
                if os.path.exists(filepath):
                    import_dataset_with_copy(conn, ds, filepath, validate, clean_first=not no_clean)
                else:
                    logger.warning(f"File not found: {filepath}")
        else:
            filepath = FILE_LISTS[dataset]
            if os.path.exists(filepath):
                import_dataset_with_copy(conn, dataset, filepath, validate, clean_first=not no_clean)
            else:
                logger.error(f"File not found: {filepath}")
                raise typer.Exit(1)
                
    finally:
        conn.close()
        logger.info("Database connection closed")


if __name__ == '__main__':
    app()