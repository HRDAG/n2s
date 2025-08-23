#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "lz4",
#   "blake3",
#   "typer",
# ]
# ///

# Author: PB and Claude
# Date: 2025-08-22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/deblobify.py

import base64
import json
import lz4.frame
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import blake3
import typer


def format_size(bytes_val: int) -> str:
    """Format bytes as human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f}{unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f}TB"


def format_timestamp(timestamp: float) -> str:
    """Format timestamp as readable date."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def restore_blob(blob_path: str, output_path: str, verify: bool = True) -> str:
    """
    Restore file from blob: read JSON → decode → decompress → write → verify.
    
    Args:
        blob_path: Path to blob file
        output_path: Where to write restored file
        verify: Whether to verify hash integrity
        
    Returns:
        Path to restored file
    """
    # Read JSON blob file
    with open(blob_path, 'r') as f:
        blob_data = json.load(f)
    
    content_b64 = blob_data['content']
    metadata = blob_data['metadata']
    
    # Use provided output path
    
    # Base64 decode content
    compressed_content = base64.b64decode(content_b64)
    
    # LZ4 decompress
    decompressed_content = lz4.frame.decompress(compressed_content)
    
    # Write restored file
    with open(output_path, 'wb') as f:
        f.write(decompressed_content)
    
    # Restore original mtime
    mtime = metadata['mtime']
    os.utime(output_path, (mtime, mtime))
    
    # Verify hash integrity
    if verify:
        actual_hash = blake3.blake3(decompressed_content).hexdigest()
        expected_hash = Path(blob_path).name  # blobid from filename
        
        if actual_hash != expected_hash:
            typer.echo(f"⚠ Hash mismatch! Expected: {expected_hash}, Got: {actual_hash}", err=True)
            raise typer.Exit(1)
    
    return output_path


def main(
    blob_path: str = typer.Argument(..., help="Path to blob file to restore"),
    output: str = typer.Option(..., "--output", "-o", help="Output path for restored file"),
    no_verify: bool = typer.Option(False, "--no-verify", help="Skip hash verification")
):
    """Restore a file from its blob representation. Requires --output path."""
    
    if not Path(blob_path).exists():
        typer.echo(f"Error: Blob file {blob_path} not found", err=True)
        raise typer.Exit(1)
    
    try:
        restored_path = restore_blob(blob_path, output, verify=not no_verify)
        
        # Get restored file info
        stat = os.stat(restored_path)
        size_str = format_size(stat.st_size)
        mtime_str = format_timestamp(stat.st_mtime)
        
        # Print summary
        typer.echo(f"Restored: {restored_path} ({size_str}, {mtime_str})")
        
        if not no_verify:
            blobid = Path(blob_path).name
            typer.echo(f"✓ Hash verified ({blobid[:16]}...)")
            
    except json.JSONDecodeError:
        typer.echo(f"Error: Invalid blob file format", err=True)
        raise typer.Exit(1)
    except lz4.frame.LZ4FrameError:
        typer.echo(f"Error: Failed to decompress blob content", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    typer.run(main)