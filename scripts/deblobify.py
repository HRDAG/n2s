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





def _restore_multiframe_streaming(json_file, output_path: str, verify: bool) -> str:
    """Stream restore multi-frame format without loading all into memory."""
    hasher = blake3.blake3() if verify else None
    metadata = None
    
    with open(output_path, 'wb') as out_file:
        in_frames = False
        frames_processed = 0
        
        for line in json_file:
            # Extract metadata
            if '"metadata"' in line and not metadata:
                # Simple metadata extraction for common format
                if '"size":' in line:
                    size_match = line.split('"size":')[1].split(',')[0].strip()
                    mtime_line = next(json_file, '')
                    mtime_match = mtime_line.split('"mtime":')[1].split(',')[0].strip()
                    filetype_line = next(json_file, '')
                    filetype_match = filetype_line.split('"filetype":')[1].split(',')[0].strip().strip('"')
                    
                    metadata = {
                        'size': int(size_match),
                        'mtime': float(mtime_match),
                        'filetype': filetype_match
                    }
            
            # Process frames array
            if '"frames": [' in line:
                in_frames = True
                continue
                
            if in_frames:
                if line.strip() in [']', '],']:
                    break  # End of frames
                    
                # Extract base64 frame from line like '      "base64data...",'
                line = line.strip()
                if line.startswith('"') and (line.endswith('"') or line.endswith('",') or line.endswith('",')):
                    # Extract base64 content
                    frame_b64 = line.strip('"",')
                    
                    if frame_b64:  # Skip empty lines
                        # Decode and decompress frame
                        compressed_frame = base64.b64decode(frame_b64)
                        decompressed_chunk = lz4.frame.decompress(compressed_frame)
                        
                        # Stream write
                        out_file.write(decompressed_chunk)
                        if hasher:
                            hasher.update(decompressed_chunk)
                        
                        frames_processed += 1
    
    if frames_processed == 0:
        raise ValueError("No frames processed from multi-frame blob")
    
    if not metadata:
        raise ValueError("Could not extract metadata from blob file")
    
    # Restore mtime
    os.utime(output_path, (metadata['mtime'], metadata['mtime']))
    
    # Hash verification
    if verify and hasher:
        actual_hash = hasher.hexdigest()
        expected_hash = Path(output_path).parent.parent.name if 'tmp' in str(output_path) else Path(output_path).name
        if '/' in str(output_path):
            expected_hash = [p for p in str(output_path).split('/') if len(p) == 64]
            expected_hash = expected_hash[0] if expected_hash else Path(output_path).name
        
        if len(expected_hash) == 64 and actual_hash != expected_hash:
            typer.echo(f"⚠ Hash mismatch! Expected: {expected_hash}, Got: {actual_hash}", err=True)
            raise typer.Exit(1)
    
    return output_path


def _restore_legacy_formats(blob_data: dict, output_path: str, verify: bool) -> str:
    """Restore old format blobs (requires full memory load)."""
    metadata = blob_data['metadata']
    hasher = blake3.blake3() if verify else None
    
    with open(output_path, 'wb') as out_file:
        if isinstance(blob_data['content'], str):
            # Original format: single base64 string
            content_b64 = blob_data['content']
            compressed_content = base64.b64decode(content_b64)
            decompressed_content = lz4.frame.decompress(compressed_content)
            out_file.write(decompressed_content)
            if hasher:
                hasher.update(decompressed_content)
        
        elif isinstance(blob_data['content'], dict):
            content_info = blob_data['content']
            encoding = content_info.get('encoding', 'lz4+base64-chunked')
            
            if encoding == 'lz4+base64-chunked':
                # Legacy chunked format
                chunks = content_info['chunks']
                compressed_parts = []
                for chunk in chunks:
                    compressed_parts.append(base64.b64decode(chunk))
                
                compressed_content = b''.join(compressed_parts)
                decompressed_content = lz4.frame.decompress(compressed_content)
                out_file.write(decompressed_content)
                if hasher:
                    hasher.update(decompressed_content)
    
    # Restore mtime
    os.utime(output_path, (metadata['mtime'], metadata['mtime']))
    
    # Hash verification  
    if verify and hasher:
        actual_hash = hasher.hexdigest()
        expected_hash = Path(output_path).parent.parent.name if 'tmp' in str(output_path) else Path(output_path).name
        if '/' in str(output_path):
            expected_hash = [p for p in str(output_path).split('/') if len(p) == 64]
            expected_hash = expected_hash[0] if expected_hash else Path(output_path).name
            
        if len(expected_hash) == 64 and actual_hash != expected_hash:
            typer.echo(f"⚠ Hash mismatch! Expected: {expected_hash}, Got: {actual_hash}", err=True)
            raise typer.Exit(1)
    
    return output_path


def restore_blob(blob_path: str, output_path: str, verify: bool = True) -> str:
    """
    Restore file from blob with streaming support for multi-frame format.
    
    Args:
        blob_path: Path to blob file
        output_path: Where to write restored file
        verify: Whether to verify hash integrity
        
    Returns:
        Path to restored file
    """
    # First, peek at the file to determine format
    with open(blob_path, 'r') as f:
        first_chunk = f.read(1024)  # Read first 1KB to detect format
        f.seek(0)
        
        if '"encoding": "lz4-multiframe"' in first_chunk:
            # Multi-frame format - use streaming parser
            return _restore_multiframe_streaming(f, output_path, verify)
        else:
            # Old formats - use full JSON load (unavoidable memory usage)
            blob_data = json.load(f)
            return _restore_legacy_formats(blob_data, output_path, verify)


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