#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "lz4",
#   "blake3",
#   "typer",
#   "python-magic",
# ]
# ///

# Author: PB and Claude
# Date: 2025-08-22
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/blobify.py

import base64
import io
import json
import lz4.frame
import os
from pathlib import Path
from typing import Dict, Any

import blake3
import magic
import typer


def get_filetype(file_content: bytes) -> str:
    """Get file type using python-magic from content buffer."""
    try:
        return magic.from_buffer(file_content)
    except Exception:
        return "unknown"


# Configuration
CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks for reading file (each becomes one LZ4 frame)


def create_blob(file_path: Path, output_dir: str = "/tmp") -> str:
    """
    Create blob from file: read → hash → compress → encode → JSON wrap → write.

    Args:
        file_path: Path to source file
        output_dir: Directory to write blob file

    Returns:
        blobid (hex string)
    """
    # Get file stats
    stat = os.stat(file_path)
    
    # Single pass: hash, compress, and stream to temporary file
    import tempfile
    temp_fd, temp_path = tempfile.mkstemp(dir=output_dir, suffix='.tmp')
    
    try:
        with os.fdopen(temp_fd, 'w') as out_file:
            # Write JSON header with multi-frame content structure
            out_file.write('{\n  "content": {\n    "encoding": "lz4-multiframe",\n    "frames": [\n')
            
            # Stream process file in single pass - each chunk becomes independent LZ4 frame
            with open(file_path, 'rb') as f:
                hasher = blake3.blake3()
                first_chunk = True
                filetype = "unknown"
                frame_count = 0
                
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                        
                    # Use first chunk for magic detection
                    if first_chunk:
                        filetype = get_filetype(chunk)
                        first_chunk = False
                        
                    # Update hash
                    hasher.update(chunk)
                    
                    # Compress each chunk as independent LZ4 frame
                    compressed_frame = lz4.frame.compress(chunk)
                    
                    # Base64 encode frame and write to JSON
                    b64_frame = base64.b64encode(compressed_frame).decode('ascii')
                    
                    if frame_count > 0:
                        out_file.write(',\n')
                    out_file.write(f'      "{b64_frame}"')
                    frame_count += 1
                
                # Generate blobid
                blobid = hasher.hexdigest()
            
            # Write JSON footer
            out_file.write('\n    ]\n  },\n  "metadata": {\n')
            out_file.write(f'    "size": {stat.st_size},\n')
            out_file.write(f'    "mtime": {stat.st_mtime},\n')
            out_file.write(f'    "filetype": "{filetype}",\n')
            out_file.write('    "encryption": false\n')
            out_file.write('  }\n}')
        
        # Move temp file to final destination
        dest_path = Path(output_dir) / blobid
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        Path(temp_path).rename(dest_path)
        
    except Exception:
        # Clean up temp file on error
        Path(temp_path).unlink(missing_ok=True)
        raise

    return blobid


def main(
    file_path: str = typer.Argument(..., help="Path to file to blobify"),
    output: str = typer.Option("/tmp", "--output", "-o", help="Output directory for blob")
):
    """Create a blob from a file and return its blobid."""

    full_path = Path(file_path)
    assert full_path.exists()
    blobid = create_blob(full_path, output)
    typer.echo(blobid)


if __name__ == "__main__":
    typer.run(main)
