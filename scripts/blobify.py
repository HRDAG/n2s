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


def create_blob(file_path: str, output_dir: str = "/tmp") -> str:
    """
    Create blob from file: read → hash → compress → encode → JSON wrap → write.

    Args:
        file_path: Path to source file
        output_dir: Directory to write blob file

    Returns:
        blobid (hex string)
    """
    # Read file content
    with open(file_path, 'rb') as f:
        file_content = f.read()

    # Generate blobid from content (pure deduplication)
    blobid = blake3.blake3(file_content).hexdigest()

    # Get file stats
    stat = os.stat(file_path)

    # Get file type from content
    filetype = get_filetype(file_content)

    # LZ4 compress content
    compressed = lz4.frame.compress(file_content)

    # Base64 encode compressed content
    content_b64 = base64.b64encode(compressed).decode('ascii')

    # Create JSON blob
    blob_data = {
        'content': content_b64,
        'metadata': {
            'size': stat.st_size,
            'mtime': stat.st_mtime,
            'filetype': filetype,
            'encryption': False
        }
    }

    # Write to output_dir/blobid
    dest_path = Path(output_dir) / blobid
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(dest_path, 'w') as f:
        json.dump(blob_data, f, indent=2)

    return blobid


def main(
    file_path: str = typer.Argument(..., help="Path to file to blobify"),
    output: str = typer.Option("/tmp", "--output", "-o", help="Output directory for blob")
):
    """Create a blob from a file and return its blobid."""

    blobid = create_blob(file_path, output)
    typer.echo(blobid)


if __name__ == "__main__":
    typer.run(main)
