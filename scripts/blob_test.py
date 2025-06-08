# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/blob_test.py

import base64
import csv
import json
import lz4.frame
import os
import time
from pathlib import Path
from typing import Dict, Any, List

import blake3
import typer
from cryptography.hazmat.primitives.ciphers.aead import AESGCM, ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

PASSPHRASE: str = "123456"


def encrypt_AESGCM(data: bytes, password: str, blobid: str) -> str:
    """Encrypt file content with AES-GCM and return base64 encoded string."""
    # blobid is already a hexdigest, convert to bytes
    blob_bytes = bytes.fromhex(blobid)
    
    salt = blob_bytes[:16]   # First 16 bytes for salt
    nonce = blob_bytes[-12:] # Last 12 bytes for nonce
    
    # Derive key
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    key = kdf.derive(password.encode())
    
    aesgcm = AESGCM(key)
    encrypted_data = aesgcm.encrypt(nonce, data, None)
    
    # Return base64 encoded string
    return base64.b64encode(encrypted_data).decode('ascii')


def encrypt_chacha(data: bytes, password: str, blobid: str) -> str:
    """Encrypt file content with ChaCha20-Poly1305 and return base64 encoded string."""
    # blobid is already a hexdigest, convert to bytes
    blob_bytes = bytes.fromhex(blobid)
    
    salt = blob_bytes[:16]   # First 16 bytes for salt
    nonce = blob_bytes[-12:] # Last 12 bytes for nonce
    
    # Derive key
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    key = kdf.derive(password.encode())
    
    chacha = ChaCha20Poly1305(key)
    encrypted_data = chacha.encrypt(nonce, data, None)
    
    # Return base64 encoded string
    return base64.b64encode(encrypted_data).decode('ascii')


def create_blob(
    file_path: str, metadata: Dict[str, Any], dest_dir: str, algorithm: str = "aesgcm"
) -> tuple[str, float, Dict[str, float]]:
    """
    Create blob from file: read → lz4 compress → encrypt → JSON wrap.
    
    Args:
        file_path: Path to source file
        metadata: Dict with path, size, timestamp, file_hash
        dest_dir: Directory to write blob file
        algorithm: Encryption algorithm ("aesgcm" or "chacha")
        
    Returns:
        (blobid, total_time, timing_breakdown)
    """
    timings = {}
    start_total = time.perf_counter()
    
    # Read file
    start = time.perf_counter()
    with open(file_path, 'rb') as f:
        file_content = f.read()
    timings['read'] = time.perf_counter() - start
    
    # LZ4 compress
    start = time.perf_counter()
    compressed = lz4.frame.compress(file_content)
    timings['compress'] = time.perf_counter() - start
    
    # Generate blobid first
    start = time.perf_counter()
    blobid = blake3.blake3(
        f"{metadata['path']}:{metadata['file_hash']}".encode()
    ).hexdigest()
    timings['blobid'] = time.perf_counter() - start
    
    # Encrypt compressed content only
    start = time.perf_counter()
    if algorithm == "aesgcm":
        encrypted_content_b64 = encrypt_AESGCM(compressed, PASSPHRASE, blobid)
    elif algorithm == "chacha":
        encrypted_content_b64 = encrypt_chacha(compressed, PASSPHRASE, blobid)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    timings['encrypt'] = time.perf_counter() - start
    
    # Create JSON blob with plaintext metadata
    start = time.perf_counter()
    blob_data = {
        'encrypted_content': encrypted_content_b64,
        'metadata': metadata
    }
    json_bytes = json.dumps(blob_data, indent=2).encode('utf-8')
    timings['json'] = time.perf_counter() - start
    
    # Write to dest_dir/blobid
    start = time.perf_counter()
    dest_path = Path(dest_dir) / blobid
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, 'wb') as f:
        f.write(json_bytes)
    timings['write'] = time.perf_counter() - start
    
    total_time = time.perf_counter() - start_total
    
    return blobid, total_time, timings


def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """Extract metadata from file."""
    stat = os.stat(file_path)
    
    # Calculate file hash
    with open(file_path, 'rb') as f:
        file_hash = blake3.blake3(f.read()).hexdigest()
    
    return {
        'path': file_path,
        'size': stat.st_size,
        'timestamp': stat.st_mtime,
        'file_hash': file_hash
    }


def format_size(bytes_val: int) -> str:
    """Format bytes as human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f}{unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f}TB"


def format_throughput(size_bytes: int, time_seconds: float) -> str:
    """Format throughput as MB/s."""
    if time_seconds == 0:
        return "∞ MB/s"
    mb_per_sec = (size_bytes / 1024 / 1024) / time_seconds
    return f"{mb_per_sec:.1f} MB/s"


def process_files(
    file_paths: List[str], dest_dir: str, csv_output: str, algorithm: str = "aesgcm", verbose: bool = False
) -> None:
    """Process multiple files and write results to CSV."""
    
    results = []
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            typer.echo(f"Warning: File {file_path} not found", err=True)
            continue
        
        if verbose:
            typer.echo(f"Processing: {file_path}")
        
        # Get file metadata
        metadata = get_file_metadata(file_path)
        original_size = metadata['size']
        
        # Create blob
        blobid, total_time, timings = create_blob(
            file_path, metadata, dest_dir, algorithm
        )
        
        # Get blob size
        blob_path = Path(dest_dir) / blobid
        blob_size = blob_path.stat().st_size
        
        # Calculate metrics
        compression_ratio = blob_size / original_size if original_size > 0 else 0
        throughput = (original_size / 1024 / 1024) / total_time if total_time > 0 else 0
        
        # Store results
        result = {
            'file_path': file_path,
            'blobid': blobid,
            'original_size': original_size,
            'blob_size': blob_size,
            'compression_ratio': compression_ratio,
            'total_time': total_time,
            'throughput_mbps': throughput,
            'read_time': timings.get('read', 0),
            'compress_time': timings.get('compress', 0),
            'json_time': timings.get('json', 0),
            'encrypt_time': timings.get('encrypt', 0),
            'blobid_time': timings.get('blobid', 0),
            'write_time': timings.get('write', 0)
        }
        results.append(result)
        
        if verbose:
            typer.echo(f"  ✓ {format_size(original_size)} → {format_size(blob_size)} "
                      f"({compression_ratio:.2f}) in {total_time:.3f}s")
    
    # Write CSV
    if results:
        with open(csv_output, 'w', newline='') as csvfile:
            fieldnames = [
                'file_path', 'blobid', 'original_size', 'blob_size', 
                'compression_ratio', 'total_time', 'throughput_mbps',
                'read_time', 'compress_time', 'json_time', 
                'encrypt_time', 'blobid_time', 'write_time'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        typer.echo(f"✓ Results written to {csv_output}")
        
        # Print summary
        print_summary(results)


def print_summary(results: List[Dict[str, Any]]) -> None:
    """Print performance summary from results."""
    if not results:
        return
    
    # Calculate totals and averages
    total_files = len(results)
    total_original_size = sum(r['original_size'] for r in results)
    total_blob_size = sum(r['blob_size'] for r in results)
    total_time = sum(r['total_time'] for r in results)
    
    # Calculate averages
    avg_compression_ratio = sum(r['compression_ratio'] for r in results) / total_files
    avg_throughput = sum(r['throughput_mbps'] for r in results) / total_files
    files_per_sec = total_files / total_time if total_time > 0 else 0
    overall_throughput = (total_original_size / 1024 / 1024) / total_time if total_time > 0 else 0
    
    # Timing breakdown averages
    avg_read = sum(r['read_time'] for r in results) / total_files * 1000  # ms
    avg_compress = sum(r['compress_time'] for r in results) / total_files * 1000
    avg_encrypt = sum(r['encrypt_time'] for r in results) / total_files * 1000
    avg_json = sum(r['json_time'] for r in results) / total_files * 1000
    avg_write = sum(r['write_time'] for r in results) / total_files * 1000
    
    # Print summary
    typer.echo("\n" + "="*50)
    typer.echo("PERFORMANCE SUMMARY")
    typer.echo("="*50)
    typer.echo(f"Files processed: {total_files}")
    typer.echo(f"Total data: {format_size(total_original_size)} → {format_size(total_blob_size)}")
    typer.echo(f"Avg compression ratio: {avg_compression_ratio:.2f}")
    typer.echo(f"Total time: {total_time:.3f}s")
    typer.echo(f"Files/sec: {files_per_sec:.1f}")
    typer.echo(f"Overall throughput: {overall_throughput:.1f} MB/s")
    typer.echo(f"Avg per-file throughput: {avg_throughput:.1f} MB/s")
    
    typer.echo(f"\nAvg timing breakdown (ms/file):")
    typer.echo(f"  Read:     {avg_read:.1f}")
    typer.echo(f"  Compress: {avg_compress:.1f}")
    typer.echo(f"  Encrypt:  {avg_encrypt:.1f}")
    typer.echo(f"  JSON:     {avg_json:.1f}")
    typer.echo(f"  Write:    {avg_write:.1f}")


def main(
    file_list: str = typer.Argument(..., help="File containing list of paths (one per line)"),
    dest_dir: str = typer.Option("./blobs", help="Output directory for blobs"),
    csv_output: str = typer.Option("blob_performance.csv", help="CSV output file"),
    algorithm: str = typer.Option("aesgcm", help="Encryption algorithm: aesgcm or chacha"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show per-file progress")
):
    """Test blob creation performance for files listed in a file."""
    
    if algorithm not in ["aesgcm", "chacha"]:
        typer.echo(f"Error: Invalid algorithm '{algorithm}'. Must be 'aesgcm' or 'chacha'", err=True)
        raise typer.Exit(1)
    
    if not os.path.exists(file_list):
        typer.echo(f"Error: File list {file_list} not found", err=True)
        raise typer.Exit(1)
    
    # Read file paths from file
    with open(file_list, 'r') as f:
        file_paths = [line.strip() for line in f if line.strip()]
    
    if not file_paths:
        typer.echo(f"Error: No file paths found in {file_list}", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"Found {len(file_paths)} files in {file_list}")
    typer.echo(f"Using {algorithm.upper()} encryption")
    process_files(file_paths, dest_dir, csv_output, algorithm, verbose)


if __name__ == "__main__":
    typer.run(main)