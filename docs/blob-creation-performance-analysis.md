<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# Blob Creation Performance Analysis

## Overview

This document analyzes the performance characteristics of n2s blob creation, documenting the optimized workflow and performance benchmarks for our production-ready implementation.

## Our Blob Creation Approach

### Workflow Pipeline

```
Original File → LZ4 Compress → ChaCha20-Poly1305 Encrypt → Base64 Encode → JSON with Metadata → Write Blob
```

**Reference implementation**: [scripts/blob_test.py](../scripts/blob_test.py)

### Implementation Details

1. **File Reading**: Read entire file content into memory
2. **LZ4 Compression**: Compress file content using LZ4 frame format  
3. **Blobid Generation**: `BLAKE3(path:file_hash)` creates deterministic identifier (path-aware blob ID)
4. **Key Derivation**: PBKDF2-HMAC-SHA256 with salt from blobid (100k iterations)
5. **ChaCha20-Poly1305 Encryption**: Encrypt compressed content with deterministic nonce from blobid
6. **Base64 Encoding**: Convert encrypted bytes to ASCII string
7. **JSON Structure**: Combine encrypted content with plaintext metadata
8. **Blob Writing**: Write JSON to file named with blobid (`BLAKE3(path:file_hash)`)

### Blob Structure

```json
{
  "encrypted_content": "base64_encoded_encrypted_compressed_data",
  "metadata": {
    "path": "relative/path/to/file.txt",
    "size": 12345,
    "timestamp": 1749388804.3256009,
    "file_hash": "blake3_hash_of_original_content"
  }
}
```

**Key Design Decisions:**
- **Path-aware blob IDs**: `BLAKE3(path:file_hash)` ensures same content at different paths creates different blobs
- **Plaintext metadata**: Enables disaster recovery without decryption
- **Encrypted content only**: Smaller encryption payload, better performance
- **Base64 encoding**: JSON compatibility with acceptable 33% overhead
- **Deterministic encryption**: Same file at same path always produces identical blob

## Performance Benchmarks

### Timing Breakdown (263 files, 25.1MB total)

| Operation | Avg Time/File | Percentage | Notes |
|-----------|---------------|------------|-------|
| Read | 0.0ms | <1% | File I/O negligible for small files |
| Compress | 0.1ms | <1% | LZ4 extremely fast |
| Encrypt | 14.0ms | 82% | PBKDF2 key derivation dominates |
| JSON | 0.3ms | 2% | JSON serialization minimal |
| Write | 0.1ms | <1% | Blob output negligible |
| **Total** | **17.0ms** | **100%** | |

### Aggregate Performance

- **Processing Rate**: 68.4 files/second
- **Overall Throughput**: 6.5 MB/s (limited by many small files)  
- **Per-file Throughput**: 3.3 MB/s average
- **Compression Ratio**: 1.23 average (25.1MB → 27.9MB including metadata)

### Performance Characteristics

**CPU-Bound Workload:**
- PBKDF2 key derivation dominates (82% of processing time)
- Excellent parallelization potential (CPU-bound operations)
- ChaCha20-Poly1305 encryption itself is extremely fast (<1ms)

**Scaling Expectations:**
- Small files (<1MB): Fixed 14ms encryption overhead dominates
- Large files (>10MB): Improved throughput as encryption scales with content size
- Parallel workers: Near-linear scaling due to CPU-bound characteristics

## Deterministic Encryption Design

### Approach

Derive both PBKDF2 salt and ChaCha20 nonce deterministically from the blobid (`BLAKE3(path:file_hash)`):

```python
blob_bytes = bytes.fromhex(blobid)
salt = blob_bytes[:16]   # First 16 bytes for PBKDF2 salt
nonce = blob_bytes[-12:] # Last 12 bytes for ChaCha20 nonce
```

### Security Properties

- **Salt uniqueness**: Each unique `path:file_hash` produces unique salt
- **Nonce safety**: Same content uses same nonce, but different derived keys make this secure
- **Key strength**: 100k PBKDF2 iterations provide computational cost against brute force
- **Content privacy**: File content encrypted; only metadata visible

### Benefits

- **Deterministic blobs**: Same file content at same path always produces identical encrypted blob
- **Path-aware deduplication**: Different paths create different blobs (`BLAKE3(path:file_hash)`)
- **Disaster recovery**: No random state needed for blob recreation

## Testing Methodology

### Test Environment
- **Platform**: Linux development environment
- **Sample**: 263 files totaling 25.1MB (mixed content types and sizes)
- **Measurement**: `time.perf_counter()` for high-resolution timing
- **Validation**: Encrypt/decrypt roundtrip verification, deterministic blob creation

### Metrics Collected
- Per-operation timing breakdown
- File-by-file processing statistics  
- Aggregate performance metrics
- Compression ratios and blob sizes

## Alternatives We Rejected

### Age Encryption

**Performance issues:**
- ~1200ms per file (1000x+ slower than ChaCha20-Poly1305)
- Constant overhead regardless of file size
- Only 0.8 MB/s throughput vs 28-41 MB/s with AEAD ciphers

**Why rejected**: Completely unusable for production workloads

### AES-GCM

**Performance**: Similar to ChaCha20-Poly1305 (~14ms per file)

**Why we chose ChaCha20-Poly1305 instead**:
- Slightly better software performance on some platforms
- More resistant to timing attacks
- Simpler constant-time implementation

### Blob Structure Alternatives

**Fully encrypted JSON blob**:
- **Problem**: Required decryption to access any metadata
- **Problem**: Larger encryption payload
- **Problem**: No disaster recovery without decryption key

**Separate binary files**:
- **Problem**: Two files per blob increases complexity
- **Problem**: Harder to ensure atomic operations
- **Problem**: More complex disaster recovery procedures

**Binary formats (MessagePack)**:
- **Problem**: Loses human-readable metadata benefit
- **Problem**: Additional parsing dependencies
- **Problem**: Harder debugging and operational visibility

**Hex encoding instead of Base64**:
- **Problem**: 100% size overhead vs 33% for Base64
- **Problem**: No performance benefit
- **Problem**: Larger blob files

## Conclusions

1. **ChaCha20-Poly1305 delivers production-ready performance**: 68+ files/sec processing rate
2. **CPU-bound workload enables excellent parallelization**: PBKDF2 dominates timing
3. **Deterministic encryption works reliably**: Consistent blob creation without security compromise
4. **Plaintext metadata provides operational benefits**: Disaster recovery and debugging without decryption
5. **LZ4 compression is negligible overhead**: <1% of processing time with good compression ratios

## Future Optimization Opportunities

1. **Parallel worker scaling**: Test concurrent processing with 8-16 workers
2. **Large file performance**: Benchmark with 100MB+ files to identify I/O bottlenecks  
3. **Memory optimization**: Profile memory usage patterns for large-scale processing
4. **Storage impact**: Compare performance across SSD, HDD, and network storage
5. **Algorithm tuning**: Evaluate PBKDF2 iteration count vs security/performance trade-off