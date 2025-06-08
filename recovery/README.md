# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# recovery/README.md

# n2s Disaster Recovery Tools

This directory contains platform-independent disaster recovery tools for n2s blob storage.

## Overview

The recovery tools enable complete data recovery using only:
- Blob storage access (via `rget` command)
- Encryption passphrase
- Standard Unix tools (`jq`, `lz4`)

## Files

- `decrypt.go` - Go source for decrypt tool
- `go.mod` - Go module dependencies
- `build.sh` - Build script for all platforms
- `disaster_recovery.sh` - Recovery script
- `bin/` - Pre-built binaries (created by build.sh)

## Quick Start

### 1. Build Recovery Tools

**Note**: Currently binaries must be built locally. In the future, we may include pre-built binaries in the repository for true zero-dependency disaster recovery.

```bash
cd recovery
./build.sh
```

This creates binaries in `bin/` for all supported platforms:
- `decrypt-linux-amd64`
- `decrypt-linux-arm64` 
- `decrypt-windows-amd64.exe`
- `decrypt-macos-amd64`
- `decrypt-macos-arm64`

### 2. Recover Single Blob

```bash
# Recover to file
./disaster_recovery.sh a1b2c3d4e5f6... mypassword recovered_file.txt

# Recover to stdout
./disaster_recovery.sh a1b2c3d4e5f6... mypassword
```

### 3. Manual Recovery

For advanced users or when scripts aren't available:

```bash
# 1. Get blob
rget a1b2c3d4e5f6... > blob.json

# 2. View metadata (no decryption needed)
jq '.metadata' blob.json

# 3. Decrypt content manually
jq -r '.encrypted_content' blob.json | \
  ./bin/decrypt-linux-amd64 a1b2c3d4e5f6... mypassword /dev/stdin | \
  lz4 -d > recovered_file.txt
```

## Dependencies

### Required
- `jq` - JSON processing
- `lz4` - Decompression  
- `rget` - Blob retrieval (implementation-specific)

### Platform Detection
The recovery script auto-detects platform and uses appropriate binary.

## Security

- Decrypt tool implements exact n2s crypto: PBKDF2 + ChaCha20-Poly1305
- Same deterministic salt/nonce derivation as blob creation
- No network dependencies - works offline with retrieved blobs
- Statically compiled binaries require no runtime dependencies

## Architecture

The recovery tools match the n2s blob format:

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

**Key insight**: Metadata is plaintext, so paths/sizes are visible without decryption. Only file content requires the passphrase.