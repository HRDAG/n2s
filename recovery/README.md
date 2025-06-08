<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.06.08
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# n2s Disaster Recovery

## Overview

The n2s simplified architecture enables robust disaster recovery with minimal dependencies. The key principle: **blobs contain complete recovery information** - you can rebuild files from just blob storage and encryption passphrase.

**Recovery capabilities:**
- Complete data recovery using only standard Unix tools + small Go binary
- No database required for basic file recovery  
- Platform-independent tools for any disaster scenario
- Verified working end-to-end recovery workflow

## Quick Start

### 1. Build Recovery Tools

```bash
cd recovery
./build.sh
```

Creates platform-specific binaries in `bin/`:
- `decrypt-linux-amd64`, `decrypt-linux-arm64`
- `decrypt-windows-amd64.exe`  
- `decrypt-macos-amd64`, `decrypt-macos-arm64`

**Note**: Binaries excluded from repo to reduce size. May include pre-built binaries in future for true zero-dependency recovery.

### 2. Test Recovery

```bash
# Test blob decryption with hash verification
./test_decrypt.sh /path/to/blob_file passphrase

# With verbose debugging  
./test_decrypt.sh --verbose /path/to/blob_file passphrase
```

### 3. Manual Recovery

```bash
# Get blob (however blobs are stored)
cp /storage/abc123def456 blob.json

# View metadata (no decryption needed)
jq '.metadata' blob.json

# Decrypt and recover file
BLOBID=$(basename blob.json)
ENCRYPTED=$(jq -r '.encrypted_content' blob.json | tr -d '\n\r ')
./bin/decrypt-linux-amd64 "$BLOBID" "passphrase" "$ENCRYPTED" | lz4 -d > recovered_file.txt
```

## Disaster Recovery Scenarios

### Scenario 1: Lost Database, Have Blob Storage

**What happened**: Local `.n2s/<backend>-manifest.db` corrupted, but blobs intact.

**Recovery process:**
1. **Enumerate blobs** from storage
2. **Extract metadata** from each blob (no decryption needed)
3. **Selectively recover** files as needed
4. **Rebuild database** if required

```bash
# List blobs and extract paths
for blob in /storage/*; do
    echo "=== $(basename $blob) ==="
    jq '.metadata | {path, size, timestamp}' "$blob"
done

# Recover specific files
./test_decrypt.sh blob_abc123def456 mypassword
```

### Scenario 2: Lost Blob Storage, Have Database  

**What happened**: Backend storage lost, database intact.

**Recovery process:**
1. **Query database** for successfully uploaded files
2. **Re-read original files** from filesystem  
3. **Recreate blobs** using deterministic blob creation
4. **Re-upload** to new/restored backend

```bash
# Find uploaded files
sqlite3 .n2s/backend-manifest.db \
  "SELECT path, file_id FROM files WHERE upload_finish_tm IS NOT NULL;"

# Re-run n2s backup to recreate blobs
n2s backup --source /data --backend new-backend
```

### Scenario 3: Lost Both Database and Storage

**What happened**: Complete system failure.

**Recovery process:**
1. **Restore from filesystem backups** (ZFS snapshots, Git history)  
2. **Re-run complete backup** to recreate everything
3. **Verify integrity** by comparing file hashes

```bash
# Restore filesystem
zfs rollback tank/data@backup-2025-01-15

# Re-run backup 
n2s backup --source /data --backend s3-prod --changeset "disaster-recovery"
```

### Scenario 4: Corrupted Individual Blobs

**What happened**: Some blobs corrupted in storage.

**Recovery process:**
1. **Test blob integrity** using recovery tools
2. **Identify corruption** via decrypt failures or hash mismatches  
3. **Re-read source files** and recreate affected blobs
4. **Replace corrupted blobs**

```bash
# Test blob integrity
./test_decrypt.sh suspect_blob_file passphrase
# Hash verification will catch corruption

# Recreate from source if available
n2s upload-file /original/path/file.txt --backend s3-prod
```

## Manual Recovery Procedures

### Single Blob Recovery

```bash
# 1. Extract metadata  
jq '.metadata' blob.json
# Shows: {"path": "docs/file.txt", "size": 1234, "timestamp": 1234567890, "file_hash": "abc123..."}

# 2. Decrypt content
BLOBID=$(basename blob.json)
ENCRYPTED=$(jq -r '.encrypted_content' blob.json | tr -d '\n\r ')  
FILENAME=$(jq -r '.metadata.path' blob.json | xargs basename)
MTIME=$(jq -r '.metadata.timestamp' blob.json)

# 3. Recover file with correct metadata
./bin/decrypt-linux-amd64 "$BLOBID" "$PASSPHRASE" "$ENCRYPTED" | lz4 -d > "$FILENAME"
touch -d "@$MTIME" "$FILENAME"

# 4. Verify integrity
EXPECTED=$(jq -r '.metadata.file_hash' blob.json)
ACTUAL=$(b3sum "$FILENAME" | cut -d' ' -f1)
[ "$EXPECTED" = "$ACTUAL" ] && echo "✓ Verified" || echo "✗ Corruption"
```

### Bulk Recovery Script

```bash
#!/bin/bash
# Recover all blobs in a directory

for blob in /storage/*; do
    echo "Processing $(basename $blob)..."
    
    # Extract metadata
    path=$(jq -r '.metadata.path' "$blob")
    filename=$(basename "$path")
    mtime=$(jq -r '.metadata.timestamp' "$blob")
    
    # Create output directory
    mkdir -p "recovered/$(dirname "$path")"
    
    # Decrypt and recover
    blobid=$(basename "$blob")
    encrypted=$(jq -r '.encrypted_content' "$blob" | tr -d '\n\r ')
    
    if ./bin/decrypt-linux-amd64 "$blobid" "$PASSPHRASE" "$encrypted" | lz4 -d > "recovered/$path"; then
        touch -d "@$mtime" "recovered/$path"
        echo "✓ Recovered: $path"
    else
        echo "✗ Failed: $path"
    fi
done
```

## Dependencies

**Required for disaster recovery:**
- `jq` - JSON processing  
- `lz4` - LZ4 decompression
- `b3sum` - BLAKE3 hash verification (Ubuntu package)
- `touch` - Set file timestamps
- **Decrypt binary** - ChaCha20 decryption utility (see Build section)

**For building decrypt binary:**
- Go 1.21+ compiler
- Internet access for Go module downloads

*Note: Once built, the decrypt binary is self-contained and requires no Go runtime.*

## Security Architecture

### Encryption Implementation

- **Algorithm**: ChaCha20-Poly1305 AEAD cipher
- **Key derivation**: PBKDF2-HMAC-SHA256 (100k iterations)
- **Salt/nonce**: Deterministic from `BLAKE3(path:file_hash)`
- **Base64 encoding**: For JSON compatibility

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

**Security properties:**
- **Metadata plaintext**: Paths/sizes visible without passphrase
- **Content encrypted**: File data requires passphrase + correct blob ID
- **Deterministic**: Same file always produces same blob
- **Tamper-evident**: Hash verification detects corruption

### Key Management

- **Passphrase storage**: Separate from storage credentials
- **Key rotation**: Consider for long-term archives  
- **Access control**: Recovery requires both storage access AND passphrase

## Files in This Directory

- `decrypt.go` - Go source for decrypt tool
- `go.mod` - Go module dependencies  
- `build.sh` - Build script for all platforms
- `test_decrypt.sh` - Single blob test with verification
- `disaster_recovery.sh` - Recovery script (needs `rget` command)
- `bin/` - Built binaries (created by build.sh, git-ignored)

## Design Benefits

1. **Minimal dependencies**: Standard Unix tools + small Go binary
2. **Self-contained blobs**: Complete recovery info in each blob
3. **No database required**: Metadata readable without decryption  
4. **Platform independent**: Works on any Unix-like system
5. **Deterministic**: Same files always produce same blobs
6. **Tamper-evident**: Hash verification built-in

The architecture prioritizes recoverability over storage efficiency - you can always get your data back with minimal tooling.