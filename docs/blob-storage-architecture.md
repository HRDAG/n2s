# Content-Addressable Backup System with Embedded Metadata

## Overview

This backup system uses content-addressable storage with embedded metadata for disaster recovery. Files are identified by their BLAKE3 hash, encrypted using age format, and stored across multiple backends (S3, IPFS) with complete metadata embedded in each encrypted blob.

## Key Design Principles

1. **Content-Addressable**: Files identified by BLAKE3 hash of original content
2. **Storage Agnostic**: Works with any blob storage backend
3. **Embedded Metadata**: All recovery information stored within encrypted blobs
4. **Disaster Recovery**: Complete system rebuild possible with just encryption key and storage credentials
5. **Deduplication**: Identical files stored only once regardless of path

## Architecture

```
Original File → BLAKE3 Hash → JSON Blob → Encrypted Blob → Storage Backends
                    ↓              ↓              ↓
               Content ID    + Metadata    Age Encryption
```

### Storage Layers

- **PostgreSQL**: Fast metadata queries and file mappings
- **S3**: Blob storage using content hash as key
- **IPFS**: Distributed storage with CID mapping
- **Encrypted Blobs**: Self-contained with embedded metadata

## Encrypted Blob Structure

Each encrypted blob contains a JSON structure:

```json
{
  "content": "48656c6c6f20576f726c6421",
  "metadata": {
    "filepath": "/home/user/documents/report.pdf",
    "dataset": "documents",
    "snapshot": "2024-01-15T10:00:00Z",
    "mtime": "2024-01-15T09:30:00Z",
    "size": 12345,
    "content_hash": "a1b2c3d4e5f6789...",
    "created_at": "2024-01-15T11:00:00Z"
  }
}
```

## Database Schema

The database separates blob storage tracking from file path mappings to enable deduplication:

- **blobs table**: One record per unique content hash (deduplicated storage)
- **files table**: One record per file path in each dataset/snapshot (many-to-one with blobs)
- **Relationship**: Multiple files can reference the same blob via `content_hash`

```sql
-- Blob storage tracking (deduplicated by content)
CREATE TABLE blobs (
    content_hash TEXT PRIMARY KEY,              -- BLAKE3 hash of original content
    size BIGINT NOT NULL,                       -- Original file size in bytes
    encrypted_size BIGINT NOT NULL,             -- Encrypted blob size in bytes
    s3_key TEXT,                               -- S3 object key (same as content_hash)
    ipfs_cid TEXT,                             -- IPFS Content ID for encrypted blob
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- File path mappings (one per file path per snapshot)
CREATE TABLE files (
    dataset TEXT NOT NULL,                     -- Dataset name (e.g., "documents")
    snapshot TEXT NOT NULL,                    -- Snapshot identifier (e.g., timestamp)
    filepath TEXT NOT NULL,                    -- Original file path
    content_hash TEXT NOT NULL,                -- References blobs.content_hash
    mtime TIMESTAMP WITH TIME ZONE NOT NULL,   -- File modification time
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (dataset, snapshot, filepath),
    FOREIGN KEY (content_hash) REFERENCES blobs(content_hash)
);

-- Indexes for common queries
CREATE INDEX idx_files_content_hash ON files(content_hash);
CREATE INDEX idx_files_dataset_snapshot ON files(dataset, snapshot);
CREATE INDEX idx_blobs_first_seen ON blobs(first_seen);
```

### Deduplication Example

```sql
-- Single blob referenced by multiple file paths
INSERT INTO blobs VALUES ('abc123...', 1024, 1200, 'abc123...', 'QmXYZ...', NOW());

-- Multiple files with same content (e.g., copied/moved files)
INSERT INTO files VALUES ('docs', '2024-01-15', '/home/user/report.pdf', 'abc123...', '2024-01-15 09:00:00+00');
INSERT INTO files VALUES ('docs', '2024-01-15', '/backup/report.pdf', 'abc123...', '2024-01-15 09:00:00+00');  
INSERT INTO files VALUES ('docs', '2024-01-16', '/home/user/report.pdf', 'abc123...', '2024-01-15 09:00:00+00');

-- Result: One blob stored, three file path references
```

## Python Implementation

### Dependencies

```bash
pip install blake3 pyrage psycopg
```

### Minimal Example

```python
import json
import blake3
import psycopg
from pyrage import encrypt, decrypt

# Create encrypted blob
def create_blob(file_content: bytes, filepath: str, age_recipients: list) -> tuple[str, bytes]:
    content_hash = blake3.blake3(file_content).hexdigest()
    blob_data = {
        'content': file_content.hex(),
        'metadata': {'filepath': filepath, 'content_hash': content_hash}
    }
    json_bytes = json.dumps(blob_data).encode('utf-8')
    encrypted_blob = encrypt(json_bytes, age_recipients)
    return content_hash, encrypted_blob

# Extract from encrypted blob  
def extract_blob(encrypted_blob: bytes, identity: str) -> tuple[bytes, dict]:
    decrypted = decrypt(encrypted_blob, [identity])
    blob_data = json.loads(decrypted.decode('utf-8'))
    file_content = bytes.fromhex(blob_data['content'])
    return file_content, blob_data['metadata']
```

## Command Line Recovery Examples

### Prerequisites

- Age encryption tool: `age`
- JSON processor: `jq`
- Hex decoder: `xxd`

### Basic Recovery Process

```bash
# 1. List all blobs in storage
aws s3 ls s3://backup-bucket/ --recursive | awk '{print $4}'

# 2. Decrypt a specific blob
age --decrypt --identity key.txt --armor < blob_file.age | jq '.'

# 3. Extract file content and save to original filename
FILENAME=$(age --decrypt --identity key.txt blob_file.age | jq -r '.metadata.filepath | split("/") | .[-1]')
age --decrypt --identity key.txt blob_file.age | jq -r '.content' | xxd -r -p > "$FILENAME"

# 4. View metadata only
age --decrypt --identity key.txt blob_file.age | jq '.metadata'
```

### Full Recovery Script

```bash
#!/bin/bash
# disaster_recovery.sh - Recover all files from backup storage

IDENTITY_KEY="key.txt"
STORAGE_PREFIX="s3://backup-bucket/"
OUTPUT_DIR="./recovered_files"

mkdir -p "$OUTPUT_DIR"

# Function to recover a single blob
recover_blob() {
    local blob_hash=$1
    echo "Recovering blob: $blob_hash"
    
    # Download blob
    aws s3 cp "${STORAGE_PREFIX}${blob_hash}" /tmp/blob.age
    
    # Decrypt and extract metadata
    local metadata=$(age --decrypt --identity "$IDENTITY_KEY" /tmp/blob.age | jq -c '.metadata')
    local filepath=$(echo "$metadata" | jq -r '.filepath')
    local dataset=$(echo "$metadata" | jq -r '.dataset')
    local snapshot=$(echo "$metadata" | jq -r '.snapshot')
    
    # Create output directory structure
    local output_path="${OUTPUT_DIR}/${dataset}/${snapshot}${filepath}"
    mkdir -p "$(dirname "$output_path")"
    
    # Extract file content
    age --decrypt --identity "$IDENTITY_KEY" /tmp/blob.age | \
        jq -r '.content' | xxd -r -p > "$output_path"
    
    echo "Recovered: $output_path"
}

# Recover all blobs
aws s3 ls "$STORAGE_PREFIX" | awk '{print $4}' | while read blob_hash; do
    recover_blob "$blob_hash"
done

echo "Recovery complete. Files restored to: $OUTPUT_DIR"
```

### Manual Recovery Steps

```bash
# 1. Download a blob from S3
aws s3 cp s3://backup-bucket/a1b2c3d4e5f6789... ./blob.age

# 2. Decrypt the blob
age --decrypt --identity key.txt ./blob.age > decrypted.json

# 3. View the structure
cat decrypted.json | jq '.'

# 4. Extract original file path
cat decrypted.json | jq -r '.metadata.filepath'
# Output: /home/user/documents/report.pdf

# 5. Extract file content
cat decrypted.json | jq -r '.content' | xxd -r -p > recovered_report.pdf

# 6. Verify integrity
sha256sum recovered_report.pdf
blake3sum recovered_report.pdf  # Should match metadata.content_hash
```

## Disaster Recovery Scenarios

### Scenario 1: Lost Database, Have Storage

```bash
# Rebuild database from all stored blobs
python3 backup_system.py disaster-recovery --identity key.txt
```

### Scenario 2: Lost Storage, Have Database

```bash
# Re-upload all files using database mappings
python3 backup_system.py restore-from-filesystem --dataset documents --snapshot latest
```

### Scenario 3: Lost Everything, Have ZFS Snapshots

```bash
# 1. Restore from ZFS snapshot
zfs rollback tank/data@backup-2024-01-15

# 2. Re-run backup to rebuild storage
python3 backup_system.py backup-dataset --dataset documents --snapshot recovery
```

### Scenario 4: Complete Manual Recovery

With only the age identity key and storage credentials:

```bash
# 1. List all blobs
aws s3 ls s3://backup-bucket/ --recursive > blob_list.txt

# 2. Create recovery script
cat > recover_all.sh << 'EOF'
#!/bin/bash
while read -r line; do
    blob=$(echo "$line" | awk '{print $4}')
    aws s3 cp "s3://backup-bucket/$blob" "/tmp/$blob"
    
    metadata=$(age --decrypt --identity key.txt "/tmp/$blob" | jq -c '.metadata')
    filepath=$(echo "$metadata" | jq -r '.filepath')
    dataset=$(echo "$metadata" | jq -r '.dataset')
    snapshot=$(echo "$metadata" | jq -r '.snapshot')
    
    output_dir="./recovered/${dataset}/${snapshot}"
    mkdir -p "$(dirname "${output_dir}${filepath}")"
    
    age --decrypt --identity key.txt "/tmp/$blob" | \
        jq -r '.content' | xxd -r -p > "${output_dir}${filepath}"
    
    echo "Recovered: ${output_dir}${filepath}"
done < blob_list.txt
EOF

chmod +x recover_all.sh
./recover_all.sh
```

## Benefits

1. **Storage Agnostic**: Works with any blob storage backend
2. **Complete Recovery**: Rebuild entire system from just blobs + key
3. **No Metadata Dependency**: S3 metadata not required for recovery
4. **Cross-Provider Migration**: Move between storage providers easily
5. **Deduplication**: Identical content stored only once
6. **Integrity Verification**: Content hash verification built-in
7. **Command Line Recovery**: No specialized tools required for disaster recovery

## Security Considerations

- Age encryption provides strong protection for blob contents
- Content hashes are not sensitive (they're deterministic)
- Filepath information is encrypted within blobs
- Recovery requires both storage access AND encryption key
- Consider using multiple age recipients for key redundancy

## Performance Characteristics

- **Deduplication**: O(1) lookup by content hash
- **Storage Overhead**: ~100 bytes metadata per file
- **Recovery Speed**: Limited by storage bandwidth and decryption
- **Scalability**: Linear with number of unique files (not total files)
