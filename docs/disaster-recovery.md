# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# docs/disaster-recovery.md

# n2s Disaster Recovery

## Overview

The n2s simplified architecture is designed for robust disaster recovery with multiple failure scenarios covered. The key principle: **blobs contain complete recovery information** - you can rebuild the entire system from just the blob storage and encryption key.

## Recovery Scenarios

### Scenario 1: Lost Database, Have Blob Storage

**What happened**: Local `.n2s/<backend>-manifest.db` corrupted or deleted, but blob storage intact.

**Recovery approach**:
1. **Enumerate all blobs** in backend storage
2. **Decrypt each blob** to extract metadata (path, file_hash, size, mtime)
3. **Reconstruct database** by inserting discovered files
4. **Rebuild changesets** by grouping files logically

**Tools needed**:
- Backend access (S3 credentials, etc.)
- Encryption passphrase
- n2s recovery tool

**Example process**:
```bash
# List all blobs in backend
n2s recovery list-blobs --backend s3-prod

# Rebuild database from blob metadata
n2s recovery rebuild-db --backend s3-prod --passphrase-file key.txt
```

### Scenario 2: Lost Blob Storage, Have Database

**What happened**: Backend storage unavailable/corrupted, but local database intact.

**Recovery approach**:
1. **Query database** for all successfully uploaded files
2. **Re-read original files** from filesystem (if still available)
3. **Recreate blobs** using deterministic blob creation workflow
4. **Re-upload to new backend** or restored backend

**Requirements**:
- Original filesystem still accessible
- Database shows `upload_finish_tm IS NOT NULL` for files

**Example process**:
```bash
# Find all uploaded files in database
n2s recovery list-uploaded --database .n2s/s3-prod-manifest.db

# Recreate and re-upload blobs
n2s recovery restore-blobs --source-dir /data --backend s3-new
```

### Scenario 3: Lost Both Database and Blob Storage

**What happened**: Complete system failure - both local database and backend storage gone.

**Recovery approach**:
1. **Restore from filesystem backups** (ZFS snapshots, Git history, etc.)
2. **Re-run initial backup** to recreate both database and blob storage
3. **Verify integrity** by comparing file hashes

**Example process**:
```bash
# Restore filesystem from backup
zfs rollback tank/data@backup-2024-01-15

# Re-run complete backup
n2s backup --source /data --backend s3-prod --changeset "disaster-recovery"
```

### Scenario 4: Corrupted Individual Blobs

**What happened**: Some blobs corrupted in storage, database intact.

**Recovery approach**:
1. **Verify blob integrity** by downloading and checking file_hash
2. **Identify corrupted blobs** via hash mismatch
3. **Re-read source files** and recreate affected blobs
4. **Re-upload corrected blobs**

**Example process**:
```bash
# Verify all blobs against database
n2s recovery verify-integrity --backend s3-prod

# Recreate specific corrupted blobs  
n2s recovery fix-corrupted --file-id abc123def456 --source-dir /data
```

## Recovery Tools and Commands

### Core Recovery Operations

**Blob enumeration**:
```bash
n2s recovery list-blobs --backend <backend_name>
# Returns: file_id, path, size, mtime from blob metadata
```

**Database reconstruction**:
```bash
n2s recovery rebuild-db --backend <backend_name> --output <new_db_path>
# Scans all blobs, extracts metadata, rebuilds database
```

**Integrity verification**:
```bash
n2s recovery verify --backend <backend_name> --database <db_path>
# Downloads blobs, verifies file_hash matches, reports corruption
```

**Selective restoration**:
```bash
n2s recovery restore-files --pattern "*.py" --target-dir /recovered
# Restores specific files matching pattern from blob storage
```

### Manual Recovery (Command Line)

For situations where n2s tools aren't available:

**Decrypt single blob**:
```bash
# Download blob
aws s3 cp s3://bucket/abc123def456 blob.encrypted

# Decrypt (prompts for passphrase)  
age --decrypt blob.encrypted > blob.json

# Extract file content
jq -r '.content' blob.json | xxd -r -p > recovered_file.txt

# View metadata
jq '.metadata' blob.json
```

**Bulk recovery script**:
```bash
#!/bin/bash
# Recover all blobs to directory structure

mkdir -p recovered
aws s3 ls s3://bucket/ | while read blob_id; do
    aws s3 cp "s3://bucket/$blob_id" /tmp/blob
    metadata=$(age --decrypt /tmp/blob | jq -c '.metadata')
    filepath=$(echo "$metadata" | jq -r '.path')
    
    # Recreate directory structure  
    mkdir -p "recovered/$(dirname "$filepath")"
    
    # Extract file content
    age --decrypt /tmp/blob | jq -r '.content' | xxd -r -p > "recovered/$filepath"
done
```

## Recovery Architecture Benefits

### Self-Contained Blobs

**Each blob contains**:
- Original file content (compressed + encrypted)
- Complete metadata (path, size, mtime, file_hash)
- No external dependencies for decryption

**Recovery implications**:
- **No database required** for basic file recovery
- **Path reconstruction** possible from blob metadata alone
- **Integrity verification** via embedded file_hash

### Deterministic Blob Creation

**`file_id = BLAKE3(path:file_hash)`** enables:
- **Idempotent recreation**: Same file always produces same blob
- **Consistency verification**: Recreated blob matches original
- **Deduplication verification**: Same path+content = same file_id

### Backend Agnostic Recovery

**Storage independence**:
- Recovery tools work with any backend (S3, local, etc.)
- Blob format identical across all storage types
- Migration between backends preserves all data

## Recovery Testing Strategy

### Regular Verification

**Automated integrity checks**:
```bash
# Weekly cron job
n2s recovery verify --backend s3-prod --sample-rate 1% --report integrity.log
```

**Recovery drills**:
```bash  
# Monthly recovery test
n2s recovery restore-files --pattern "test/*" --target-dir /tmp/recovery-test
diff -r /data/test /tmp/recovery-test
```

### Disaster Recovery Runbook

1. **Assess damage**: Database vs storage vs both
2. **Identify scenario**: Map to one of the four scenarios above
3. **Execute recovery**: Use appropriate tools and procedures
4. **Verify integrity**: Check file hashes and completeness
5. **Resume operations**: Test normal backup/restore workflow

## Security Considerations

**Encryption key management**:
- Store passphrase separately from storage credentials
- Consider key rotation procedures for long-term archives
- Document key recovery procedures

**Access control**:
- Recovery tools require both storage access AND encryption key
- Separate permissions for recovery vs normal operations
- Audit trail for disaster recovery operations

The simplified architecture's design prioritizes recoverability - even in worst-case scenarios, data can be recovered with minimal tooling and clear procedures.