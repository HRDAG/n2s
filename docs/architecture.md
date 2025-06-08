<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.06.08
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# n2s Architecture

## Overview

n2s is a storage coordination service that connects version-based data sources with storage backends. It prioritizes operational reliability and disaster recovery over storage optimization, using a 2-table design with path-aware blob creation for deterministic operations.

**Core philosophy**: The architecture trades storage efficiency for operational reliability - you can always recover your data with minimal tooling.

## Design Principles

### 1. Separation of Concerns
- **Clients**: Make all policy decisions (what to store, when to backup, retention policies)  
- **n2s**: Provides reliable storage service with no policy decisions
- **Backends**: Handle actual byte storage (S3, IPFS, etc.)

### 2. Operational Simplicity
- **Single backend per database**: Eliminates complex coordination overhead
- **Path-aware blob creation**: Same content at different paths creates different blobs, enabling disaster recovery without database dependency
- **Local SQLite databases**: Travel with the data, no infrastructure requirements
- **Deterministic operations**: Identical content at same path always produces same blob ID

### 3. Zero Tolerance for Data Loss
- Complete audit trail through upload state management
- Robust retry logic for failed uploads
- Self-contained encrypted blobs with embedded metadata
- Disaster recovery possible from storage alone

## Component Architecture

### Client Layer
**Purpose**: File discovery and workflow management

**Client Types**:
- **DSG Client**: Data versioning system (Git-like for data) - primary HRDAG workflow tool
- **ZFS Client**: Uses `zfs diff` between snapshots for incremental backup
- **Btrfs Client**: Uses `btrfs subvolume find-new` for snapshot-based incremental backup  
- **Find Client**: Uses `find -mtime` for time-based file discovery

**Responsibilities**:
- Discover changed/new files using various strategies
- Provide dataset identification and metadata
- Handle user workflow and configuration
- Call n2s API for storage operations

### Frontend API Layer
**Purpose**: Clean interface for clients that want to store things

**Responsibilities**: 
- Accept storage requests from clients
- Validate input data and metadata
- Return storage confirmations and retrieval responses

**Interface**: RESTful API, gRPC, or library bindings

### Service Manager Layer
**Purpose**: Business logic and coordination

**Responsibilities**:
- Content hashing (BLAKE3) and path-aware blob creation  
- Encryption/decryption operations (ChaCha20-Poly1305)
- Changeset creation and management
- Single backend coordination
- Configuration management (credentials, backend address, encryption keys)

**State**: 
- Configuration and credentials (loaded at startup)
- Database connections and backend clients
- Minimal operational state for performance (connection pooling, caching)
- **Horizontally scalable**: Multiple Service Manager instances can operate on different changesets

### Database Layer
**Purpose**: Persistent storage of metadata and state

**Implementation**: SQLite databases located at `{n2sroot}/.n2s/{backend_name}-manifest.db`

**Benefits of local SQLite databases:**
- **Travels with data**: Database stays with the file tree being tracked  
- **Backend isolation**: Multiple backends can operate on same n2sroot
- **No infrastructure**: No PostgreSQL setup required
- **Multi-process safe**: SQLite WAL mode handles concurrent workers

**Responsibilities**:
- Store changeset and file metadata (2-table design)
- Track upload status with start/finish timestamps
- Provide ACID transactions for consistency

### Backend Provider Layer
**Purpose**: Single storage backend per database deployment for operational simplicity

**Supported Providers**:
- **S3**: Object storage with content-addressable keys
- **IPFS**: Distributed storage with CID mapping
- **rclone**: Any rclone-supported backend (Google Drive, Dropbox, etc.)
- **unixfs:local**: Local filesystem storage
- **unixfs:ssh**: Remote filesystem over SSH

## Database Schema (2-Table Design)

### 1. changesets - Changeset Tracking

```sql
CREATE TABLE changesets (
    changeset_id TEXT PRIMARY KEY,                  -- hash(name, sorted_file_list)
    name TEXT NOT NULL,                             -- Human-readable changeset name
    content_hash TEXT NOT NULL,                     -- hash(sorted_file_ids) - changeset content signature
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_count INTEGER DEFAULT 0,                   -- Number of files processed
    total_size INTEGER DEFAULT 0,                   -- Total original bytes
    status TEXT DEFAULT 'pending'                   -- pending, processing, completed, failed
);
```

### 2. files - File Records

```sql
CREATE TABLE files (
    path TEXT NOT NULL,                             -- Relative file path
    changeset_id TEXT NOT NULL,                     -- References changesets.changeset_id
    size INTEGER NOT NULL,                          -- Original file size
    mtime TIMESTAMP NOT NULL,                       -- File modification time  
    file_hash TEXT NOT NULL,                        -- BLAKE3 hash of original content
    file_id TEXT NOT NULL,                          -- hash(path:file_hash) - storage key
    upload_start_tm TIMESTAMP,                      -- When upload began
    upload_finish_tm TIMESTAMP,                     -- When upload completed
    
    PRIMARY KEY (path, changeset_id),
    FOREIGN KEY (changeset_id) REFERENCES changesets(changeset_id)
);

-- Indexes for performance
CREATE INDEX idx_files_upload_status ON files(upload_finish_tm);
CREATE INDEX idx_files_file_id ON files(file_id);
CREATE INDEX idx_changesets_status ON changesets(status);
```

## Blob Creation and Encryption

### Workflow Pipeline

```
Original File → LZ4 Compress → ChaCha20-Poly1305 Encrypt → Base64 Encode → JSON with Metadata → Write Blob
```

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

### Performance Characteristics

- **Processing Rate**: 68+ files/second
- **Throughput**: 6-41 MB/s depending on file sizes
- **Encryption Dominance**: 82% of time spent in key derivation (CPU-bound)
- **Parallelization Potential**: Excellent due to CPU-bound workload

**For detailed performance analysis**: [Blob Creation Performance Analysis](blob-creation-performance-analysis.md)

## Key Design Decisions

### Path-Aware Blob Creation

**Decision**: `file_id = BLAKE3(path:file_hash)` instead of `file_id = file_hash`

**Benefits:**
- **Disaster recovery without database**: Blobs contain complete path information
- **Deterministic operations**: Same content at same path always produces same blob
- **Simple resume logic**: Re-process files where `upload_finish_tm IS NULL`
- **No reference counting**: Eliminates complex blob lifecycle management

**Trade-off accepted**: Same content at different paths creates separate blobs (storage duplication for operational simplicity)

### Upload State Management

The dual timestamp approach provides clear state tracking:

```sql
-- File states derived from timestamps:
-- upload_start_tm=NULL, upload_finish_tm=NULL  → Not started
-- upload_start_tm=X, upload_finish_tm=NULL     → In progress (or stuck)  
-- upload_start_tm=X, upload_finish_tm=Y        → Completed in (Y-X) time
```

### Single Backend Per Database

**Benefits:**
- **No coordination complexity**: Eliminates multi-backend failure scenarios
- **Local database**: SQLite travels with the data
- **Parallel processing**: Multiple workers can safely update different files
- **Clear ownership**: Each deployment has simple 1:1 relationships

**Scaling**: Deploy multiple database instances rather than coordinate shared state

## Operational Workflows

### Upload Process

1. **Changeset Creation**: `changeset_id = BLAKE3(name + sorted_file_list)`
2. **File Processing**: For each file:
   - Generate `file_hash = BLAKE3(file_content)`
   - Generate `file_id = BLAKE3(path:file_hash)`  
   - Create blob via tested workflow
   - Insert file record with both timestamps as `NULL`
   - Update `upload_start_tm = NOW()`
   - Upload blob to backend
   - Update `upload_finish_tm = NOW()`

### Resume/Retry Logic

```sql
-- Find files needing upload (never started or stuck)
SELECT path, file_id FROM files WHERE upload_finish_tm IS NULL;

-- Find stuck uploads (started >1 hour ago, not finished)
SELECT path, file_id FROM files 
WHERE upload_start_tm IS NOT NULL 
  AND upload_finish_tm IS NULL
  AND upload_start_tm < datetime('now', '-1 hour');
```

### Multi-Process Concurrency

**GNU Parallel Integration**:
```bash
# Split file processing across multiple workers
parallel -j 8 ./upload_chunk.py {} ::: chunk1.txt chunk2.txt ... chunk8.txt
```

**SQLite Configuration for Parallel Processing**:
```python
import sqlite3

# In each worker process
conn = sqlite3.connect(f"{n2sroot}/.n2s/{backend_name}-manifest.db", timeout=30.0)
conn.execute("PRAGMA journal_mode=WAL")     # Enable WAL mode for better concurrency
conn.execute("PRAGMA synchronous=NORMAL")   # Faster writes, still safe
```

**Database contention**: <0.5% of total processing time

## Disaster Recovery

### Complete System Rebuild

**From Blobs Only** (no database):
1. List all blobs in backend storage
2. Decrypt each blob to extract metadata
3. Reconstruct filesystem using `metadata.path`

**From Database Only** (no blobs):
1. Query files table for all uploaded files
2. Re-read original files and recreate blobs  
3. Upload missing blobs to backend

### Recovery Tools

Complete disaster recovery toolset with minimal dependencies:
- `jq` - JSON processing  
- `lz4` - LZ4 decompression
- `b3sum` - BLAKE3 hash verification
- `touch` - Set file timestamps
- **Decrypt binary** - ChaCha20 decryption utility (built from Go source)

**For complete recovery procedures**: [Disaster Recovery](../recovery/README.md)

## Data Flow

```
Client Request → Frontend API → Service Manager → Database + Backend Providers
                                     ↓              ↓           ↓
                             Encryption/Hashing   Metadata   Encrypted Blobs
                                     ↓
                               SQLite Operations
```

## Frontend API Design

### Storage Operations

**push**: Store files in a new changeset
```python
push(
    project: str,                    # "dsg", "zfs-backup", etc.
    dataset: str,                    # "BB", "tank/home", etc. 
    file_paths: List[str],           # Files to store
    source_snapshot: str = None,     # ZFS snapshot, git commit, etc.
    description: str = None,         # Human readable description
    backends: List[str] = None       # Which backends to push to
) -> str  # Returns changeset_id
```

**pull**: Restore files from storage
```python
pull(
    project: str,
    dataset: str,
    target_path: str,                # Where to restore files
    source_snapshot: str = "latest", # Specific snapshot or "latest"
    files: List[str] = None,         # Specific files or None for full changeset
    force: bool = False              # Overwrite existing files
)
```

### Query Operations

**list_files**: Files in a specific changeset with upload status
```python
list_files(project: str, dataset: str, source_snapshot: str, pattern: str = None)
```

**search_files**: Files matching pattern across changesets with upload status
```python
search_files(project: str, dataset: str, pattern: str, since: str = None, until: str = None)
```

**list_changesets**: Available snapshots/changesets
```python
list_changesets(project: str, dataset: str, since: str = None, until: str = None)
```

## Benefits of This Approach

1. **Reduced Complexity**: Dramatically fewer failure modes and edge cases
2. **Improved Debuggability**: Clear data lineage and direct mappings
3. **Better Disaster Recovery**: Complete recovery possible from blobs alone
4. **Easier Testing**: Fewer components and interactions to test
5. **Simpler Scaling**: Add database instances rather than coordinate shared state
6. **Clearer Operations**: Obvious retry semantics and status tracking

## Implementation Status

### Completed Decisions
1. **Backend coordination**: Single backend per database eliminates complex coordination
2. **Database design**: 2-table SQLite schema with local manifest databases
3. **Blob creation**: ChaCha20-Poly1305 encryption with path-aware deterministic blob IDs
4. **Disaster recovery**: Complete recovery toolset with cross-platform Go binaries

### Remaining Implementation
1. **Frontend API**: RESTful API design and implementation
2. **Service Manager**: Business logic and blob creation orchestration  
3. **Client integrations**: DSG, ZFS, Btrfs, and Find client implementations
4. **Configuration management**: TOML-based configuration with environment substitution

## Scale Expectations

- **Files per deployment**: 5-15 million files
- **Daily deltas**: 20-50K files (20-50 GB)
- **Processing window**: 24-hour batch processing acceptable
- **Database size estimate**: 
  - File records: ~3 GB
  - Changeset records: ~50 MB
  - Total: ~3.5 GB for 15M files

## Future Considerations

1. **Storage cost monitoring**: Track duplication levels in real deployments
2. **Parallel processing**: Test scaling with 16+ workers using GNU parallel
3. **Large file optimization**: Measure performance with 100MB+ files and network I/O
4. **Network upload testing**: Benchmark actual backend upload speeds vs blob creation times