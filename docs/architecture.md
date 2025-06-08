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

n2s is primarily a **library** for storage coordination, with a CLI interface for development and debugging. External clients handle domain-specific file discovery and workflow logic.

### External Clients (User-Specific)
**Purpose**: Domain-specific file discovery and workflow management

**Client Examples**:
```python
# DSG client - Git-like data workflows
dsg_files = discover_files_from_dsg_status()
n2s.push_changeset("dsg-daily", dsg_files)

# ZFS client - Snapshot-based incremental backup
zfs_changes = zfs_diff("tank/data@yesterday", "tank/data@today") 
n2s.push_changeset("zfs-incremental", zfs_changes)

# Custom client - Project-specific workflow
project_files = find_changed_files_since_last_backup()
n2s.push_changeset("project-backup", project_files)
```

**Responsibilities**:
- Discover changed/new files using domain-specific strategies
- Handle user workflow and policy decisions
- Call n2s library API for storage operations
- Manage configuration and credentials

### CLI Layer
**Purpose**: Development, debugging, and simple use cases

**Commands**:
```bash
n2s push [files...] --changeset-name NAME --config CONFIG
n2s pull --changeset-id ID --target-path PATH  
n2s status --backend NAME [--changeset-id ID]
```

**Usage**: Primarily for development and manual operations, not the main interface

### Library API Layer  
**Purpose**: Clean programmatic interface for external clients

**Core Methods**:
```python
# Primary library interface
service.push_changeset(name: str, file_paths: List[str]) -> str
service.pull_changeset(changeset_id: str, target_path: str)
service.get_changeset_status(changeset_id: str) -> dict
```

**Responsibilities**: 
- Accept storage requests from external clients
- Validate input data and metadata
- Return storage confirmations and retrieval responses
- Provide clean abstraction over service layer

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

**Implementation**: SQLAlchemy abstraction over embedded databases at `{n2sroot}/.n2s/{backend_name}-manifest.db`

**Database Options**:
- **SQLite**: Default embedded database (travels with data)
- **In-memory SQLite**: For testing (`sqlite:///:memory:`)
- **Future options**: DuckDB for analytics, other embedded databases

**Benefits of embedded database approach:**
- **Travels with data**: Database stays with the file tree being tracked  
- **Backend isolation**: Multiple backends can operate on same n2sroot
- **No infrastructure**: No external database setup required
- **Multi-process safe**: SQLite WAL mode handles concurrent workers
- **Database portability**: SQLAlchemy enables different embedded databases

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
    st_dev INTEGER NOT NULL,                        -- Device number (st_dev)
    st_inode INTEGER NOT NULL,                      -- Inode number (st_ino)
    size INTEGER NOT NULL,                          -- Original file size (or symlink target length)
    mtime TIMESTAMP NOT NULL,                       -- File modification time  
    file_hash TEXT NOT NULL,                        -- BLAKE3 hash of content or symlink target
    file_id TEXT NOT NULL,                          -- Storage key (shared across hardlink groups)
    is_canonical BOOLEAN NOT NULL,                  -- TRUE for canonical path in hardlink group
    is_symlink BOOLEAN DEFAULT FALSE,               -- TRUE for symbolic links
    upload_start_tm TIMESTAMP,                      -- When upload began
    upload_finish_tm TIMESTAMP,                     -- When upload completed
    
    PRIMARY KEY (path, changeset_id),
    FOREIGN KEY (changeset_id) REFERENCES changesets(changeset_id)
);

-- Indexes for performance
CREATE INDEX idx_files_upload_status ON files(upload_finish_tm);
CREATE INDEX idx_files_file_id ON files(file_id);
CREATE INDEX idx_files_hardlinks ON files(st_dev, st_inode);
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

**Regular Files:**
```json
{
  "encrypted_content": "base64_encoded_encrypted_compressed_data",
  "metadata": {
    "path": "relative/path/to/file.txt",
    "size": 12345,
    "timestamp": 1749388804.3256009,
    "file_hash": "blake3_hash_of_original_content",
    "type": "file"
  }
}
```

**Symbolic Links:**
```json
{
  "symlink_target": "../target/file.txt",
  "metadata": {
    "path": "relative/path/to/symlink",
    "size": 16,
    "timestamp": 1749388804.3256009,
    "file_hash": "blake3_hash_of_symlink_target",
    "type": "symlink"
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

### File Type Handling

**Regular Files:**
- `file_id = BLAKE3(canonical_path:file_hash)` for unique content
- Multiple paths can share same `file_id` (hardlink groups)
- Full blob creation with ChaCha20-Poly1305 encryption

**Hardlink Groups:**
- Client provides `(path, st_dev, st_inode)` tuples
- Group files by `(st_dev, st_inode)` to detect hardlinks
- Pick canonical path (lexicographically first) for blob creation
- All paths in group inherit same `file_id` from canonical path
- Only canonical path (`is_canonical = TRUE`) gets uploaded
- Massive deduplication for rsync --link-dest hardlink forests

**Symbolic Links:**
- `file_id = BLAKE3(path:target_hash)` where target_hash = BLAKE3(symlink_target)
- `file_hash = BLAKE3(symlink_target)` instead of file content
- Special blob with plaintext symlink target (no encryption needed)
- `is_symlink = TRUE` in database
- Perfect symlink recreation during disaster recovery

**Benefits:**
- **Hardlink deduplication**: Hundreds of thousands of hardlinks → single blob
- **Symlink preservation**: Maintains symlink semantics exactly
- **Disaster recovery**: Complete recreation of file structure including hardlinks
- **Path-aware design**: Same content at different paths still creates different blobs (unless hardlinked)

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
2. **File Discovery**: Client provides `[(path, st_dev, st_inode), ...]` tuples
3. **Hardlink Group Processing**:
   - Group files by `(st_dev, st_inode)`
   - Pick canonical path (lexicographically first) for each group
   - Generate `file_id = BLAKE3(canonical_path:file_hash)` for group
   - All paths in group inherit same `file_id`
4. **File Processing**: For each file:
   - **Regular files**: `file_hash = BLAKE3(file_content)`
   - **Symlinks**: `file_hash = BLAKE3(symlink_target)`
   - Insert file record with both timestamps as `NULL`
   - Mark canonical path with `is_canonical = TRUE`
5. **Upload Processing**: For each canonical file:
   - Update `upload_start_tm = NOW()`
   - Create and upload blob to backend
   - Update `upload_finish_tm = NOW()` for entire hardlink group

### Resume/Retry Logic

```sql
-- Find canonical files needing upload (only canonical paths get uploaded)
SELECT path, file_id FROM files 
WHERE is_canonical = TRUE AND upload_finish_tm IS NULL;

-- Find stuck uploads (canonical files started >1 hour ago, not finished)
SELECT path, file_id FROM files 
WHERE is_canonical = TRUE
  AND upload_start_tm IS NOT NULL 
  AND upload_finish_tm IS NULL
  AND upload_start_tm < datetime('now', '-1 hour');

-- Mark entire hardlink group as completed
UPDATE files SET upload_finish_tm = NOW() 
WHERE file_id = ? AND changeset_id = ?;
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
2. Decrypt each blob to extract metadata and content
3. **Regular files**: Write content to `metadata.path`
4. **Symlinks**: Create symlink using `symlink_target`
5. **Hardlinks**: Cannot be recreated without database (blobs only contain canonical path)

**From Database Only** (no blobs):
1. Query files table for all uploaded files
2. Re-read original files and recreate blobs  
3. Upload missing blobs to backend

**Complete Recovery** (blobs + database):
1. Restore files using blob content
2. Recreate hardlink groups using database `(st_dev, st_inode)` information
3. Perfect recreation of original filesystem structure

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
External Client → Library API → Service Manager → Database + Backend Providers
                       ↓              ↓              ↓           ↓
               CLI (dev/debug)  Encryption/Hashing   Metadata   Encrypted Blobs
                       ↓              ↓                         
                Display/Logging SQLAlchemy Operations
```

## Code Structure

```
src/n2s/
├── cli/
│   ├── __init__.py
│   ├── main.py          # Typer app entry point
│   ├── commands/
│   │   ├── __init__.py
│   │   ├── push.py      # push command
│   │   ├── pull.py      # pull command
│   │   └── status.py    # status/query commands
│   └── display.py       # Console output formatting
├── clients/
│   ├── __init__.py
│   ├── base.py          # Abstract base client (for internal testing)
│   └── find_client.py   # Simple find-based client (for CLI)
├── service/
│   ├── __init__.py
│   ├── manager.py       # Service manager (business logic)
│   ├── database/
│   │   ├── __init__.py
│   │   ├── models.py    # SQLAlchemy models/tables
│   │   ├── operations.py # Database operations
│   │   └── migrations.py # Schema migrations
│   └── blob.py          # Blob creation/encryption
├── backends/
│   ├── __init__.py
│   ├── base.py          # Abstract backend interface
│   └── ssh_backend.py   # SSH/SCP implementation
├── config/
│   ├── __init__.py
│   └── settings.py      # TOML config loading
└── logging/
    ├── __init__.py
    └── setup.py         # Loguru configuration
```

## Library API Design

### Primary Interface (for External Clients)

**Storage Operations**:
```python
# Core library methods - used by external clients
service.push_changeset(
    name: str,                       # Human-readable changeset name
    file_paths: List[str]            # Absolute paths to files
) -> str                             # Returns changeset_id

service.pull_changeset(
    changeset_id: str,               # Changeset to restore
    target_path: str,                # Where to restore files
    files: List[str] = None          # Specific files or None for all
)

service.get_changeset_status(
    changeset_id: str = None         # Specific changeset or None for all
) -> dict                            # Upload status, file counts, etc.
```

### CLI Interface (for Development/Debugging)

**Commands**:
```bash
# Simple file-based interface
n2s push file1.txt file2.txt --changeset-name "manual-backup"
n2s pull --changeset-id abc123 --target-path /restore/here
n2s status [--changeset-id abc123]
```

**Usage Pattern**: External clients import the library, CLI is for manual operations

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