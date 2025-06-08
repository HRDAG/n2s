<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# n2s Service Architecture

## Overview

n2s is a simplified storage coordination service with clear layer separation, providing an API for clients that want to store data while maintaining state through a local metadata database and a single configured backend storage provider per deployment.

## Component Layers

### 0. Client Layer
- **Purpose**: File discovery and workflow management
- **Client Types**:
  - **DSG Client**: Data versioning system (Git-like for data) - **primary HRDAG workflow tool**
  - **ZFS Client**: Uses `zfs diff` between snapshots for incremental backup
  - **Btrfs Client**: Uses `btrfs subvolume find-new` for snapshot-based incremental backup  
  - **Find Client**: Uses `find -mtime` for time-based file discovery
- **Interface**: Calls Frontend API with discovered file lists
- **Responsibilities**:
  - Discover changed/new files using various strategies
  - Provide dataset identification and metadata
  - Handle user workflow and configuration
  - Call n2s for storage operations
- **Common Pattern**: All clients follow the same lightweight interface - discover files, call n2s API with paths and dataset info

### 1. Frontend API Layer
- **Purpose**: Clean interface for clients that want to store things
- **Responsibilities**: 
  - Accept storage requests from clients
  - Validate input data and metadata
  - Return storage confirmations and retrieval responses
- **Interface**: RESTful API, gRPC, or library bindings

### 2a. Service Manager Layer
- **Purpose**: Business logic and coordination between frontend and backends
- **Responsibilities**:
  - Content hashing (BLAKE3) and path-aware blob creation  
  - Encryption/decryption operations (ChaCha20-Poly1305)
  - Changeset creation and management
  - Single backend coordination
  - Configuration management (credentials, backend address, encryption keys)
- **State**: Configuration and credentials, but stateless for operations

### 2b. Metadata Database Layer
- **Purpose**: Persistent storage of metadata and state
- **Responsibilities**:
  - Store changeset and file metadata (2-table design)
  - Track upload status with start/finish timestamps
  - Provide ACID transactions for consistency
- **Storage**: SQLite databases located at `{n2sroot}/.n2s/{backend_name}-manifest.db`
- **Interface**: ORM layer for database abstraction

#### ORM Decoupling Benefits
- **Database Portability**: Same code works with SQLite or PostgreSQL
- **Deployment Flexibility**: Start with SQLite, scale to PostgreSQL when needed
- **Testing**: In-memory SQLite for fast unit tests
- **Development**: No PostgreSQL setup required for local development

#### Database Location Pattern
```toml
# Configuration specifies backend and n2sroot
[backend]
name = "s3-prod"
n2sroot = "/data"

# Database automatically created at:
# {n2sroot}/.n2s/{backend_name}-manifest.db
# Example: /data/.n2s/s3-prod-manifest.db
```

**Benefits of local SQLite databases:**
- **Travels with data**: Database stays with the file tree being tracked  
- **Backend isolation**: Multiple backends can operate on same n2sroot
- **No infrastructure**: No PostgreSQL setup required
- **Multi-process safe**: SQLite WAL mode handles concurrent workers

### 3. Backend Provider Layer
- **Purpose**: Single storage backend per database deployment for operational simplicity
- **Supported Providers**:
  - **S3**: Object storage with content-addressable keys
  - **IPFS**: Distributed storage with CID mapping
  - **rclone**: Any rclone-supported backend (Google Drive, Dropbox, etc.)
  - **unixfs:local**: Local filesystem storage
  - **unixfs:ssh**: Remote filesystem over SSH

#### Common Backend Interface
All backends implement the same interface for Service Manager:
```python
class BackendProvider:
    def store_blobs(self, blobs: Dict[str, bytes]) -> Dict[str, dict]
    def retrieve_blobs(self, content_hashes: List[str]) -> Dict[str, bytes]
    def delete_blobs(self, content_hashes: List[str]) -> Dict[str, bool]
    def health_check(self) -> dict
    def list_blobs(self, prefix: str = None) -> List[str]
```

**Design Philosophy**: All operations use plural forms for efficiency. Single operations pass single-item collections:
```python
# Store one blob
result = backend.store_blobs({"abc123": encrypted_blob_data})

# Retrieve one blob  
blobs = backend.retrieve_blobs(["abc123"])
```

#### Backend-Specific Returns
- **S3**: `store_blobs` returns S3 object keys and ETags
- **IPFS**: `store_blobs` returns CIDs (stored in database for mapping)
- **rclone**: `store_blobs` returns remote path confirmations
- **unixfs**: `store_blobs` returns local/remote file paths

#### Configuration
Backend credentials and settings configured in Service Manager TOML (see Layer 2a)

## Architectural Questions

### Coupling Concerns

**Current Question**: The metadata layer needs to handle encryption/decryption and must see the frontend data. This creates coupling between layers 1 and 2.

**Potential Decoupling**: Split layer 2 into:
- **2a. Service Manager**: Handles encryption, hashing, and business logic
- **2b. Database**: Pure data storage (local SQLite or remote RDBMS)

This would allow:
- Database to be swappable (SQLite for local, PostgreSQL for remote)
- Service manager to be stateless and horizontally scalable
- Clear separation between coordination logic and data persistence

## Data Flow

```
Client Request → Frontend API → Service Manager → Database + Backend Providers
                                      ↓              ↓           ↓
                              Encryption/Hashing   Metadata   Encrypted Blobs
                                      ↓
                                ORM Operations
                                      ↓
                              SQLite/PostgreSQL
```

## Benefits of Layer Separation

1. **Scalability**: Each layer can scale independently
2. **Flexibility**: Swap backend providers without affecting clients
3. **Testing**: Mock individual layers for isolated testing
4. **Deployment**: Different deployment models (single binary vs. distributed services)
5. **Database Choice**: SQLite for single-user, PostgreSQL for multi-user

## Database Schema

### Three-Table Design

**changesets** - Groups of related operations pushed together:
```sql
CREATE TABLE changesets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Identification
    dataset TEXT NOT NULL,                      -- Source dataset name
    source_snapshot TEXT,                       -- ZFS snapshot, git commit, etc.
    client_id TEXT,                            -- Which client initiated this
    
    -- Timing
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,       -- When push began
    completed_at TIMESTAMP WITH TIME ZONE,     -- When push finished
    
    -- Status tracking  
    status TEXT NOT NULL DEFAULT 'pending',    -- pending, pushing, completed, failed
    file_count INTEGER DEFAULT 0,              -- Number of files in this changeset
    blob_count INTEGER DEFAULT 0,              -- Number of unique blobs
    total_size BIGINT DEFAULT 0,               -- Total bytes (original content)
    
    -- Backend tracking
    backends_pushed TEXT[],                     -- Array of completed backends
    backends_failed TEXT[],                     -- Array of failed backends
    
    -- Metadata
    description TEXT,                           -- Human readable description
    metadata JSONB                              -- Flexible additional data
);
```

**blobs** - Content-addressable storage (from existing blob-storage-architecture.md)

**files** - File path mappings, linked to changesets:
```sql
ALTER TABLE files ADD COLUMN changeset_id UUID REFERENCES changesets(id);
```

### Benefits of PostgreSQL Arrays
- Efficient 1:many relationship for backend status tracking
- Native array operations for querying backend completion status
- Avoids separate junction tables for simple status lists

## Service Manager Design

The Service Manager uses an ORM layer for database interaction, making local vs remote database deployment a simple configuration change:
- **Local**: `sqlite:///local/path/n2s.db`
- **Remote**: `postgresql://user:pass@host:port/dbname`

### Configuration
- All configuration stored in TOML files including backend credentials and encryption keys
- Service Manager loads config at startup and maintains in memory
- See `configuration.md` (TODO) for complete configuration reference and examples

### Core Operations
- **Content Operations**: BLAKE3 hashing and encryption/decryption (see `blob-storage-architecture.md`)
- **Database Operations**: Stateless operations via ORM layer
- **Backend Coordination**: Multi-backend push/pull coordination (see `backend-coordination.md`)

#### Blob Structure and Metadata
The Service Manager creates encrypted blobs containing both file content and metadata for disaster recovery:

**On Push**:
1. Read file content + extract metadata (filepath, mtime, size)
2. Create JSON blob: `{"content": "hex_data", "metadata": {"filepath": "...", "mtime": "...", "content_hash": "..."}}`
3. Encrypt JSON blob and send to backends

**On Pull**:
1. Retrieve encrypted blob from backend and decrypt
2. Extract file content from JSON
3. Use **files table** for authoritative filepath and mtime (blob metadata may be outdated)
4. Write file with correct metadata from database

**Disaster Recovery**: Blob metadata allows full system rebuild if database is lost, though filepath/mtime in blobs may be superseded by later updates since blobs are deduplicated by content_hash.

### Deployment Models
- **Single Process**: SQLite database, all components in one binary
- **Distributed**: PostgreSQL database, multiple Service Manager instances
- **Scaling**: Service Manager is stateless for operations, can scale horizontally

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

**list_failed_uploads**: Files needing retry
```python
list_failed_uploads(project: str, dataset: str, backend: str = None)
```

**backend_status**: Backend health and failure rates
```python
backend_status(project: str, dataset: str)
```

### Status Operations

**changeset_status**: Per-backend upload status for a changeset
```python
changeset_status(project: str, dataset: str, source_snapshot: str)
# Returns file-by-file, backend-by-backend status for partial transmission recovery
```

### Integrity Operations

**validate**: Background integrity checking
```python
validate(project: str, dataset: str, pattern: str = None, sample_rate: int = None)
# Downloads and verifies blob hashes for corruption detection
```

## Simplified Database Schema (2-Table Design)

The n2s simplified architecture uses only 2 tables for operational simplicity:

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

**Upload State Management:**
- `upload_start_tm=NULL, upload_finish_tm=NULL` → Not started
- `upload_start_tm=X, upload_finish_tm=NULL` → In progress (or stuck)  
- `upload_start_tm=X, upload_finish_tm=Y` → Completed in (Y-X) time

**For complete blob creation details, see:**
- [Simplified Architecture](simplified-architecture.md) - 2-table design rationale
- [Blob Creation Performance Analysis](blob-creation-performance-analysis.md) - ChaCha20-Poly1305 implementation and testing

## Design Philosophy: Simplicity Over Complexity

The n2s simplified architecture prioritizes **operational reliability** over storage optimization:

### Path-Aware Blob Creation

**Key Decision**: `file_id = BLAKE3(path:file_hash)` creates path-aware blobs

**Benefits:**
- **Disaster recovery without database**: Blobs contain complete path information
- **Deterministic operations**: Same content at same path always produces same blob
- **Simple resume logic**: Re-process files where `upload_finish_tm IS NULL`
- **No reference counting**: Eliminates complex blob lifecycle management

**Trade-off accepted**: Same content at different paths creates separate blobs (storage duplication for operational simplicity)

### Single Backend Per Database

**Key Decision**: One backend per SQLite database instance

**Benefits:**
- **No coordination complexity**: Eliminates multi-backend failure scenarios
- **Local database**: SQLite travels with the data at `{n2sroot}/.n2s/{backend}-manifest.db`
- **Parallel processing**: Multiple workers can safely update different files
- **Clear ownership**: Each deployment has simple 1:1 relationships

**Scaling**: Deploy multiple database instances rather than coordinate shared state

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