<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# n2s Service Architecture

## Overview

n2s is reimagined as a storage service with clear layer separation, providing an API for clients that want to store data while maintaining state through a metadata database and supporting multiple backend storage providers.

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
  - Content hashing (BLAKE3) and deduplication
  - Encryption/decryption operations
  - Changeset creation and management
  - Backend provider coordination and failover
  - Configuration management (credentials, backend addresses, encryption keys)
- **State**: Configuration and credentials, but stateless for operations

### 2b. Metadata Database Layer
- **Purpose**: Persistent storage of metadata and state
- **Responsibilities**:
  - Store changeset, blob, and file metadata
  - Track backend push status and completion
  - Provide ACID transactions for consistency
- **Storage**: PostgreSQL (production) or SQLite (development/single-user)
- **Interface**: ORM layer for database abstraction

#### ORM Decoupling Benefits
- **Database Portability**: Same code works with SQLite or PostgreSQL
- **Deployment Flexibility**: Start with SQLite, scale to PostgreSQL when needed
- **Testing**: In-memory SQLite for fast unit tests
- **Development**: No PostgreSQL setup required for local development

#### Connection Configuration Examples
```toml
# Single-user/development
[database]
url = "sqlite:///local/n2s.db"

# Production PostgreSQL
[database] 
url = "postgresql://n2s_user:password@db.example.com:5432/n2s_prod"

# Distributed deployment
[database]
url = "postgresql://n2s_user:password@db-cluster.internal:5432/n2s"
pool_size = 20
```

### 3. Backend Provider Layer
- **Purpose**: Multiple storage backends for redundancy and flexibility
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

## Database Schema Updates

With partial transmission tracking, we now need 4 tables:

### 1. changesets - Groups of related operations
```sql
CREATE TABLE changesets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Identification
    project TEXT NOT NULL,                      -- "dsg", "zfs-backup", etc.
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

CREATE INDEX idx_changesets_project_dataset ON changesets(project, dataset);
CREATE INDEX idx_changesets_status ON changesets(status);
CREATE INDEX idx_changesets_created_at ON changesets(created_at);
```

### 2. blobs - Content-addressable storage
```sql
-- From blob-storage-architecture.md
CREATE TABLE blobs (
    content_hash TEXT PRIMARY KEY,              -- BLAKE3 hash of original content
    size BIGINT NOT NULL,                       -- Original file size in bytes
    encrypted_size BIGINT NOT NULL,             -- Encrypted blob size in bytes
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_blobs_first_seen ON blobs(first_seen);
```

### 3. files - File path mappings
```sql
-- From blob-storage-architecture.md with changeset_id added
CREATE TABLE files (
    dataset TEXT NOT NULL,                     -- Dataset name (e.g., "documents")
    snapshot TEXT NOT NULL,                    -- Snapshot identifier (e.g., timestamp)
    filepath TEXT NOT NULL,                    -- Original file path
    content_hash TEXT NOT NULL,                -- References blobs.content_hash
    mtime TIMESTAMP WITH TIME ZONE NOT NULL,   -- File modification time
    changeset_id UUID NOT NULL,               -- References changesets.id
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (dataset, snapshot, filepath),
    FOREIGN KEY (content_hash) REFERENCES blobs(content_hash),
    FOREIGN KEY (changeset_id) REFERENCES changesets(id)
);

CREATE INDEX idx_files_content_hash ON files(content_hash);
CREATE INDEX idx_files_changeset_id ON files(changeset_id);
```

### 4. blob_backends - Per-blob, per-backend status tracking
```sql
-- New table for partial transmission recovery
CREATE TABLE blob_backends (
    blob_hash TEXT REFERENCES blobs(content_hash),
    backend TEXT,
    status TEXT, -- 'pending', 'completed', 'failed'
    attempted_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,                         -- Last error if failed
    retry_count INTEGER DEFAULT 0,
    PRIMARY KEY (blob_hash, backend)
);

CREATE INDEX idx_blob_backends_status ON blob_backends(status);
CREATE INDEX idx_blob_backends_backend ON blob_backends(backend);
```

## Open Questions

1. How should the service manager handle multiple backend providers?
2. What's the optimal interface between layers (HTTP, gRPC, direct calls)?
3. How do we handle backend provider failures and failover?