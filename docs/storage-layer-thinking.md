# n2s Storage System - Core Design Decisions

## System Overview
n2s is a deduplicating storage coordination service that connects version-based data sources with storage backends. It handles encryption, deduplication, and reliable storage operations while maintaining complete audit trails.

## Key Design Principles

### 1. Separation of Concerns
- **Clients**: Make all policy decisions (what to store, when to backup, retention policies)
- **n2s**: Provides reliable storage service with no policy decisions
- **Backends**: Handle actual byte storage (S3, IPFS, etc.)

### 2. Installation-Scoped Architecture
- **Installation** = one filesystem + one backend pair
- Multiple backends for same filesystem = multiple installations
- Eliminates multi-backend coordination complexity
- Clean operational boundaries

### 3. Zero Tolerance for Data Loss
- Complete audit trail of all storage operations
- Event-driven storage operation tracking
- Periodic integrity verification through random sampling
- Eventual consistency acceptable, data loss is not

## Core Entities and Relationships

### Entity Hierarchy
```
Installations → Snapshots → File References → Stored Blobs → Storage Operations
```

### Database Schema Design

**Installations** (installation_id PK)
- Configuration for filesystem + backend pairs
- Scopes all other operations

**Snapshots** (installation_id, snapshot_timestamp PK)
- Point-in-time captures of a dataset
- Belongs to one installation
- Contains many file references

**File References** (installation_id, snapshot_timestamp, filepath PK)
- Logical file paths at specific points in time
- References stored blobs via content_hash
- Enables many-to-many relationship: files evolve over time (→ different blobs), same content appears in multiple files (→ same blob)

**Stored Blobs** (content_hash PK)
- Global deduplication across all installations
- Content-addressable storage using BLAKE3 hashes
- Contains encrypted JSON with content + metadata

**Storage Operations** (content_hash, installation_id, timestamp, operation_type PK)
- Event log of all backend interactions
- Tracks upload attempts, verifications, deletions
- Enables retry logic and complete audit trails
- Expected to be largest table (~90% of total database size)

### Key Relationships
- **Snapshot (1) → File References (many)**
- **File Reference (many) → Stored Blob (1)** - enables deduplication
- **Stored Blob (1) → Storage Operations (many)** - complete audit trail
- **Installation (1) → Storage Operations (many)** - scoped operation tracking

## API Design

### Core Operations
1. **push_snapshot**: Client sends dataset state at specific time
2. **pull_files**: Client requests specific files/content
3. **delete_files** *(maybe)*: Client-directed deletion

### Client Responsibility
- All policy decisions (what, when, how long to keep)
- Snapshot timing and content selection
- Restoration targets and timing
- Retention and deletion policies

### n2s Responsibility
- Reliable storage with retry logic
- Encryption and deduplication
- Backend coordination
- Integrity verification
- Audit trail maintenance

## Operational Characteristics

### Scale Expectations
- 5-15 million files per deployment
- Daily deltas of 20-50K files (20-50 GB)
- 24-hour batch processing window acceptable
- Database size estimate: ~55 GB total
  - Storage Operations: ~50 GB (90% of total)
  - File References: ~3 GB
  - Stored Blobs: ~1.5 GB

### Data Retention Strategy
- Storage Operations table requires cleanup strategy from day one
- Event log grows 4-5x faster than core file data
- Potential approaches: rolling windows, archival, aggregation

### Integrity Verification
- Client-configurable random sampling of stored blobs
- Policies specified per installation:
  - Sample rate, frequency, bias toward recent/old files
  - Failure handling (alert, re-upload, mark corrupted)
- Results logged in Storage Operations audit trail

## Architecture Benefits

1. **Operational Simplicity**: Single shared database, global view of all storage
2. **Reliability**: Complete audit trails, retry logic, integrity verification
3. **Flexibility**: Policy decisions remain with clients, not storage layer
4. **Scalability**: Clean separation allows independent scaling of components
5. **Disaster Recovery**: Self-contained encrypted blobs with embedded metadata
