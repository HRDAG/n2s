<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.06.08
  License: (c) HRDAG, 2025, GPL-2 or newer
 --> 

# n2s - Storage Coordination Service

## Overview

n2s is a storage coordination service. It connects version-based and incrementally updating data sources with any storage backend. It discovers changed files through clients (`dsg` repositories, ZFS/btrfs snapshots, or filesystem scanning), encrypts the content with path-aware blob creation, then pushes it to configured backends (S3-compatible, IPFS, rclone-supported services, or local archives) with upload tracking and restart capability. A database tracks all operations, enabling fast queries about what's stored where and when the upload was confirmed.

All content is hashed using BLAKE3, ensuring that data integrity can be verified at every point in the system. Clients push data to backends in units called changesets, which represent the state of the client's data at a specific time. The simplified architecture prioritizes operational reliability and disaster recovery over storage deduplication.

The architecture separates concerns: clients handle file discovery, n2s handles storage logistics, and backends handle the actual bytes. This design supports everything from single-user laptop backups to organization-wide data preservation workflows.

**Clients - File Discovery Layer:**
- **dsg**: Store versioned data repositories
- **ZFS or btrfs snapshots**: backup incremental filesystem changes between snapshots
- **Find-based scanning**: Schedule regular backups of recently modified files in any directory tree

**n2s - Storage Coordination Layer:**
- **API operations**: Provides push, pull, list, and search operations for clients
- **Data management**: Handles encryption and path-aware content-addressable storage
- **State tracking**: Manages database records of what's stored, where, and when

**Backends - Storage Layer:**
- **Cloud storage**: S3-compatible for reliable object storage
- **Distributed storage**: IPFS for content-addressed peer-to-peer storage
- **Flexible destinations**: Any rclone-supported backend (Google Drive, Dropbox, etc.) or unix filesystems (local or SSH)

## Simplified Architecture

n2s uses a simplified 2-table design optimizing for operational simplicity:
- **Path-aware blob creation**: Same content at different paths creates different blobs, enabling disaster recovery without database dependency
- **Deterministic operations**: Identical content at same path always produces same blob ID (same file hash + same path = same blob)
- **Single backend per database**: Eliminates complex coordination overhead
- **Resume capability**: Simple retry logic for failed uploads

This design trades storage efficiency for operational reliability - you can always recover your data with minimal tooling.

### Storage Features
- **Path-aware content-addressable storage**: Files identified by BLAKE3(path:file_hash)
- **Deterministic blob creation**: Identical content at same path always produces same blob ID
- **Multiple backends**: Store to S3, IPFS, rclone-supported services, or local storage  
- **ChaCha20-Poly1305 encryption**: All content encrypted before storage with deterministic key derivation
- **Disaster recovery**: Complete system rebuild from storage and encryption keys alone

### Database Schema

The system uses a simplified 2-table design:
- **changesets**: Groups of files pushed together with status tracking
- **files**: File records with upload status and blob references

### Deeper Dive

For detailed technical documentation:
- [Architecture](docs/architecture.md) - Complete system design including components, database schema, API, and design decisions
- [Blob Creation Performance Analysis](docs/blob-creation-performance-analysis.md) - ChaCha20-Poly1305 implementation and performance testing
- [Disaster Recovery](recovery/README.md) - Complete disaster recovery guide with working tools

## Installation

```bash
# Requires Python >= 3.13 and uv
git clone git@github.com:HRDAG/n2s.git
cd n2s
uv pip install -e .
```


## License

GPL-2 or newer, (c) HRDAG 2025
