<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 --> 

# n2s - Storage Coordination Service

## Overview

n2s is a deduplicating storage service. It connects version-based and incrementally updating data sources with any storage backend. It discovers changed files through clients (`dsg` repositories, ZFS/btrfs snapshots, or filesystem scanning), encrypts and deduplicates the content, then pushes it to one or several possible backends (S3-compatible, IPFS, rclone-supported services, or local archives) with upload tracking and restart capability. A database tracks all operations, enabling fast queries about what's stored where and when the upload was confirmed.

All content is hashed using BLAKE3, ensuring that data integrity can be verified at every point in the system. Clients push data to backends in units called changesets, which represent the state of the client's data at a specific time. This design enables partial upload recovery, efficient deduplication, and point-in-time restoration.

The architecture separates concerns: clients handle file discovery, n2s handles storage logistics, and backends handle the actual bytes. This design supports everything from single-user laptop backups to organization-wide data preservation workflows.

**Clients - File Discovery Layer:**
- **dsg**: Store versioned data repositories
- **ZFS or btrfs snapshots**: backup incremental filesystem changes between snapshots
- **Find-based scanning**: Schedule regular backups of modified files in any directory tree

**n2s - Storage Coordination Layer:**
- **API operations**: Provides push, pull, list, and search operations for clients
- **Data management**: Handles encryption, file-level deduplication, and content-addressable storage
- **State tracking**: Manages database records of what's stored, where, and when

**Backends - Storage Layer:**
- **Cloud storage**: S3-compatible for reliable object storage
- **Distributed storage**: IPFS for content-addressed peer-to-peer storage
- **Flexible destinations**: Any rclone-supported backend (Google Drive, Dropbox, etc.) or unix filesystems (local or SSH)

### Deeper Dive

For detailed technical documentation:
- [Service Architecture](docs/service-architecture.md) - Complete system design including API, database schema, and component interactions
- [Blob Storage Architecture](docs/blob-storage-architecture.md) - Encryption, deduplication, and disaster recovery design
- [Backend Coordination](docs/backend-coordination.md) - Multi-backend push coordination, failure handling, and recovery workflows

## Installation

```bash
# Requires Python >= 3.13 and uv
git clone git@github.com:HRDAG/n2s.git
cd n2s
uv pip install -e .
```


## License

GPL-2 or newer, (c) HRDAG 2025
