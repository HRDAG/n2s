<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# Backend Provider Coordination

## Overview

The Service Manager coordinates storage operations across multiple backend providers with robust handling of partial failures, network issues, and recovery scenarios. This is critical for reliable data storage when dealing with large datasets and unreliable networks.

## Backend Provider Implementation

This document covers the implementation details of backend coordination. For the backend interface definition and list of supported backends, see the Backend Provider Layer section in `service-architecture.md`.

### Backend Configuration
Each backend provider is configured in TOML with credentials and settings:

```toml
[backends.s3_primary]
type = "s3"
bucket = "backup-bucket"
region = "us-west-2"
access_key = "AKIA..."
secret_key = "..."

[backends.ipfs_node]
type = "ipfs"
api_url = "http://localhost:5001"

[backends.gdrive]
type = "rclone"
remote = "gdrive:"
config_path = "/path/to/rclone.conf"

[backends.local_dev]
type = "local"
path = "/backup/storage"
```

## Push Coordination

### Multi-Backend Push Process

1. **Changeset Creation**: Create changeset record with `status = 'pending'`
2. **Blob Preparation**: Hash content, encrypt, create blob records
3. **Backend Status Initialization**: Create `blob_backends` records for each blob/backend combination with `status = 'pending'`
4. **Parallel Push**: Push to all configured backends simultaneously
5. **Status Tracking**: Update `blob_backends` status as each push completes/fails
6. **Changeset Completion**: Mark changeset complete when sufficient backends succeed or retry limits reached

### Push Design Philosophy

**Non-Atomic by Design**: Push operations are designed to require multiple restarts and partial completion. The system assumes network failures, backend outages, and interruptions are normal operating conditions.

**Restart-Friendly**: Every push operation can be safely restarted at any point. The `blob_backends` table tracks exactly which blobs have been successfully pushed to which backends.

## Failure Handling and Recovery

### Failure Types

**Network Failures**:
- Temporary connection issues
- Timeout errors
- DNS resolution failures

**Backend Failures**:
- Authentication errors
- Storage quota exceeded
- Service unavailable

**Data Failures**:
- Corruption during upload
- Hash mismatch verification
- Incomplete transfers

### Retry Strategy

```python
# Exponential backoff with jitter
def calculate_retry_delay(attempt: int, base_delay: float = 1.0, max_delay: float = 300.0):
    delay = min(base_delay * (2 ** attempt), max_delay)
    jitter = random.uniform(0.8, 1.2)
    return delay * jitter

# Retry configuration per backend type
retry_config = {
    "s3": {"max_attempts": 5, "base_delay": 2.0},
    "ipfs": {"max_attempts": 3, "base_delay": 5.0},
    "rclone": {"max_attempts": 4, "base_delay": 1.0},
    "local": {"max_attempts": 2, "base_delay": 0.5}
}
```

### Partial Recovery Operations

**Resume Push**: Continue failed changeset push
```python
def resume_push(changeset_id: str, backends: List[str] = None):
    # Find all blobs with status 'pending' or 'failed' for specified backends
    # Retry push for those blobs only
    # Update blob_backends status as operations complete
```

**Verify and Repair**: Check backend consistency
```python
def verify_changeset(changeset_id: str, backend: str):
    # Download each blob from backend
    # Verify hash matches database
    # Mark as 'failed' if corruption detected
    # Optionally re-push corrupted blobs
```

## Backend Health Monitoring

### Health Check Operations

**Connectivity Check**:
- Test basic connection to each backend
- Verify authentication
- Check available storage space

**Performance Monitoring**:
- Track upload/download speeds
- Monitor error rates
- Measure response times

**Periodic Validation**:
- Random blob integrity checks
- Cross-backend consistency verification
- Automated repair of detected issues

### Health Status Tracking

```sql
CREATE TABLE backend_health (
    backend_name TEXT PRIMARY KEY,
    last_check TIMESTAMP WITH TIME ZONE,
    status TEXT, -- 'healthy', 'degraded', 'failed'
    error_rate FLOAT,
    avg_response_time FLOAT,
    last_error TEXT,
    consecutive_failures INTEGER
);
```

## Error Recovery Workflows

### Automatic Recovery

**Failed Backend Handling**:
- Mark backend as degraded after consecutive failures
- Route new pushes to healthy backends only
- Periodically retry failed backend
- Resume pushes when backend recovers

**Partial Push Recovery**:
- Detect incomplete changesets on startup
- Resume pushing missing blobs
- Handle cases where some backends succeeded

**Permanently Unavailable Backends**:
- Generate extensive log warnings when backends become permanently unavailable
- Continue operations with remaining healthy backends
- Alert operators through logging system

### Manual Recovery Tools

**Backend Sync**:
```bash
# Sync missing blobs to a specific backend
n2s backend sync --backend s3_primary --changeset <id>
```

**Consistency Check**:
```bash
# Verify all blobs exist across backends
n2s backend verify --project dsg --dataset BB
```

**Repair Operations**:
```bash
# Re-push corrupted or missing blobs
n2s backend repair --backend ipfs_node --since 2025-01-01
```

## Configuration Examples

### High Reliability Setup
```toml
[coordination]
require_minimum_backends = 2  # Success if at least 2 backends succeed
priority_backends = ["s3_primary", "s3_secondary"]  # Backend priority order

[monitoring]
health_check_interval = "5m"        # Configurable health check frequency
integrity_check_rate = 100          # Check 1 in 100 blobs randomly
```

### Bandwidth-Limited Setup
```toml
[coordination]
require_minimum_backends = 1
priority_backends = ["local", "s3_primary"]

[monitoring]
health_check_interval = "15m"       # Less frequent checks for limited bandwidth
integrity_check_rate = 1000         # Very low background validation
```

## Design Decisions

1. **Non-Atomic Pushes**: Operations are designed to require multiple restarts and handle partial completion gracefully
2. **Backend Priority**: Configurable via TOML settings for different deployment scenarios  
3. **Health Check Frequency**: Fully configurable via TOML to balance monitoring vs. resource usage
4. **Read Failover**: Not implemented - reads use first available backend without automatic failover
5. **Permanently Failed Backends**: Handled through extensive logging and alerting, operations continue with remaining backends