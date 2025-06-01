<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.01.06
  License: (c) HRDAG, 2025, GPL-2 or newer
 -->

# TODO: New Documentation Files for n2s

This document outlines three new documentation files that would complete the n2s documentation suite.

## 1. cli-interface.md

**Purpose**: Define the command-line interface structure and provide usage examples.

**Content to include**:
- Command hierarchy (e.g., `n2s push`, `n2s pull`, `n2s list`, `n2s backend`)
- Common options and flags
- Authentication and configuration setup
- Usage examples for each major workflow:
  - Pushing a changeset
  - Pulling/restoring files
  - Listing changesets and files
  - Checking backend status
  - Resuming failed operations
- Exit codes and error handling
- Integration with different clients (DSG, ZFS, etc.)

**Key sections**:
- Installation and Setup
- Command Reference
- Common Workflows
- Troubleshooting

## 2. configuration.md

**Purpose**: Complete reference for all configuration options in TOML format.

**Content to include**:
- Full TOML configuration schema
- All sections with descriptions:
  - `[database]` - Database connection settings
  - `[encryption]` - Encryption keys and settings
  - `[backends.*]` - Backend provider configurations
  - `[coordination]` - Multi-backend coordination settings
  - `[monitoring]` - Health check and validation settings
  - `[logging]` - Logging configuration
- Environment variable overrides
- Configuration file locations and precedence
- Example configurations for different deployment scenarios:
  - Single-user with SQLite
  - Multi-user with PostgreSQL
  - High-reliability with multiple backends
  - Bandwidth-limited environments
- Security best practices for configuration

**Key sections**:
- Configuration File Format
- Complete Setting Reference
- Example Configurations
- Security Considerations

**Draft Complete Configuration Example**:
```toml
# n2s.toml - Complete configuration reference

[database]
# SQLite for single-user/development
url = "sqlite:///var/n2s/n2s.db"
# PostgreSQL for production
# url = "postgresql://n2s_user:password@db.example.com:5432/n2s_prod"
# pool_size = 20  # Connection pool size for PostgreSQL

[encryption]
# Age encryption passphrase (use environment variable in production)
passphrase = "${N2S_ENCRYPTION_PASSPHRASE}"
# Or use identity file
# identity_file = "/etc/n2s/age.key"

[backends.s3_primary]
type = "s3"
bucket = "n2s-backup-primary"
region = "us-west-2"
access_key = "${AWS_ACCESS_KEY_ID}"
secret_key = "${AWS_SECRET_ACCESS_KEY}"
# Optional: custom endpoint for S3-compatible storage
# endpoint = "https://s3.example.com"

[backends.s3_secondary]
type = "s3"
bucket = "n2s-backup-secondary"
region = "eu-west-1"
access_key = "${AWS_ACCESS_KEY_ID_EU}"
secret_key = "${AWS_SECRET_ACCESS_KEY_EU}"

[backends.ipfs_local]
type = "ipfs"
api_url = "http://localhost:5001"
# Optional: pin objects after upload
pin = true

[backends.gdrive]
type = "rclone"
remote = "gdrive:n2s-backup"
config_path = "/etc/n2s/rclone.conf"

[backends.local_archive]
type = "unixfs:local"
path = "/mnt/archive/n2s"

[backends.remote_ssh]
type = "unixfs:ssh"
host = "backup.example.com"
user = "n2s"
path = "/backup/n2s"
# SSH key authentication
identity_file = "/etc/n2s/ssh/id_ed25519"

[coordination]
# Minimum backends that must succeed for push to be considered successful
require_minimum_backends = 2
# Backend priority order (higher priority backends tried first)
priority_backends = ["s3_primary", "s3_secondary", "ipfs_local"]
# Maximum concurrent backend operations
max_concurrent_pushes = 3
# Retry configuration
max_retry_attempts = 3
retry_base_delay = 2.0  # seconds
retry_max_delay = 300.0  # 5 minutes

[monitoring]
# Health check configuration
health_check_interval = "5m"
health_check_timeout = "30s"
# Background integrity validation
integrity_check_enabled = true
integrity_check_rate = 100  # Check 1 in 100 blobs
integrity_check_interval = "1h"
# Alert thresholds
backend_failure_threshold = 5  # Consecutive failures before marking unhealthy
error_rate_threshold = 0.10  # 10% error rate triggers alerts

[logging]
level = "INFO"  # DEBUG, INFO, WARNING, ERROR
format = "json"  # json or text
# Loguru-specific settings
rotation = "100 MB"
retention = "30 days"
compression = "gz"
# Log file location
path = "/var/log/n2s/n2s.log"

[api]
# Frontend API settings
listen = "127.0.0.1:8080"
# TLS configuration (optional)
# tls_cert = "/etc/n2s/cert.pem"
# tls_key = "/etc/n2s/key.pem"
# Authentication
auth_enabled = true
# Token-based auth
auth_token_header = "X-N2S-Token"
# Or basic auth
# auth_users = { "user1" = "hashed_password" }

[limits]
# Maximum file size (in bytes)
max_file_size = 5368709120  # 5GB
# Maximum files per changeset
max_files_per_changeset = 10000
# Maximum concurrent operations
max_concurrent_operations = 50
# Rate limiting
rate_limit_enabled = true
rate_limit_requests_per_minute = 100

[cache]
# Optional Redis cache for metadata
enabled = false
# redis_url = "redis://localhost:6379/0"
# ttl = "1h"
```

**Additional notes for configuration.md**:
- Environment Variable Substitution: `${VAR_NAME}` syntax
- Configuration file search order:
  1. `--config` flag
  2. `$N2S_CONFIG_FILE` environment variable
  3. `./n2s.toml` (current directory)
  4. `~/.config/n2s/n2s.toml` (user config)
  5. `/etc/n2s/n2s.toml` (system config)

## 3. development-setup.md

**Purpose**: Guide for developers to get started with n2s development.

**Content to include**:
- Prerequisites (Python 3.13+, PostgreSQL, etc.)
- Development environment setup:
  - Using `uv` for dependency management
  - Setting up pre-commit hooks
  - Configuring development database (SQLite)
- Running tests:
  - Unit test structure
  - Integration test setup
  - Test database configuration
- Development workflow:
  - Code style and formatting
  - Making changes (following CLAUDE.md)
  - Testing changes locally
  - Submitting pull requests
- Debugging tips:
  - Logging configuration for development
  - Common development issues
  - Performance profiling
- Architecture overview for new developers

**Key sections**:
- Prerequisites
- Environment Setup
- Running Tests
- Development Workflow
- Architecture Guide

## Priority

1. **cli-interface.md** - Most important for users
2. **configuration.md** - Needed for deployment
3. **development-setup.md** - For contributors

## Notes

- Each document should follow the file header format from CLAUDE.md
- Keep consistent with existing documentation style
- Include practical examples throughout
- Cross-reference other documents where appropriate