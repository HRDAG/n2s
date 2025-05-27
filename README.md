# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# README.md

# n2s - ZFS to S3 Encrypted Backup

Incremental ZFS backup to S3 with dual-layer encryption and PostgreSQL path mapping.

## Overview

n2s uses `zfs diff` to identify changed files and backs them up to S3 with:
- **File contents**: Encrypted with [pyrage](https://github.com/woodruffw/pyrage) ([age format](https://age-encryption.org/)) - interoperable with standard age/rage tools
- **File paths**: Encrypted with AES-256-ECB for deterministic mapping and simple recovery
- **PostgreSQL**: Tracks real→encrypted path mappings and backup status

## Architecture

```
ZFS snapshots → zfs diff → Changed files → Dual encryption → S3
                                 ↓
                           PostgreSQL DB
                      (path mappings & status)
```

## Encryption Design

**Dual-encryption approach** (see `docs/encryption_architecture_doc.md`):
- **Contents**: pyrage for strong security and interoperability
- **Paths**: AES-ECB for deterministic encryption with universal recovery

Trade-off: ECB mode is cryptographically weaker but enables:
- Consistent S3 keys for the same file path
- Recovery using only OpenSSL (available everywhere)
- Simple disaster recovery without specialized tools

## Installation

```bash
# Requires Python >= 3.13
git clone git@github.com:HRDAG/n2s.git
cd n2s
uv pip install -e .
```

## Disaster Recovery

Without Python/n2s, you can still recover files:

```bash
# Decrypt file contents (requires age/rage CLI)
# Install: https://github.com/FiloSottile/age or https://github.com/str4d/rage
age -d -p encrypted_file.age > original_file

# Decrypt file paths (requires only OpenSSL)
KEY=$(echo -n "PASSWORD" | openssl dgst -sha256 | cut -d' ' -f2)
echo "ENCRYPTED_HEX_PATH" | xxd -r -p | openssl enc -d -aes-256-ecb -K $KEY -nopad
```

## License

GPL-2 or newer, (c) HRDAG 2025