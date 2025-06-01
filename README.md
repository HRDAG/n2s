<!-- 
  Author: PB & Claude
  Maintainer: PB
  Original date: 2025.05.13
  License: (c) HRDAG, 2025, GPL-2 or newer
 --> 

# n2s - ZFS to S3 Backup

Incremental ZFS backup to S3 with PostgreSQL tracking.

## Overview

n2s uses `zfs diff` to identify changed files and backs them up to S3 with PostgreSQL tracking.

## Installation

```bash
# Requires Python >= 3.13
git clone git@github.com:HRDAG/n2s.git
cd n2s
uv pip install -e .
```


## License

GPL-2 or newer, (c) HRDAG 2025
