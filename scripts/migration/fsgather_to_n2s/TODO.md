# fsgather_to_n2s Migration Project

## Overview
Migrate 46+ million file records from existing PostgreSQL `fs` table to n2s format, handling massive hardlink deduplication (46M → 3.8M unique inodes, ~12x ratio).

## Current State Analysis Needed

### 1. Assess Missing/Problem Files
- **Issue**: Migration process was incomplete, thousands of paths with "weird normalization" issues
- **Action**: Analyze PostgreSQL `fs` table to identify:
  - Files that failed normalization
  - Paths with encoding issues
  - Records missing critical fields (`cantfind=true` entries?)
  - Pattern analysis of problem paths

```sql
-- Investigation queries needed:
SELECT COUNT(*) FROM fs WHERE cantfind = true;
SELECT COUNT(*) FROM fs WHERE hash IS NULL AND main = true;
-- Look for weird path patterns, encoding issues, etc.
```

### 2. Data Quality Assessment
- Validate inode consistency across trees
- Check for orphaned records
- Identify files needing re-processing
- Document cleanup strategy

## Migration Implementation Plan

### Phase 1: Data Analysis & Cleanup
- [ ] Connect to PostgreSQL `fs` table on target server
- [ ] Analyze missing/problem file patterns  
- [ ] Develop cleanup/normalization strategy
- [ ] Create test subset for validation

### Phase 2: Migration Infrastructure
- [ ] Build `fsgather_to_n2s` converter
  - [ ] PostgreSQL → n2s schema mapping
  - [ ] `tree` → `st_dev` conversion logic
  - [ ] `main` → `is_canonical` mapping
  - [ ] Batch processing (100k-1M records per changeset)
- [ ] Configuration management
- [ ] Progress tracking and resumability

### Phase 3: Changeset-Based Migration
- [ ] **Test changeset**: Single tree subset (e.g., `backup` tree, first 100k `main=true` files)
- [ ] **Tree changeset**: Complete single tree migration
- [ ] **Full migration**: All trees, all files
- [ ] Validation and verification tools

## Technical Requirements

### Database Schema Mapping
```
PostgreSQL fs table → n2s files table:
pth          → path
inode        → st_inode  
tree         → st_dev (via mapping logic)
main         → is_canonical
hash         → file_hash (SHA256 → keep as-is for now)
size         → size
calcd        → mtime
```

### Migration Script Structure
```
scripts/migration/fsgather_to_n2s/
├── migrate.py           # Main migration orchestrator
├── config.toml          # DB connections + batch settings  
├── schema_mapper.py     # PostgreSQL fs → n2s conversion
├── batch_processor.py   # Changeset chunking logic
├── validator.py         # Data quality checks
└── README.md           # Usage and troubleshooting
```

### Configuration Options
```toml
[source]
postgresql_url = "postgresql://user:pass@host/pbnas"
table_name = "fs"

[target]  
n2s_database_url = "sqlite:///migration_test.db"

[migration]
batch_size = 100000
encryption_enabled = false  # Start with plaintext for testing
resume_from_changeset = null

[validation]
sample_rate = 0.01  # Validate 1% of migrated records
```

## Success Criteria
- [ ] All 46M+ records successfully migrated
- [ ] Hardlink deduplication preserved (3.8M canonical files)
- [ ] Data integrity validated
- [ ] Performance benchmarks met
- [ ] Missing file issues identified and resolved
- [ ] Changeset-based resumability proven

## Next Steps
1. **Immediate**: Set up development environment on server with PostgreSQL access
2. **Data Analysis**: Investigate incomplete migration issues  
3. **Prototype**: Build basic converter for small test batch
4. **Scale Test**: Validate with progressively larger changesets

## Notes
- Original population took "weeks" - n2s migration should be orders of magnitude faster
- Focus on `main=true` files first (only 3.8M vs 46M total)
- Keep migration logic separate from core n2s library
- Plan for iterative testing with changeset approach