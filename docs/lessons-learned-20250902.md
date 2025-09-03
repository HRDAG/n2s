<!--
Author: PB & Claude
Maintainer: PB
Original date: 2025.09.02
License: (c) HRDAG, 2025, GPL-2 or newer

------
docs/lessons-learned-20250902.md
-->

# Lessons Learned - September 2, 2025

## Incident Summary

On September 2, 2025, we discovered two critical data integrity issues in the pbnas blob storage system:
1. **Bad blob format**: ~28,000 blobs were written with incorrect format (raw gzip+sha256 instead of JSON-wrapped lz4+blake3)
2. **Mixed timezone data**: Timestamp columns contained an unfixable mix of UTC and local timezone values

## Impact

### Bad Blob Format
- **Total affected**: 28,466 blobs
  - 27,907 GZIP format (raw compressed data)
  - 555 OTHER format (unknown/corrupt)
- **Time range**: Uploads between 18:00 and 23:54 UTC on Sept 2
- **Root cause**: Experimental worker using wrong compression and missing JSON wrapper

### Timezone Contamination
- **Affected columns**:
  - `fs.calcd`: timestamp without time zone (should be with)
  - `fs.uploaded`: 21+ million UTC-contaminated records
  - `work_queue.claimed_at`: timestamp without time zone
  - `work_queue.created_at`: 1.2+ million UTC-contaminated records
- **Root cause**: Database timezone set to UTC, inconsistent timestamp handling between Python and SQL

## What Went Wrong

### 1. Blob Format Issue

**The Contract Violation**:
- Expected format: JSON wrapper containing lz4-compressed data with blake3 hash
- JSON header should start with:
  ```json
  {
    "content": {
      "encoding": "lz4-multiframe",
      "frames": [
  ```
- Actual format: Raw gzip-compressed data with sha256 hash, no wrapper

**The Parallel Execution Problem**:
- Old and new workers ran simultaneously against the same queue
- Created interleaved good/bad blobs, making binary search for transition point impossible
- No validation on upload meant bad blobs were committed to storage

### 2. Timezone Issue

**Schema Inconsistency**:
```sql
-- Wrong: no timezone awareness
calcd timestamp without time zone
claimed_at timestamp without time zone  
created_at timestamp without time zone

-- Right: timezone aware
uploaded timestamp with time zone
last_missing_at timestamp with time zone
```

**Mixed Sources**:
- Database configured with `timezone = 'Etc/UTC'`
- Scripts used mix of `NOW()` (UTC from DB) and Python local times
- Policy was "local time only" but implementation was inconsistent

## How We Fixed It

### Bad Blob Cleanup

**Detection** (by separate analysis script):
- Checked file headers for JSON wrapper signature
- Looked for `'{\n  "content": {\n    "encoding": "lz4-multiframe"'`
- Scanned entire time range due to interleaving
- Output: pipe-delimited file with `type | blobid | uploaded | path`

**Remediation** (split workflow):
1. Storage cleanup (other team): Delete blob files from `/n2s/block_storage`
2. Database cleanup (our script `cleanup_bad_blobs_db.py`):
   ```sql
   -- For each unique blobid
   UPDATE fs SET blobid = NULL, uploaded = NULL WHERE blobid = ?;
   INSERT INTO work_queue (pth) 
     SELECT pth FROM fs WHERE blobid = ? 
     ON CONFLICT DO NOTHING;
   ```

### Timezone Standardization

**Environment fixes**:
```sql
-- Set database default timezone
ALTER DATABASE pbnas SET timezone = 'America/Los_Angeles';
```

**Connection fixes**:
```python
# Force local timezone in all connections
conn_string = f"host={DB_HOST} ... options='-c timezone=America/Los_Angeles'"
```

**Policy enforcement**:
- Always use `NOW()` in SQL (never Python timestamps)
- Database generates all timestamps
- No client-side timestamp generation

**Why we didn't fix historical data**:
- Mixed UTC/local values are indistinguishable without metadata
- Risk of double-converting already-correct values
- Accepted the historical corruption, fixed going forward

## Preventive Measures

### Implemented
1. ✅ Database timezone set to local (`America/Los_Angeles`)
2. ✅ All connections force local timezone
3. ✅ Cleanup scripts ready for bad blob remediation
4. ✅ Deprecated experimental worker scripts archived
5. ✅ Single production worker with correct format

### Recommended
1. **Contract validation**: Check JSON wrapper on upload, reject invalid blobs
2. **Format versioning**: Add schema version to wrapper for migration support
3. **Environment isolation**: Separate queue/storage for experimental workers
4. **Schema discipline**: Use `timestamp with time zone` everywhere
5. **Monitoring**: Periodic sampling of new blobs to verify format compliance

## Current Status

- Bad blob cleanup: Pending storage deletion, then database cleanup
- Timezone: Fixed for new data, historical data remains mixed
- Worker: Single correct implementation (`pbnas_blob_worker.py`)
- Scripts: Production-ready tools only, test scripts archived

## Appendix: Cleanup Commands

```bash
# After storage cleanup completes
./scripts/cleanup_bad_blobs_db.py tmp/bad_blobs_20250902_165441.txt -y

# Verify database timezone
psql -h snowball -U pball -d pbnas -c "SHOW timezone;"
# Should return: America/Los_Angeles
```

## Key Takeaways

1. **Parallel experimentation is dangerous**: Running old and new implementations simultaneously created an unrecoverable data mess
2. **Contracts need enforcement**: Upload validation would have prevented bad blobs from entering storage
3. **Timezone discipline matters**: Mixed timezone data without metadata is essentially corrupted
4. **Forward-only fixes are sometimes best**: Not all data corruption can be safely repaired
