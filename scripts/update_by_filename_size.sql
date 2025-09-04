-- Author: PB & Claude
-- Maintainer: PB
-- Original date: 2025.09.04
-- License: (c) HRDAG, 2025, GPL-2 or newer
--
-- ------
-- scripts/update_by_filename_size.sql

-- Update main=false files with blobids based on filename+size matches
-- from main=true files that already have blobids.
-- Run this directly on the database server for best performance.

\timing on
\echo 'Starting filename+size based blobid updates'

-- First show the potential
\echo 'Checking how many main=false files could be updated...'
SELECT 
    COUNT(DISTINCT (substring(pth from '[^/]+$'), size)) as unique_pairs,
    COUNT(*) as total_files
FROM fs
WHERE main = false 
  AND blobid IS NULL
  AND size IS NOT NULL;

-- Create temp table with main=true files that have blobids
\echo 'Creating temp table of main=true files with blobids...'
CREATE TEMP TABLE main_true_blobids AS
SELECT DISTINCT
    substring(pth from '[^/]+$') as filename,
    size,
    blobid
FROM fs
WHERE main = true 
  AND blobid IS NOT NULL
  AND size IS NOT NULL;

-- Create index on temp table for fast lookups
\echo 'Creating index on temp table...'
CREATE INDEX idx_main_true_blobids ON main_true_blobids(filename, size);

-- Analyze temp table for query optimization
ANALYZE main_true_blobids;

-- Show how many matches we can find
\echo 'Checking potential matches...'
WITH potential_matches AS (
    SELECT 
        substring(f.pth from '[^/]+$') as filename,
        f.size
    FROM fs f
    WHERE f.main = false 
      AND f.blobid IS NULL
      AND f.size IS NOT NULL
      AND EXISTS (
          SELECT 1 
          FROM main_true_blobids m
          WHERE m.filename = substring(f.pth from '[^/]+$')
            AND m.size = f.size
      )
    LIMIT 10000
)
SELECT 
    COUNT(*) as sample_files_with_matches,
    COUNT(DISTINCT (filename, size)) as unique_filename_size_pairs
FROM potential_matches;

-- Now do the actual update
\echo 'Performing update (this may take several minutes)...'
WITH update_batch AS (
    UPDATE fs
    SET blobid = m.blobid
    FROM main_true_blobids m
    WHERE fs.main = false
      AND fs.blobid IS NULL
      AND substring(fs.pth from '[^/]+$') = m.filename
      AND fs.size = m.size
    RETURNING fs.pth
)
SELECT COUNT(*) as files_updated FROM update_batch;

-- Show final statistics
\echo 'Final statistics:'
SELECT 
    main,
    COUNT(*) as total_files,
    COUNT(CASE WHEN blobid IS NOT NULL THEN 1 END) as has_blobid,
    COUNT(CASE WHEN blobid IS NULL THEN 1 END) as needs_blobid
FROM fs
GROUP BY main
ORDER BY main DESC;

-- More detailed stats on what's left
\echo 'Remaining main=false files without blobids:'
SELECT 
    COUNT(*) as total_remaining,
    COUNT(CASE WHEN size > 10485760 THEN 1 END) as large_files_over_10mb,
    COUNT(CASE WHEN size > 1048576 THEN 1 END) as files_over_1mb,
    COUNT(CASE WHEN size <= 1048576 THEN 1 END) as small_files,
    pg_size_pretty(SUM(size)) as total_size
FROM fs
WHERE main = false 
  AND blobid IS NULL;

\echo 'Update complete!'