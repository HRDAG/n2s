-- Author: PB & Claude
-- Maintainer: PB
-- Original date: 2025.09.04
-- License: (c) HRDAG, 2025, GPL-2 or newer
--
-- ------
-- scripts/update_by_filename_size_safe.sql

-- SAFER VERSION: Update main=false files with blobids based on filename+size matches
-- Only updates when there's a SINGLE consistent blobid for a filename+size pair

\timing on
\echo 'Starting SAFE filename+size based blobid updates'

-- First check for conflicts
\echo 'Checking for filename+size pairs with conflicting blobids...'
WITH blobid_counts AS (
    SELECT 
        substring(pth from '[^/]+$') as filename,
        size,
        blobid,
        COUNT(*) as file_count
    FROM fs
    WHERE main = true 
      AND blobid IS NOT NULL
      AND size IS NOT NULL
    GROUP BY substring(pth from '[^/]+$'), size, blobid
),
conflicts AS (
    SELECT 
        filename,
        size,
        COUNT(DISTINCT blobid) as different_blobids,
        array_agg(DISTINCT blobid) as blobid_list
    FROM blobid_counts
    GROUP BY filename, size
    HAVING COUNT(DISTINCT blobid) > 1
)
SELECT 
    COUNT(*) as conflicting_pairs,
    SUM(different_blobids) as total_different_blobids
FROM conflicts;

-- Show some examples of conflicts
\echo 'Sample conflicts (same filename+size, different blobids):'
WITH blobid_counts AS (
    SELECT 
        substring(pth from '[^/]+$') as filename,
        size,
        blobid,
        COUNT(*) as file_count
    FROM fs
    WHERE main = true 
      AND blobid IS NOT NULL
      AND size IS NOT NULL
    GROUP BY substring(pth from '[^/]+$'), size, blobid
),
conflicts AS (
    SELECT 
        filename,
        size,
        COUNT(DISTINCT blobid) as different_blobids,
        array_agg(DISTINCT substring(blobid, 1, 16)) as blobid_prefixes
    FROM blobid_counts
    GROUP BY filename, size
    HAVING COUNT(DISTINCT blobid) > 1
)
SELECT * FROM conflicts 
ORDER BY different_blobids DESC, size DESC
LIMIT 10;

-- Create temp table with ONLY unambiguous matches
\echo 'Creating temp table of unambiguous filename+size+blobid mappings...'
CREATE TEMP TABLE safe_blobids AS
WITH unique_mappings AS (
    SELECT 
        substring(pth from '[^/]+$') as filename,
        size,
        blobid,
        COUNT(*) as file_count
    FROM fs
    WHERE main = true 
      AND blobid IS NOT NULL
      AND size IS NOT NULL
    GROUP BY substring(pth from '[^/]+$'), size, blobid
),
unambiguous AS (
    SELECT 
        filename,
        size,
        MAX(blobid) as blobid,  -- Will only have one value
        SUM(file_count) as total_files
    FROM unique_mappings
    GROUP BY filename, size
    HAVING COUNT(DISTINCT blobid) = 1  -- Only one blobid for this filename+size
)
SELECT * FROM unambiguous;

-- Create index
CREATE INDEX idx_safe_blobids ON safe_blobids(filename, size);
ANALYZE safe_blobids;

-- Show stats
\echo 'Safe mapping statistics:'
SELECT 
    COUNT(*) as unique_filename_size_pairs,
    SUM(total_files) as total_main_true_files
FROM safe_blobids;

-- Check how many main=false files we can safely update
\echo 'Checking how many main=false files can be SAFELY updated...'
SELECT COUNT(*) as safe_update_candidates
FROM fs f
WHERE f.main = false 
  AND f.blobid IS NULL
  AND f.size IS NOT NULL
  AND EXISTS (
      SELECT 1 
      FROM safe_blobids s
      WHERE s.filename = substring(f.pth from '[^/]+$')
        AND s.size = f.size
  );

-- Do the update (only safe matches)
\echo 'Performing SAFE update...'
WITH update_batch AS (
    UPDATE fs
    SET blobid = s.blobid
    FROM safe_blobids s
    WHERE fs.main = false
      AND fs.blobid IS NULL
      AND substring(fs.pth from '[^/]+$') = s.filename
      AND fs.size = s.size
    RETURNING fs.pth
)
SELECT COUNT(*) as files_safely_updated FROM update_batch;

-- Final statistics
\echo 'Final statistics:'
SELECT 
    main,
    COUNT(*) as total_files,
    COUNT(CASE WHEN blobid IS NOT NULL THEN 1 END) as has_blobid,
    COUNT(CASE WHEN blobid IS NULL THEN 1 END) as needs_blobid
FROM fs
GROUP BY main
ORDER BY main DESC;

\echo 'Safe update complete!'
\echo 'Note: Files with ambiguous filename+size matches were NOT updated.'