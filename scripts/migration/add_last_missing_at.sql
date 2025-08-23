-- Author: PB and Claude
-- Date: 2025-08-23
-- License: (c) HRDAG, 2025, GPL-2 or newer
--
-- ------
-- n2s/scripts/migration/add_last_missing_at.sql

-- Add timezone-aware timestamp column to track when files were last found missing
-- This allows pbnas_blob_worker to skip files that don't exist and avoid infinite loops

-- Set timezone for this session
SET timezone = 'America/Los_Angeles';

-- Add the column (safe operation - nullable column with no default)
ALTER TABLE fs ADD COLUMN IF NOT EXISTS last_missing_at TIMESTAMP WITH TIME ZONE;

-- Create index for efficient querying (worker filters on this column)
CREATE INDEX IF NOT EXISTS idx_fs_last_missing_at
ON fs(last_missing_at)
WHERE last_missing_at IS NOT NULL;

-- Show current stats
SELECT
    COUNT(*) as total_files,
    COUNT(*) FILTER (WHERE main = true) as main_files,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL) as unprocessed_main_files,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL AND last_missing_at IS NULL) as ready_to_process
FROM fs;

-- Show which trees have unprocessed files
SELECT
    tree,
    COUNT(*) FILTER (WHERE main = true AND blobid IS NULL AND last_missing_at IS NULL) as ready_to_process
FROM fs
WHERE tree IN ('osxgather', 'dump-2019')
GROUP BY tree
ORDER BY ready_to_process DESC;

-- Optional: After running pbnas_blob_worker, check which files were marked as missing
-- (Run this query later to see what files are missing from the filesystem)
/*
SELECT
    tree,
    COUNT(*) as missing_files,
    MIN(last_missing_at) as first_missing,
    MAX(last_missing_at) as last_missing
FROM fs
WHERE last_missing_at IS NOT NULL
GROUP BY tree
ORDER BY missing_files DESC;

-- Show some example missing file paths for investigation
SELECT tree, pth, last_missing_at
FROM fs
WHERE last_missing_at IS NOT NULL
ORDER BY last_missing_at DESC
LIMIT 20;
*/
