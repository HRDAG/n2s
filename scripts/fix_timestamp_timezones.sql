-- Author: PB & Claude
-- Maintainer: PB
-- Original date: 2025.09.02
-- License: (c) HRDAG, 2025, GPL-2 or newer
--
-- ------
-- scripts/fix_timestamp_timezones.sql

-- Migration to fix timestamp columns that should be timezone-aware
-- This fixes the "seriously dumb mistake" where some timestamps lack timezone info

\timing on

BEGIN;

-- Show current column types before migration
\echo 'BEFORE MIGRATION:'
SELECT 
    table_name, 
    column_name, 
    data_type 
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name IN ('fs', 'work_queue') 
  AND column_name LIKE '%at' OR column_name = 'calcd'
ORDER BY table_name, column_name;

\echo ''
\echo 'Starting migration...'

-- Fix fs.calcd column
\echo 'Fixing fs.calcd column (timestamp without time zone -> timestamp with time zone)'
ALTER TABLE fs 
ALTER COLUMN calcd TYPE timestamp with time zone 
USING calcd AT TIME ZONE 'UTC';

-- Fix work_queue.claimed_at column
\echo 'Fixing work_queue.claimed_at column (timestamp without time zone -> timestamp with time zone)'
ALTER TABLE work_queue 
ALTER COLUMN claimed_at TYPE timestamp with time zone 
USING claimed_at AT TIME ZONE 'UTC';

-- Fix work_queue.created_at column and update default
\echo 'Fixing work_queue.created_at column (timestamp without time zone -> timestamp with time zone)'
ALTER TABLE work_queue 
ALTER COLUMN created_at TYPE timestamp with time zone 
USING created_at AT TIME ZONE 'UTC';

-- Update the default for created_at to be timezone-aware
\echo 'Updating work_queue.created_at default to NOW() with timezone'
ALTER TABLE work_queue 
ALTER COLUMN created_at SET DEFAULT NOW();

-- Show column types after migration
\echo ''
\echo 'AFTER MIGRATION:'
SELECT 
    table_name, 
    column_name, 
    data_type,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'public' 
  AND table_name IN ('fs', 'work_queue') 
  AND (column_name LIKE '%at' OR column_name = 'calcd')
ORDER BY table_name, column_name;

-- Show sample data to verify conversion worked correctly
\echo ''
\echo 'SAMPLE DATA VERIFICATION:'
\echo 'fs table - recent calcd values:'
SELECT calcd, COUNT(*) 
FROM fs 
WHERE calcd IS NOT NULL 
GROUP BY calcd 
ORDER BY calcd DESC 
LIMIT 5;

\echo ''
\echo 'work_queue table - recent created_at values:'
SELECT created_at, COUNT(*) 
FROM work_queue 
WHERE created_at IS NOT NULL 
GROUP BY created_at 
ORDER BY created_at DESC 
LIMIT 5;

\echo ''
\echo 'work_queue table - claimed_at values (if any):'
SELECT claimed_at, COUNT(*) 
FROM work_queue 
WHERE claimed_at IS NOT NULL 
GROUP BY claimed_at 
ORDER BY claimed_at DESC 
LIMIT 5;

\echo ''
\echo 'Migration completed successfully!'
\echo 'All timestamp columns are now timezone-aware (timestamp with time zone)'

COMMIT;
