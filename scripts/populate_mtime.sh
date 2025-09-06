#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/populate_mtime.sh
#
# Efficiently populate mtime values in database for 46M files
# Uses parallel processing across physical drives to minimize I/O contention

set -euo pipefail

# Database connection
DB_HOST="snowball"
DB_USER="pball"
DB_NAME="pbnas"

# Logging
LOG_DIR="$HOME/.n2s/mtime_population"
mkdir -p "$LOG_DIR"
MAIN_LOG="$LOG_DIR/populate_mtime.log"
ERROR_LOG="$LOG_DIR/errors.log"

# Progress tracking
CHECKPOINT_FILE="$LOG_DIR/checkpoint.txt"

echo "[$(date)] Starting mtime population process" | tee "$MAIN_LOG"

# Create temporary table for bulk loading
echo "[$(date)] Creating temporary table..." | tee -a "$MAIN_LOG"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
DROP TABLE IF EXISTS temp_mtime;
CREATE TABLE temp_mtime (
    path TEXT PRIMARY KEY,
    mtime DOUBLE PRECISION NOT NULL
);
EOF

# Function to process a volume
process_volume() {
    local volume_path="$1"
    local volume_name="$(basename "$volume_path")"
    local output_file="$LOG_DIR/${volume_name}.tsv"
    local progress_file="$LOG_DIR/${volume_name}.progress"
    
    echo "[$(date)] Processing $volume_name..." | tee -a "$MAIN_LOG"
    
    # Use fd with -x (--exec) to directly call gstat for each file
    # Output format: mtime\034path\n
    fd --type f --no-ignore --hidden . "$volume_path" \
        -x gstat --printf "%Y\034%n\n" {} >> "$output_file" 2>>"$ERROR_LOG"
    
    local line_count=$(wc -l < "$output_file")
    echo "[$(date)] $volume_name: Found $line_count files" | tee -a "$MAIN_LOG"
    
    # Load into database using COPY
    echo "[$(date)] Loading $volume_name data into database..." | tee -a "$MAIN_LOG"
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
\copy temp_mtime(mtime, path) FROM '$output_file' WITH (FORMAT text, DELIMITER E'\t')
EOF
    
    echo "[$(date)] $volume_name: Complete" | tee -a "$MAIN_LOG"
    echo "$volume_name" >> "$CHECKPOINT_FILE"
}

# Process disk9 (osxgather) - HFS drive
process_volume "/Volumes/osxgather" &
PID_DISK9=$!

# Process disk7 (dump-2019)  
process_volume "/Volumes/dump-2019" &
PID_DISK7=$!

# Process disk4 volumes sequentially (same physical drive)
(
    process_volume "/Volumes/archives-2019"
    process_volume "/Volumes/backup"
) &
PID_DISK4=$!

# Wait for all background processes
echo "[$(date)] Waiting for all drives to complete..." | tee -a "$MAIN_LOG"
wait $PID_DISK9 $PID_DISK7 $PID_DISK4

# Update the main fs table from temp table
echo "[$(date)] Updating fs table with mtime values..." | tee -a "$MAIN_LOG"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
BEGIN;
UPDATE fs 
SET mtime = t.mtime 
FROM temp_mtime t 
WHERE fs.pth = t.path;

-- Also update work_queue if paths are there
UPDATE work_queue 
SET mtime = t.mtime 
FROM temp_mtime t 
WHERE work_queue.pth = t.path;

COMMIT;
EOF

# Get statistics
echo "[$(date)] Getting statistics..." | tee -a "$MAIN_LOG"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t <<EOF | tee -a "$MAIN_LOG"
SELECT 
    'Files with mtime: ' || COUNT(*) 
FROM fs 
WHERE mtime IS NOT NULL;

SELECT 
    'Files without mtime: ' || COUNT(*) 
FROM fs 
WHERE mtime IS NULL;
EOF

echo "[$(date)] Process complete!" | tee -a "$MAIN_LOG"
echo "Logs available at: $LOG_DIR"