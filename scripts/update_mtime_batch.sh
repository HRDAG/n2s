#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/update_mtime_batch.sh
#
# Load TSV files and update database in batches

set -euo pipefail

# Database connection
DB_HOST="snowball"
DB_USER="pball"
DB_NAME="pbnas"

# Paths
LOG_DIR="$HOME/.n2s/mtime_population"
BATCH_LOG="$LOG_DIR/batch_update.log"
BATCH_SIZE=100000

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== BATCH MTIME UPDATE ===${NC}"
echo "[$(date)] Starting batch update process" | tee "$BATCH_LOG"

# Check if TSV files exist
TSV_COUNT=$(ls -1 "$LOG_DIR"/*.tsv 2>/dev/null | wc -l)
if [[ "$TSV_COUNT" -eq 0 ]]; then
    echo -e "${RED}No TSV files found in $LOG_DIR${NC}"
    exit 1
fi

echo -e "${BLUE}Found $TSV_COUNT TSV files to process${NC}"
ls -lh "$LOG_DIR"/*.tsv

# Create or recreate temp table
echo -e "\n${YELLOW}Creating temp_mtime table...${NC}"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
DROP TABLE IF EXISTS temp_mtime;
CREATE TABLE temp_mtime (
    path TEXT,
    mtime DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_temp_mtime_path ON temp_mtime(path);
EOF

# Load each TSV file
for tsv_file in "$LOG_DIR"/*.tsv; do
    if [[ ! -f "$tsv_file" ]]; then
        continue
    fi
    
    volume_name=$(basename "$tsv_file" .tsv)
    line_count=$(wc -l < "$tsv_file")
    
    echo -e "\n${BLUE}Loading $volume_name: $(printf "%'d" $line_count) records${NC}"
    echo "[$(date)] Loading $tsv_file ($line_count lines)" >> "$BATCH_LOG"
    
    # Use COPY to load data
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
\copy temp_mtime(mtime, path) FROM '$tsv_file' WITH (FORMAT text, DELIMITER E'\t')
EOF
    
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✓ Loaded $volume_name successfully${NC}"
    else
        echo -e "${RED}✗ Failed to load $volume_name${NC}"
        echo "[$(date)] ERROR: Failed to load $tsv_file" >> "$BATCH_LOG"
    fi
done

# Get total count in temp table
TEMP_COUNT=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM temp_mtime")
echo -e "\n${GREEN}Total records in temp_mtime: $(printf "%'d" $TEMP_COUNT)${NC}"

# Update fs table in batches
echo -e "\n${YELLOW}Updating fs table in batches of $(printf "%'d" $BATCH_SIZE)...${NC}"

# Track progress
TOTAL_UPDATED=0
BATCH_NUM=0

while true; do
    BATCH_NUM=$((BATCH_NUM + 1))
    
    echo -n "[$(date)] Batch $BATCH_NUM: " | tee -a "$BATCH_LOG"
    
    # Update a batch and get count of updated rows
    UPDATED=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t <<EOF
BEGIN;
-- Create temp table with this batch
DROP TABLE IF EXISTS batch_paths;
CREATE TEMP TABLE batch_paths AS
SELECT path FROM temp_mtime 
WHERE path NOT IN (
    SELECT pth FROM fs WHERE mtime IS NOT NULL
)
LIMIT $BATCH_SIZE;

-- Update fs table
WITH updated AS (
    UPDATE fs 
    SET mtime = t.mtime
    FROM temp_mtime t
    WHERE fs.pth = t.path
    AND fs.pth IN (SELECT path FROM batch_paths)
    RETURNING 1
)
SELECT COUNT(*) FROM updated;
COMMIT;
EOF
)
    
    UPDATED=$(echo "$UPDATED" | xargs)
    TOTAL_UPDATED=$((TOTAL_UPDATED + UPDATED))
    
    echo "Updated $UPDATED records (Total: $(printf "%'d" $TOTAL_UPDATED))" | tee -a "$BATCH_LOG"
    
    # If we updated fewer records than batch size, we're done
    if [[ "$UPDATED" -lt "$BATCH_SIZE" ]]; then
        echo -e "${GREEN}✓ Completed all updates${NC}"
        break
    fi
    
    # Show progress
    if [[ $((BATCH_NUM % 10)) -eq 0 ]]; then
        # Check how many still need updating
        REMAINING=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM fs WHERE mtime IS NULL AND pth IN (SELECT path FROM temp_mtime)")
        REMAINING=$(echo "$REMAINING" | xargs)
        echo -e "${BLUE}  Progress: $(printf "%'d" $TOTAL_UPDATED) updated, $(printf "%'d" $REMAINING) remaining${NC}"
    fi
done

# Also update work_queue table if it exists
echo -e "\n${YELLOW}Checking work_queue table...${NC}"
WQ_EXISTS=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c \
    "SELECT 1 FROM information_schema.tables WHERE table_name = 'work_queue'" | xargs)

if [[ "$WQ_EXISTS" == "1" ]]; then
    echo "Updating work_queue table in batches..."
    
    WQ_UPDATED=0
    WQ_BATCH_NUM=0
    
    while true; do
        WQ_BATCH_NUM=$((WQ_BATCH_NUM + 1))
        
        echo -n "[$(date)] Work queue batch $WQ_BATCH_NUM: " | tee -a "$BATCH_LOG"
        
        UPDATED=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t <<EOF
BEGIN;
WITH updated AS (
    UPDATE work_queue 
    SET mtime = t.mtime
    FROM temp_mtime t
    WHERE work_queue.pth = t.path
    AND work_queue.mtime IS NULL
    LIMIT $BATCH_SIZE
    RETURNING 1
)
SELECT COUNT(*) FROM updated;
COMMIT;
EOF
)
        
        UPDATED=$(echo "$UPDATED" | xargs)
        WQ_UPDATED=$((WQ_UPDATED + UPDATED))
        
        echo "Updated $UPDATED records (Total: $(printf "%'d" $WQ_UPDATED))" | tee -a "$BATCH_LOG"
        
        if [[ "$UPDATED" -eq 0 ]]; then
            echo -e "${GREEN}✓ Work queue updates completed${NC}"
            break
        fi
    done
fi

# Final statistics
echo -e "\n${GREEN}=== FINAL STATISTICS ===${NC}"
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" <<EOF
SELECT 
    'Files with mtime in fs:' as description,
    COUNT(*) as count
FROM fs 
WHERE mtime IS NOT NULL
UNION ALL
SELECT 
    'Files without mtime in fs:' as description,
    COUNT(*) 
FROM fs 
WHERE mtime IS NULL
UNION ALL
SELECT 
    'Total files in fs:' as description,
    COUNT(*) 
FROM fs;
EOF

echo -e "\n${GREEN}✓ Batch update complete!${NC}"
echo "[$(date)] Batch update completed. Total updated: $TOTAL_UPDATED" >> "$BATCH_LOG"
echo "Log saved to: $BATCH_LOG"