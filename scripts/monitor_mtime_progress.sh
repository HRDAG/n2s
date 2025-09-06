#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.09.04
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/monitor_mtime_progress.sh
#
# Monitor progress of mtime population

set -euo pipefail

# Database connection
DB_HOST="snowball"
DB_USER="pball"
DB_NAME="pbnas"

# Logging
LOG_DIR="$HOME/.n2s/mtime_population"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

while true; do
    # Clear screen and reset cursor
    clear
    echo "==================================================================="
    echo "                    MTIME POPULATION MONITOR"
    echo "==================================================================="
    echo ""
    
    # Get temp_mtime count only (fast query)
    echo -e "${GREEN}Database Status:${NC}"
    
    # Check if temp_mtime exists and count
    TEMP_EXISTS=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'temp_mtime'
        )" 2>/dev/null | xargs)
    
    if [[ "$TEMP_EXISTS" == "t" ]]; then
        IN_TEMP=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "
            SELECT COUNT(*) FROM temp_mtime" 2>/dev/null | xargs)
        echo -e "  Records staged in temp_mtime: $(printf "%'d" ${IN_TEMP:-0})"
    else
        echo -e "  temp_mtime table not created yet"
    fi
    
    # Get count of files with mtime (should be fast with index)
    WITH_MTIME=$(psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT COUNT(*) FROM fs WHERE mtime IS NOT NULL" 2>/dev/null | xargs)
    
    if [[ -n "$WITH_MTIME" ]]; then
        echo -e "  Files with mtime in fs:       $(printf "%'d" $WITH_MTIME)"
    fi
    
    # Check file sizes
    if [[ -d "$LOG_DIR" ]]; then
        echo ""
        echo -e "${YELLOW}TSV File Sizes:${NC}"
        for tsv in "$LOG_DIR"/*.tsv; do
            if [[ -f "$tsv" ]]; then
                SIZE=$(du -h "$tsv" | cut -f1)
                LINES=$(wc -l < "$tsv" 2>/dev/null || echo "0")
                NAME=$(basename "$tsv" .tsv)
                printf "  %-20s %10s  %15s lines\n" "$NAME:" "$SIZE" "$(printf "%'d" $LINES)"
            fi
        done
    fi
    
    # Show recent log entries
    if [[ -f "$LOG_DIR/populate_mtime.log" ]]; then
        echo ""
        echo -e "${GREEN}Recent Activity:${NC}"
        tail -n 3 "$LOG_DIR/populate_mtime.log" | sed 's/^/  /'
    fi
    
    sleep 5
done