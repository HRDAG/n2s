#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/scripts/check_throughput.py

import psycopg2
import sys
from datetime import datetime

# Configuration (same as pbnas_blob_worker)
DB_HOST = "snowball"
DB_USER = "pball"
DB_NAME = "pbnas"


def get_db_connection():
    """Create database connection with timezone set."""
    conn_string = f"host={DB_HOST} port=5432 user={DB_USER} dbname={DB_NAME} connect_timeout=10"
    conn = psycopg2.connect(conn_string)
    # Set timezone for this session
    with conn.cursor() as cur:
        cur.execute("SET timezone = 'America/Los_Angeles'")
    conn.commit()
    return conn


def check_throughput(start_time=None, end_time=None):
    """Check files processed in time window."""
    conn = get_db_connection()
    
    try:
        with conn.cursor() as cur:
            if start_time and end_time:
                # Specific time window
                query = """
                SELECT COUNT(*) as files_processed
                FROM fs 
                WHERE uploaded >= %s 
                  AND uploaded <= %s
                  AND main = true;
                """
                cur.execute(query, (start_time, end_time))
                result = cur.fetchone()
                files_processed = result[0] if result else 0
                
                print(f"Files processed between {start_time} and {end_time}: {files_processed:,}")
                
                # Calculate rate per hour
                window_minutes = 10  # Assuming 10-minute window
                files_per_hour = (files_processed * 60) / window_minutes
                print(f"Rate: {files_per_hour:.1f} files/hour")
                
            else:
                # Last hour stats
                query = """
                SELECT 
                    COUNT(*) as files_last_hour,
                    MIN(uploaded) as first_upload,
                    MAX(uploaded) as last_upload
                FROM fs 
                WHERE uploaded > NOW() - INTERVAL '1 hour'
                  AND main = true;
                """
                cur.execute(query)
                result = cur.fetchone()
                
                if result and result[0] > 0:
                    files, first, last = result
                    print(f"Files processed in last hour: {files:,}")
                    print(f"Time window: {first} to {last}")
                    
                    # Calculate actual rate
                    if first and last:
                        duration_seconds = (last - first).total_seconds()
                        if duration_seconds > 0:
                            files_per_hour = (files * 3600) / duration_seconds
                            print(f"Average rate: {files_per_hour:.1f} files/hour")
                else:
                    print("No files processed in last hour")
                    
    except Exception as e:
        print(f"Query failed: {e}")
        sys.exit(1)
    finally:
        conn.close()


def main():
    """Check throughput for specified time window or last hour."""
    if len(sys.argv) == 3:
        # Specific time window: check_throughput.py "12:15" "12:25"
        start_time = f"2025-08-23 {sys.argv[1]}:00"
        end_time = f"2025-08-23 {sys.argv[2]}:59"
        check_throughput(start_time, end_time)
    else:
        # Default: last hour
        check_throughput()


if __name__ == "__main__":
    main()