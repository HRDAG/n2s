#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# recovery/disaster_recovery.sh

set -e

# Auto-detect platform and binary
PLATFORM=""
case "$(uname -s)" in
    Linux*)     
        case "$(uname -m)" in
            x86_64) PLATFORM="linux-amd64" ;;
            aarch64|arm64) PLATFORM="linux-arm64" ;;
            *) echo "Unsupported Linux architecture: $(uname -m)" >&2; exit 1 ;;
        esac
        ;;
    Darwin*)    
        case "$(uname -m)" in
            x86_64) PLATFORM="macos-amd64" ;;
            arm64) PLATFORM="macos-arm64" ;;
            *) echo "Unsupported macOS architecture: $(uname -m)" >&2; exit 1 ;;
        esac
        ;;
    CYGWIN*|MINGW*|MSYS*) PLATFORM="windows-amd64.exe" ;;
    *) echo "Unsupported platform: $(uname -s)" >&2; exit 1 ;;
esac

# Find decrypt binary
SCRIPT_DIR="$(dirname "$0")"
DECRYPT_BIN="$SCRIPT_DIR/bin/decrypt-$PLATFORM"

if [ ! -f "$DECRYPT_BIN" ]; then
    echo "Error: Decrypt binary not found: $DECRYPT_BIN" >&2
    echo "Run: cd recovery && ./build.sh" >&2
    exit 1
fi

# Usage check
if [ $# -lt 2 ]; then
    echo "Usage: $0 <blob_id> <passphrase> [output_file]" >&2
    echo "" >&2
    echo "Example:" >&2
    echo "  $0 a1b2c3d4e5f6... mypassword recovered_file.txt" >&2
    echo "  $0 a1b2c3d4e5f6... mypassword  # outputs to stdout" >&2
    exit 1
fi

BLOB_ID="$1"
PASSPHRASE="$2"
OUTPUT_FILE="$3"

# Function to recover a single blob
recover_blob() {
    local blob_id="$1"
    local passphrase="$2"
    local output_file="$3"
    
    echo "Recovering blob: $blob_id" >&2
    
    # Get blob (assumes 'rget' command exists)
    if ! command -v rget >/dev/null 2>&1; then
        echo "Error: 'rget' command not found. Please implement blob retrieval." >&2
        exit 1
    fi
    
    # Create temporary file for blob
    TEMP_BLOB=$(mktemp)
    trap "rm -f $TEMP_BLOB" EXIT
    
    # Retrieve blob
    if ! rget "$blob_id" > "$TEMP_BLOB"; then
        echo "Error: Failed to retrieve blob $blob_id" >&2
        exit 1
    fi
    
    # Check if it's valid JSON
    if ! jq empty "$TEMP_BLOB" 2>/dev/null; then
        echo "Error: Retrieved blob is not valid JSON" >&2
        exit 1
    fi
    
    # Extract metadata
    local path=$(jq -r '.metadata.path' "$TEMP_BLOB")
    local size=$(jq -r '.metadata.size' "$TEMP_BLOB")
    local file_hash=$(jq -r '.metadata.file_hash' "$TEMP_BLOB")
    
    echo "  Path: $path" >&2
    echo "  Size: $size bytes" >&2
    echo "  Hash: $file_hash" >&2
    
    # Extract encrypted content and decrypt
    local encrypted_b64=$(jq -r '.encrypted_content' "$TEMP_BLOB")
    
    # Decrypt and decompress pipeline
    if [ -n "$output_file" ]; then
        # Output to file
        if ! echo "$encrypted_b64" | "$DECRYPT_BIN" "$blob_id" "$passphrase" /dev/stdin | lz4 -d > "$output_file"; then
            echo "Error: Decryption/decompression failed" >&2
            exit 1
        fi
        echo "Recovered to: $output_file" >&2
    else
        # Output to stdout
        if ! echo "$encrypted_b64" | "$DECRYPT_BIN" "$blob_id" "$passphrase" /dev/stdin | lz4 -d; then
            echo "Error: Decryption/decompression failed" >&2
            exit 1
        fi
    fi
}

# Check dependencies
if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required but not installed." >&2
    exit 1
fi

if ! command -v lz4 >/dev/null 2>&1; then
    echo "Error: lz4 is required but not installed." >&2
    exit 1
fi

# Recover the blob
recover_blob "$BLOB_ID" "$PASSPHRASE" "$OUTPUT_FILE"