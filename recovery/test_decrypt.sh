#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# recovery/test_decrypt.sh

set -e

VERBOSE=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        *)
            break
            ;;
    esac
done

if [ $# -ne 2 ]; then
    echo "Usage: $0 [--verbose] <blob_file> <passphrase>" >&2
    echo "Example: $0 ../blobs/abc123def456 '123456'" >&2
    echo "         $0 --verbose ../blobs/abc123def456 '123456'" >&2
    exit 1
fi

BLOB_FILE="$1"
PASSPHRASE="$2"
BLOBID=$(basename "$BLOB_FILE")

# Check dependencies
for cmd in jq lz4 b3sum touch; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Error: $cmd is required but not installed." >&2
        exit 1
    fi
done

# Find decrypt binary
if [ -n "$GO_DECRYPT" ]; then
    # Use environment variable if set
    DECRYPT_BIN="$GO_DECRYPT"
    if [ ! -f "$DECRYPT_BIN" ]; then
        echo "Error: GO_DECRYPT binary not found: $DECRYPT_BIN" >&2
        exit 1
    fi
else
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
        *) echo "Unsupported platform: $(uname -s)" >&2; exit 1 ;;
    esac

    DECRYPT_BIN="$(dirname "$0")/bin/decrypt-$PLATFORM"
    if [ ! -f "$DECRYPT_BIN" ]; then
        echo "Error: Decrypt binary not found: $DECRYPT_BIN" >&2
        echo "Try setting GO_DECRYPT environment variable to the decrypt binary path" >&2
        exit 1
    fi
fi

echo "Testing blob decryption..."
echo "Blob file: $BLOB_FILE"
echo "Blobid: $BLOBID"

# 1. Extract metadata from blob
echo "Extracting metadata..."
PATH_FULL=$(jq -r '.metadata.path' "$BLOB_FILE")
SIZE=$(jq -r '.metadata.size' "$BLOB_FILE") 
MTIME=$(jq -r '.metadata.timestamp' "$BLOB_FILE")
EXPECTED_HASH=$(jq -r '.metadata.file_hash' "$BLOB_FILE")

# Get just filename (not full path)
FILENAME=$(basename "$PATH_FULL")

echo "  Original path: $PATH_FULL"
echo "  Filename: $FILENAME"
echo "  Size: $SIZE bytes"
echo "  Mtime: $MTIME"
echo "  Expected hash: $EXPECTED_HASH"

# 2. Extract and decrypt content
echo "Decrypting content..."
ENCRYPTED_B64=$(jq -r '.encrypted_content' "$BLOB_FILE" | tr -d '\n\r ')

if [ "$VERBOSE" = true ]; then
    echo "Debug info:"
    echo "  Blobid length: ${#BLOBID}"
    echo "  Base64 length: ${#ENCRYPTED_B64}"
    echo "  Base64 first 50 chars: ${ENCRYPTED_B64:0:50}..."
    echo "  Base64 last 20 chars: ...${ENCRYPTED_B64: -20}"
    echo "  Running: $DECRYPT_BIN $BLOBID [passphrase] /dev/stdin"
fi

# Decrypt and decompress to temp file
TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

if [ "$VERBOSE" = true ]; then
    echo "Attempting decryption..."
fi

if ! "$DECRYPT_BIN" "$BLOBID" "$PASSPHRASE" "$ENCRYPTED_B64" | lz4 -d > "$TEMP_FILE"; then
    echo "Error: Decryption/decompression failed" >&2
    if [ "$VERBOSE" = true ]; then
        echo "Debug: Trying manual steps..."
        echo "1. Testing base64 decode:"
        echo -n "$ENCRYPTED_B64" | base64 -d | wc -c || echo "Base64 decode failed"
        echo "2. Testing decrypt without lz4:"
        "$DECRYPT_BIN" "$BLOBID" "$PASSPHRASE" "$ENCRYPTED_B64" | wc -c || echo "Decrypt failed"
    fi
    exit 1
fi

# 3. Set the mtime on recovered file
echo "Setting file timestamp..."
touch -d "@$MTIME" "$TEMP_FILE"

# 4. Verify file hash
echo "Verifying file integrity..."
ACTUAL_HASH=$(b3sum "$TEMP_FILE" | cut -d' ' -f1)

echo "  Expected: $EXPECTED_HASH"
echo "  Actual:   $ACTUAL_HASH"

if [ "$EXPECTED_HASH" = "$ACTUAL_HASH" ]; then
    echo "✓ Hash verification PASSED"
else
    echo "✗ Hash verification FAILED"
    exit 1
fi

# 5. Copy to final filename with correct mtime
echo "Creating recovered file: $FILENAME"
cp "$TEMP_FILE" "$FILENAME"
touch -d "@$MTIME" "$FILENAME"

# Show final result
echo "Recovery complete:"
echo "  File: $FILENAME"
echo "  Size: $(wc -c < "$FILENAME") bytes"
echo "  Mtime: $(date -d "@$MTIME" '+%Y-%m-%d %H:%M:%S')"
echo "  Hash: $ACTUAL_HASH"

echo "✓ Test PASSED - blob successfully decrypted and verified"