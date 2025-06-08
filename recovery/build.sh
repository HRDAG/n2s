#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# recovery/build.sh

set -e

echo "Building n2s disaster recovery tools..."

# Ensure we're in the recovery directory
cd "$(dirname "$0")"

# Initialize Go module if needed
if [ ! -f go.sum ]; then
    go mod tidy
fi

# Create bin directory
mkdir -p bin

# Build for all platforms
echo "Building for Linux amd64..."
GOOS=linux GOARCH=amd64 go build -o bin/decrypt-linux-amd64 .

echo "Building for Linux arm64..."
GOOS=linux GOARCH=arm64 go build -o bin/decrypt-linux-arm64 .

echo "Building for Windows amd64..."
GOOS=windows GOARCH=amd64 go build -o bin/decrypt-windows-amd64.exe .

echo "Building for macOS amd64..."
GOOS=darwin GOARCH=amd64 go build -o bin/decrypt-macos-amd64 .

echo "Building for macOS arm64..."
GOOS=darwin GOARCH=arm64 go build -o bin/decrypt-macos-arm64 .

echo "Build complete. Binaries available in bin/"
ls -la bin/