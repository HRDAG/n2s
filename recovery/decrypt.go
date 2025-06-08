// Author: PB & Claude
// Maintainer: PB
// Original date: 2025.05.13
// License: (c) HRDAG, 2025, GPL-2 or newer
//
// ------
// recovery/decrypt.go

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/pbkdf2"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <blobid> <password> <encrypted_b64>\n", os.Args[0])
		os.Exit(1)
	}

	blobid := os.Args[1]
	password := os.Args[2]
	encryptedB64 := os.Args[3]

	// Decode blobid from hex
	blobBytes, err := hex.DecodeString(blobid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding blobid: %v\n", err)
		os.Exit(1)
	}

	// Extract salt (first 16 bytes) and nonce (last 12 bytes)
	salt := blobBytes[:16]
	nonce := blobBytes[len(blobBytes)-12:]

	// Derive key using PBKDF2-SHA256
	key := pbkdf2.Key([]byte(password), salt, 100000, 32, sha256.New)

	// Create ChaCha20-Poly1305 cipher
	cipher, err := chacha20poly1305.New(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating cipher: %v\n", err)
		os.Exit(1)
	}

	// Decode base64 encrypted data
	encryptedData, err := base64.StdEncoding.DecodeString(encryptedB64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding base64: %v\n", err)
		os.Exit(1)
	}

	// Decrypt
	plaintext, err := cipher.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Decryption failed: %v\n", err)
		os.Exit(1)
	}

	// Write plaintext to stdout
	os.Stdout.Write(plaintext)
}