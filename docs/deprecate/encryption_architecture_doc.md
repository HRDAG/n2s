# Filesystem Encryption Architecture Decision

## Executive Summary

For a filesystem-like storage mechanism with encryption requirements, we recommend a **dual-encryption approach**:

- **File Contents**: pyrage (Python bindings for rage/age format)
- **File Paths**: AES-256-ECB with deterministic key derivation

This approach balances security, interoperability, and disaster recovery requirements.

---

## File Content Encryption: pyrage/rage

### Decision Rationale

**Why rage/age format:**
- **Interoperability**: Files encrypted with pyrage can be decrypted by command-line `age` or `rage` tools
- **Modern cryptography**: Uses ChaCha20-Poly1305 with X25519 key exchange
- **Simplicity**: No configuration options, good defaults, minimal attack surface  
- **Standardized format**: Well-defined specification, multiple implementations

**Why pyrage over other Python bindings:**
- **Production-ready**: Mature, actively maintained with security updates
- **Performance**: Rust-based implementation is much faster than pure Python alternatives
- **Type safety**: Full type hints for better development experience
- **API quality**: Clean, explicit interface with proper error handling

**Performance considerations:**
- rage is ~2x slower than Go age implementation for large files
- For filesystem storage, this is acceptable as I/O is typically the bottleneck
- Memory usage: pyrage loads entire file into memory (not streaming)

### Implementation

```python
from pyrage import passphrase

# File encryption
def encrypt_file(input_path: str, output_path: str, password: str):
    with open(input_path, "rb") as f:
        data = f.read()
    
    encrypted = passphrase.encrypt(data, password)
    
    with open(output_path, "wb") as f:
        f.write(encrypted)

# File decryption  
def decrypt_file(input_path: str, output_path: str, password: str):
    with open(input_path, "rb") as f:
        encrypted_data = f.read()
    
    decrypted = passphrase.decrypt(encrypted_data, password)
    
    with open(output_path, "wb") as f:
        f.write(decrypted)
```

### Command-Line Recovery

Files encrypted with pyrage are fully compatible with standard age/rage tools:

```bash
# Decrypt with age
age -d -p encrypted_file.age > decrypted_file.txt

# Decrypt with rage  
rage -d -p encrypted_file.age > decrypted_file.txt

# Both will prompt for the same password used in Python
```

### Alternative Evaluation

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **pyrage** | Modern crypto, interoperable, maintained | 2x slower than age, memory usage | ✅ **Chosen** |
| **page** | Simple API, Rust-based | Less mature, limited documentation | ❌ Too new |
| **age/pyage** | Pure Python | Explicitly marked insecure by author | ❌ Not secure |
| **libsodium** | Excellent performance | No standard format, custom implementation needed | ❌ Too complex |
| **GPG** | Universal compatibility | Complex, large attack surface | ❌ Overkill |

---

## File Path Encryption: AES-256-ECB

### Decision Rationale

**Why deterministic encryption:**
- Same filepath must always encrypt to the same result
- Enables consistent S3 object keys or filesystem paths
- Required for filesystem-like behavior

**Why AES-ECB despite cryptographic weaknesses:**
- **Universal recovery**: Any system with OpenSSL can decrypt paths
- **Simplicity**: One-liner command for disaster recovery
- **Adequate security**: Filepaths are metadata, not sensitive content
- **Deterministic**: Perfect for our use case requirements

**Security trade-off justification:**
- ECB mode reveals patterns when encrypting similar data
- For filepaths, this is acceptable vs. requiring specialized tools for recovery
- File contents use strong encryption (age format)
- Primary threat is unauthorized access to storage, not cryptanalysis of paths

### Implementation

```python
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib

class PathEncryptor:
    def __init__(self, password: str):
        # Derive 256-bit key from password
        self.key = hashlib.sha256(password.encode()).digest()
    
    def _pad(self, data: bytes) -> bytes:
        """PKCS7 padding to 16-byte blocks"""
        pad_len = 16 - (len(data) % 16)
        return data + bytes([pad_len]) * pad_len
    
    def _unpad(self, data: bytes) -> bytes:
        """Remove PKCS7 padding"""
        pad_len = data[-1]
        return data[:-pad_len]
    
    def encrypt_path(self, path: str) -> str:
        """Encrypt filepath to hex string suitable for S3 keys"""
        padded = self._pad(path.encode('utf-8'))
        cipher = Cipher(algorithms.AES(self.key), modes.ECB())
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()
        return encrypted.hex()
    
    def decrypt_path(self, encrypted_hex: str) -> str:
        """Decrypt hex string back to original filepath"""
        encrypted = bytes.fromhex(encrypted_hex)
        cipher = Cipher(algorithms.AES(self.key), modes.ECB())
        decryptor = cipher.decryptor()
        decrypted = decryptor.update(encrypted) + decryptor.finalize()
        return self._unpad(decrypted).decode('utf-8')
```

### Command-Line Recovery

Disaster recovery without Python using OpenSSL:

```bash
#!/bin/bash
# path_recovery.sh - Decrypt encrypted filepath

ENCRYPTED_HEX="$1"
PASSWORD="$2"

# Derive AES-256 key from password (same as Python implementation)
KEY=$(echo -n "$PASSWORD" | openssl dgst -sha256 | cut -d' ' -f2)

# Decrypt the hex-encoded path
echo "$ENCRYPTED_HEX" | xxd -r -p | openssl enc -d -aes-256-ecb -K "$KEY" -nopad

echo  # Add newline
```

**Usage:**
```bash
./path_recovery.sh "a7f3d9e2b8c1547629384756ab2c9d8e7f3a1b4c5d6e8f9a0b1c2d3e4f567890" "mypassword"
# Output: documents/secret/file.txt
```

### Alternative Evaluation

| Option | Recovery Simplicity | Security | Deterministic | Verdict |
|--------|-------------------|----------|---------------|---------|
| **AES-ECB** | ✅ OpenSSL everywhere | ⚠️ Weak but adequate | ✅ Yes | ✅ **Chosen** |
| **AES-SIV** | ❌ Specialized tools only | ✅ Excellent | ✅ Yes | ❌ Recovery too hard |
| **AES-CBC (fixed IV)** | ✅ OpenSSL | ⚠️ Better than ECB | ✅ Yes | ⚠️ Close second |
| **HMAC-SHA256** | ✅ Universal | ✅ Good | ✅ Yes | ❌ One-way only |
| **ChaCha20** | ❌ Limited tools | ✅ Excellent | ✅ Yes | ❌ Recovery harder |

---

## Complete System Architecture

### Encryption Workflow

```python
class FilesystemEncryption:
    def __init__(self, password: str):
        self.path_encryptor = PathEncryptor(password)
        self.content_password = password
    
    def store_file(self, original_path: str, content: bytes) -> str:
        """Store file with encrypted path and content"""
        # Encrypt the path for S3 key
        encrypted_path = self.path_encryptor.encrypt_path(original_path)
        
        # Encrypt content with age format
        encrypted_content = passphrase.encrypt(content, self.content_password)
        
        # Store to S3 or filesystem
        s3_key = f"encrypted/{encrypted_path}.age"
        # s3.put_object(Key=s3_key, Body=encrypted_content)
        
        return s3_key
    
    def retrieve_file(self, s3_key: str) -> tuple[str, bytes]:
        """Retrieve and decrypt file"""
        # Extract encrypted path from S3 key
        encrypted_path = s3_key.replace("encrypted/", "").replace(".age", "")
        
        # Decrypt the path
        original_path = self.path_encryptor.decrypt_path(encrypted_path)
        
        # Retrieve and decrypt content
        # encrypted_content = s3.get_object(Key=s3_key)['Body'].read()
        # decrypted_content = passphrase.decrypt(encrypted_content, self.content_password)
        
        return original_path, decrypted_content
```

### Disaster Recovery Documentation

Create `RECOVERY.md` for colleagues:

```markdown
# Disaster Recovery Instructions

## File Content Recovery
Use standard age/rage tools:
```bash
age -d -p encrypted_file.age > recovered_file.txt
```

## File Path Recovery  
Use OpenSSL (available on most Unix systems):
```bash
KEY=$(echo -n "PASSWORD" | openssl dgst -sha256 | cut -d' ' -f2)
echo "ENCRYPTED_HEX_PATH" | xxd -r -p | openssl enc -d -aes-256-ecb -K $KEY -nopad
```

## Dependencies
- File contents: `age` or `rage` command-line tool
- File paths: `openssl` and `xxd` (standard on most systems)
```

---

## Security Analysis

### Threat Model
- **Primary threat**: Unauthorized access to storage backend (S3, filesystem)
- **Secondary threat**: Accidental data exposure
- **Out of scope**: Nation-state attackers, side-channel attacks

### Security Properties
- **File contents**: Strong encryption with authenticated format (age/rage)
- **File paths**: Adequate encryption with universal recovery capability
- **Key management**: Single password for both content and path encryption
- **Forward secrecy**: Not provided (acceptable for filesystem storage)

### Known Limitations
- **AES-ECB patterns**: Similar filepaths will have similar encrypted forms
- **Memory usage**: Large files loaded entirely into memory during encryption
- **No streaming**: Cannot encrypt files larger than available RAM
- **Single password**: Compromise affects both paths and contents

---

## Implementation Recommendations

1. **Use pyrage 1.2.5+** for security updates
2. **Document recovery procedures** in project README
3. **Test recovery procedures** on different systems
4. **Consider file size limits** based on available memory
5. **Use strong passwords** with good entropy
6. **Rotate passwords periodically** for long-term storage
7. **Monitor pyrage updates** for security patches

---

## Conclusion

This dual-encryption approach provides the optimal balance of:
- **Security**: Strong encryption for sensitive file contents
- **Interoperability**: Standard age format for broad tool support  
- **Recoverability**: Simple command-line decryption for disaster scenarios
- **Practicality**: Deterministic path encryption for filesystem-like behavior

The security trade-offs are justified by operational requirements, and the recovery procedures ensure long-term data accessibility even without the original Python implementation.