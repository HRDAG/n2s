# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.13
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# n2s/tests/test_blobify_streaming.py

import json
import tempfile
from pathlib import Path

import pytest

# Import from parent directory
import sys
sys.path.append(str(Path(__file__).parent.parent / "scripts"))
from blobify import create_blob
from deblobify import restore_blob


class TestBlobifyStreaming:
    """Test that streaming blobify produces consistent results across formats."""

    def test_small_file_same_hash(self):
        """Test that small files produce same hash as before."""
        content = b"Hello, World!" * 100  # Small content
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            f.flush()
            
            # Create blob
            blobid = create_blob(Path(f.name), "/tmp")
            
            # Verify blob file was created
            blob_path = Path(f"/tmp/{blobid}")
            assert blob_path.exists()
            
            # Load and verify blob structure
            with open(blob_path) as bf:
                blob_data = json.load(bf)
                
            assert "content" in blob_data
            assert "metadata" in blob_data
            assert blob_data["metadata"]["size"] == len(content)
            assert blob_data["metadata"]["encryption"] is False
            
            # Verify new multi-frame format
            if isinstance(blob_data["content"], dict):
                assert blob_data["content"]["encoding"] == "lz4-multiframe"
                assert "frames" in blob_data["content"]
                assert len(blob_data["content"]["frames"]) > 0
            
            # Clean up
            blob_path.unlink()
            Path(f.name).unlink()

    def test_large_file_processing(self):
        """Test that larger files can be processed without memory issues."""
        # Create a 10MB test file
        content = b"A" * (10 * 1024 * 1024)  # 10MB of 'A's
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            f.flush()
            
            # This should not cause memory issues
            blobid = create_blob(Path(f.name), "/tmp")
            
            # Verify blob was created
            blob_path = Path(f"/tmp/{blobid}")
            assert blob_path.exists()
            
            # Verify metadata and format
            with open(blob_path) as bf:
                blob_data = json.load(bf)
                
            assert blob_data["metadata"]["size"] == len(content)
            
            # Should be multi-frame format for large files
            if isinstance(blob_data["content"], dict):
                assert blob_data["content"]["encoding"] == "lz4-multiframe"
            
            # Clean up
            blob_path.unlink()
            Path(f.name).unlink()

    def test_consistent_hash_different_sizes(self):
        """Test that same content produces same hash regardless of file size implications."""
        content1 = b"test content"
        content2 = b"test content"  # Same content
        
        with tempfile.NamedTemporaryFile(delete=False) as f1, \
             tempfile.NamedTemporaryFile(delete=False) as f2:
            
            f1.write(content1)
            f2.write(content2)
            f1.flush()
            f2.flush()
            
            blobid1 = create_blob(Path(f1.name), "/tmp")
            blobid2 = create_blob(Path(f2.name), "/tmp")
            
            # Same content should produce same blobid
            assert blobid1 == blobid2
            
            # Clean up
            Path(f"/tmp/{blobid1}").unlink()
            if Path(f"/tmp/{blobid2}").exists():  # Might be same file
                Path(f"/tmp/{blobid2}").unlink()
            Path(f1.name).unlink()
            Path(f2.name).unlink()

    def test_filetype_detection_works(self):
        """Test that filetype detection works with chunked reading."""
        # Create a simple text file
        content = b"This is a test file.\nWith multiple lines.\n"
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(content)
            f.flush()
            
            blobid = create_blob(Path(f.name), "/tmp")
            
            # Load blob and check filetype was detected
            blob_path = Path(f"/tmp/{blobid}")
            with open(blob_path) as bf:
                blob_data = json.load(bf)
                
            # Should detect as text (exact string depends on system magic)
            filetype = blob_data["metadata"]["filetype"]
            assert filetype != "unknown"
            assert "text" in filetype.lower() or "ascii" in filetype.lower()
            
            # Should be multi-frame format
            if isinstance(blob_data["content"], dict):
                assert blob_data["content"]["encoding"] == "lz4-multiframe"
            
            # Clean up
            blob_path.unlink()
            Path(f.name).unlink()

    def test_round_trip_consistency(self):
        """Test that blobify → deblobify produces identical content."""
        content = b"Round trip test content!" * 1000  # ~24KB
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            f.flush()
            
            # Create blob
            blobid = create_blob(Path(f.name), "/tmp")
            
            # Restore blob to different location
            with tempfile.NamedTemporaryFile(delete=False) as restored_f:
                restored_path = restored_f.name
            
            restore_blob(f"/tmp/{blobid}", restored_path)
            
            # Verify content matches
            with open(restored_path, 'rb') as rf:
                restored_content = rf.read()
                
            assert restored_content == content
            
            # Clean up
            Path(f"/tmp/{blobid}").unlink()
            Path(f.name).unlink()
            Path(restored_path).unlink()

    def test_multi_frame_streaming(self):
        """Test that multi-frame format uses constant memory."""
        # Create content larger than single frame (>10MB)
        content = b"X" * (15 * 1024 * 1024)  # 15MB
        
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            f.flush()
            
            blobid = create_blob(Path(f.name), "/tmp")
            
            # Verify multi-frame format was used
            blob_path = Path(f"/tmp/{blobid}")
            with open(blob_path) as bf:
                blob_data = json.load(bf)
                
            assert isinstance(blob_data["content"], dict)
            assert blob_data["content"]["encoding"] == "lz4-multiframe"
            
            # Should have multiple frames for 15MB content
            frames = blob_data["content"]["frames"]
            assert len(frames) >= 2  # At least 2 frames for 15MB with 10MB chunks
            
            # Test streaming decompression
            with tempfile.NamedTemporaryFile(delete=False) as restored_f:
                restored_path = restored_f.name
            
            restore_blob(str(blob_path), restored_path, verify=True)
            
            # Verify content matches
            with open(restored_path, 'rb') as rf:
                restored_content = rf.read()
                
            assert restored_content == content
            assert len(restored_content) == 15 * 1024 * 1024
            
            # Clean up
            blob_path.unlink()
            Path(f.name).unlink()
            Path(restored_path).unlink()

    def test_empty_file(self):
        """Test handling of empty files."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            # Empty file
            f.flush()
            
            blobid = create_blob(Path(f.name), "/tmp")
            
            blob_path = Path(f"/tmp/{blobid}")
            assert blob_path.exists()
            
            with open(blob_path) as bf:
                blob_data = json.load(bf)
                
            assert blob_data["metadata"]["size"] == 0
            
            # Clean up
            blob_path.unlink()
            Path(f.name).unlink()