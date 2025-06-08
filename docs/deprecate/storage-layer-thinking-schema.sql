-- n2s Storage System PostgreSQL Schema
-- Supports multi-installation deduplicating storage with complete audit trails

-- Installation configurations (filesystem + backend pairs)
CREATE TABLE installations (
    installation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    filesystem_path TEXT NOT NULL,
    backend_type TEXT NOT NULL, -- 's3', 'ipfs', 'local', etc.
    backend_config JSONB NOT NULL, -- connection details, credentials, etc.
    encryption_config JSONB NOT NULL, -- age keys, passphrase info
    verification_config JSONB, -- sampling rates, policies
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Point-in-time snapshots of datasets
CREATE TABLE snapshots (
    installation_id UUID NOT NULL,
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    total_files BIGINT NOT NULL DEFAULT 0,
    total_size BIGINT NOT NULL DEFAULT 0, -- original uncompressed size
    metadata JSONB, -- snapshot-specific info, client version, etc.
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (installation_id, snapshot_timestamp),
    FOREIGN KEY (installation_id) REFERENCES installations(installation_id) ON DELETE CASCADE
);

-- Global blob storage (deduplicated content)
CREATE TABLE stored_blobs (
    content_hash TEXT PRIMARY KEY, -- BLAKE3 hash of original content
    original_size BIGINT NOT NULL,
    encrypted_size BIGINT NOT NULL,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    reference_count INTEGER NOT NULL DEFAULT 0, -- how many files reference this
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- File path mappings within snapshots
CREATE TABLE file_references (
    installation_id UUID NOT NULL,
    snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    filepath TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    mtime TIMESTAMP WITH TIME ZONE NOT NULL,
    mode INTEGER, -- unix file permissions
    metadata JSONB, -- extended attributes, etc.
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (installation_id, snapshot_timestamp, filepath),
    FOREIGN KEY (installation_id, snapshot_timestamp)
        REFERENCES snapshots(installation_id, snapshot_timestamp) ON DELETE CASCADE,
    FOREIGN KEY (content_hash) REFERENCES stored_blobs(content_hash)
);

-- Complete audit trail of all storage operations
CREATE TABLE storage_operations (
    operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_hash TEXT NOT NULL,
    installation_id UUID NOT NULL,
    operation_type TEXT NOT NULL, -- 'upload_started', 'upload_completed', 'upload_failed', 'verification_success', 'verification_failed', 'deletion_requested', 'deletion_completed'
    status TEXT, -- 'success', 'failed', 'timeout', 'auth_error', etc.
    operation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    details JSONB, -- error messages, timing info, backend responses
    retry_count INTEGER DEFAULT 0,

    FOREIGN KEY (content_hash) REFERENCES stored_blobs(content_hash),
    FOREIGN KEY (installation_id) REFERENCES installations(installation_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_snapshots_installation_timestamp ON snapshots(installation_id, snapshot_timestamp DESC);
CREATE INDEX idx_file_references_content_hash ON file_references(content_hash);
CREATE INDEX idx_file_references_installation_snapshot ON file_references(installation_id, snapshot_timestamp);
CREATE INDEX idx_storage_operations_content_hash ON storage_operations(content_hash);
CREATE INDEX idx_storage_operations_installation ON storage_operations(installation_id);
CREATE INDEX idx_storage_operations_timestamp ON storage_operations(operation_timestamp DESC);
CREATE INDEX idx_storage_operations_type_status ON storage_operations(operation_type, status);

-- Partial index for finding blobs that need verification
CREATE INDEX idx_storage_operations_pending_verification
    ON storage_operations(content_hash, operation_timestamp DESC)
    WHERE operation_type IN ('upload_completed', 'verification_success');

-- Triggers to maintain reference counts
CREATE OR REPLACE FUNCTION update_blob_reference_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE stored_blobs
        SET reference_count = reference_count + 1
        WHERE content_hash = NEW.content_hash;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE stored_blobs
        SET reference_count = reference_count - 1
        WHERE content_hash = OLD.content_hash;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_blob_reference_count
    AFTER INSERT OR DELETE ON file_references
    FOR EACH ROW EXECUTE FUNCTION update_blob_reference_count();

-- Partitioning for storage_operations table (by month)
-- This is essential given the expected volume
CREATE TABLE storage_operations_template (LIKE storage_operations INCLUDING ALL);

-- Example monthly partition (would be created automatically)
-- CREATE TABLE storage_operations_2024_01 PARTITION OF storage_operations
--     FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Views for common queries
CREATE VIEW current_blob_status AS
SELECT DISTINCT ON (content_hash, installation_id)
    content_hash,
    installation_id,
    operation_type,
    status,
    operation_timestamp,
    details
FROM storage_operations
ORDER BY content_hash, installation_id, operation_timestamp DESC;

CREATE VIEW verification_candidates AS
SELECT
    sb.content_hash,
    i.installation_id,
    i.name as installation_name,
    sb.original_size,
    MAX(so.operation_timestamp) as last_verification
FROM stored_blobs sb
JOIN file_references fr ON sb.content_hash = fr.content_hash
JOIN installations i ON fr.installation_id = i.installation_id
LEFT JOIN storage_operations so ON sb.content_hash = so.content_hash
    AND so.installation_id = i.installation_id
    AND so.operation_type = 'verification_success'
GROUP BY sb.content_hash, i.installation_id, i.name, sb.original_size
HAVING MAX(so.operation_timestamp) IS NULL
    OR MAX(so.operation_timestamp) < NOW() - INTERVAL '7 days';

-- Example queries

-- Find all files in a specific snapshot
-- SELECT filepath, file_size, mtime
-- FROM file_references
-- WHERE installation_id = $1 AND snapshot_timestamp = $2;

-- Check if content already exists (deduplication check)
-- SELECT content_hash FROM stored_blobs WHERE content_hash = $1;

-- Get current storage status for a blob
-- SELECT * FROM current_blob_status
-- WHERE content_hash = $1 AND installation_id = $2;

-- Find blobs needing verification
-- SELECT * FROM verification_candidates
-- WHERE installation_id = $1
-- ORDER BY RANDOM() LIMIT 100;
