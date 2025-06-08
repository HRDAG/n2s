-- Schema for ZFS to S3 file tracking
CREATE TYPE upload_status AS ENUM ('pending', 'uploading', 'completed', 'failed', 'rejected');
CREATE TYPE dataset_type AS ENUM ('backup', 'legacy', 'working');

CREATE TABLE files (
    id BIGSERIAL PRIMARY KEY,
    file_path TEXT NOT NULL,
    dataset dataset_type NOT NULL,
    file_size BIGINT,
    mtime TIMESTAMP,
    encrypted_path TEXT,
    batch_id INTEGER,
    upload_status upload_status DEFAULT 'pending',
    upload_timestamp TIMESTAMP,
    s3_key TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_dataset_path UNIQUE (dataset, file_path)
);

-- Indexes for performance
CREATE INDEX idx_upload_status ON files(upload_status);
CREATE INDEX idx_batch_id ON files(batch_id);
CREATE INDEX idx_dataset ON files(dataset);
CREATE INDEX idx_created_at ON files(created_at);

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_files_updated_at BEFORE UPDATE ON files
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();