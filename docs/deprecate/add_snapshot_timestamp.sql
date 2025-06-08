-- Add snapshot_timestamp column to files table
ALTER TABLE files ADD COLUMN snapshot_timestamp TIMESTAMP;

-- Create index for efficient querying by snapshot time
CREATE INDEX idx_snapshot_timestamp ON files(dataset, snapshot_timestamp);