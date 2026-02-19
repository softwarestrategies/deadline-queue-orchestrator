-- V1__Create_scheduled_events_table.sql
-- Creates the scheduled_events table with RANGE partitioning by scheduled date

-- Create enum types
CREATE TYPE event_status AS ENUM (
    'PENDING',
    'PROCESSING',
    'COMPLETED',
    'FAILED',
    'DEAD_LETTER',
    'CANCELLED'
);

CREATE TYPE delivery_type AS ENUM (
    'HTTP',
    'KAFKA'
);

-- Create the main partitioned table
CREATE TABLE scheduled_events (
                                  id UUID NOT NULL,
                                  external_job_id VARCHAR(255) NOT NULL,
                                  source VARCHAR(100) NOT NULL,
                                  scheduled_at TIMESTAMPTZ NOT NULL,
                                  delivery_type VARCHAR(20) NOT NULL,
                                  destination VARCHAR(2048) NOT NULL,
                                  payload JSONB NOT NULL,
                                  status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                                  retry_count INTEGER NOT NULL DEFAULT 0,
                                  max_retries INTEGER NOT NULL DEFAULT 3,
                                  last_error VARCHAR(4000),
                                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                  executed_at TIMESTAMPTZ,
                                  locked_by VARCHAR(100),
                                  lock_expires_at TIMESTAMPTZ,
                                  partition_key INTEGER NOT NULL,
                                  PRIMARY KEY (id, partition_key)
) PARTITION BY RANGE (partition_key);

-- Create indexes for common queries
-- Index for finding events ready for execution
CREATE INDEX idx_scheduled_events_status_scheduled_at
    ON scheduled_events (status, scheduled_at)
    WHERE status IN ('PENDING', 'PROCESSING');

-- Index for external job ID lookups
CREATE INDEX idx_scheduled_events_external_job_id
    ON scheduled_events (external_job_id);

-- Index for source-based queries
CREATE INDEX idx_scheduled_events_source
    ON scheduled_events (source);

-- Index for finding expired locks
CREATE INDEX idx_scheduled_events_lock_expires
    ON scheduled_events (lock_expires_at)
    WHERE status = 'PROCESSING';

-- Index for cleanup operations
CREATE INDEX idx_scheduled_events_cleanup
    ON scheduled_events (status, executed_at)
    WHERE status IN ('COMPLETED', 'DEAD_LETTER', 'CANCELLED');

-- Composite index for deduplication checks
-- MUST include partition_key because table is partitioned by it
CREATE UNIQUE INDEX idx_scheduled_events_unique_submission
    ON scheduled_events (external_job_id, source, scheduled_at, partition_key);

-- Function to automatically create partitions
CREATE OR REPLACE FUNCTION create_partition_if_not_exists(
    partition_key_val INTEGER
) RETURNS VOID AS $$
DECLARE
partition_name TEXT;
    start_val INTEGER;
    end_val INTEGER;
BEGIN
    -- Calculate partition bounds (each partition covers 10 days worth of partition keys)
    start_val := (partition_key_val / 10) * 10;
    end_val := start_val + 10;
    partition_name := 'scheduled_events_p' || start_val;

    -- Check if partition exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
        AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF scheduled_events
             FOR VALUES FROM (%s) TO (%s)',
            partition_name, start_val, end_val
        );
        RAISE NOTICE 'Created partition: %', partition_name;
END IF;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions for the next year
-- Each partition covers roughly 10 days (partition_key is year*1000 + day_of_year)
DO $$
DECLARE
current_year INTEGER := EXTRACT(YEAR FROM CURRENT_DATE);
    next_year INTEGER := current_year + 1;
    i INTEGER;
BEGIN
    -- Create partitions for current year
FOR i IN 0..37 LOOP
        PERFORM create_partition_if_not_exists(current_year * 1000 + i * 10);
END LOOP;

    -- Create partitions for next year
FOR i IN 0..37 LOOP
        PERFORM create_partition_if_not_exists(next_year * 1000 + i * 10);
END LOOP;
END $$;

-- Create a trigger to auto-create partitions for future data
CREATE OR REPLACE FUNCTION scheduled_events_partition_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM create_partition_if_not_exists(NEW.partition_key);
RETURN NEW;
EXCEPTION
    WHEN duplicate_table THEN
        -- Partition already exists, ignore
        RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Note: Triggers on partitioned tables require PostgreSQL 13+
-- For earlier versions, this needs to be handled in application code

COMMENT ON TABLE scheduled_events IS 'Stores scheduled events for future execution. Partitioned by scheduled date for efficient queries and cleanup.';
COMMENT ON COLUMN scheduled_events.partition_key IS 'Partition key derived from scheduled_at: year*1000 + day_of_year';
COMMENT ON COLUMN scheduled_events.locked_by IS 'Worker ID that has acquired this event for processing';
COMMENT ON COLUMN scheduled_events.lock_expires_at IS 'Time when the lock expires, allowing recovery from crashed workers';