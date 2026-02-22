-- V1__Create_scheduled_events_table.sql
-- Creates the scheduled_events table with RANGE partitioning by scheduled_at (monthly partitions)

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
-- Partitioned directly on scheduled_at for natural date-range queries and simple partition management.
-- Monthly partitions allow instant DROP TABLE cleanup and correct PostgreSQL partition pruning
-- on all scheduled_at predicates without any custom integer encoding.
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
    PRIMARY KEY (id, scheduled_at)
) PARTITION BY RANGE (scheduled_at);

-- Index for finding events ready for execution (scheduler's primary query)
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

-- Unique index for deduplication.
-- scheduled_at is now the partition column so it must be included in the primary key and unique index.
CREATE UNIQUE INDEX idx_scheduled_events_unique_submission
    ON scheduled_events (external_job_id, source, scheduled_at);

-- Function to automatically create a monthly partition if it does not already exist.
-- Partition names are human-readable: scheduled_events_2025_01, scheduled_events_2025_02, etc.
CREATE OR REPLACE FUNCTION create_monthly_partition_if_not_exists(
    target_date TIMESTAMPTZ
) RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_date     TIMESTAMPTZ;
    end_date       TIMESTAMPTZ;
BEGIN
    start_date     := DATE_TRUNC('month', target_date);
    end_date       := start_date + INTERVAL '1 month';
    partition_name := 'scheduled_events_' || TO_CHAR(start_date, 'YYYY_MM');

    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
          AND n.nspname = 'public'
    ) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF scheduled_events
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions: 3 months back through 12 months ahead.
-- Provides coverage for historical event queries and future scheduling.
DO $$
DECLARE
    m INTEGER;
BEGIN
    FOR m IN -3..12 LOOP
        PERFORM create_monthly_partition_if_not_exists(
            DATE_TRUNC('month', NOW()) + (m || ' months')::INTERVAL
        );
    END LOOP;
END $$;

-- Trigger function to auto-create a monthly partition whenever a new row is inserted
-- for a month that does not yet have a partition.
CREATE OR REPLACE FUNCTION scheduled_events_partition_trigger()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM create_monthly_partition_if_not_exists(NEW.scheduled_at);
    RETURN NEW;
EXCEPTION
    WHEN duplicate_table THEN
        RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE scheduled_events IS 'Stores scheduled events for future execution. Partitioned monthly by scheduled_at for efficient queries and simple partition-level cleanup.';
COMMENT ON COLUMN scheduled_events.scheduled_at IS 'When the event should be executed. Also the partition column — monthly partitions are named scheduled_events_YYYY_MM.';
COMMENT ON COLUMN scheduled_events.locked_by IS 'Worker ID that has acquired this event for processing';
COMMENT ON COLUMN scheduled_events.lock_expires_at IS 'Time when the lock expires, allowing recovery from crashed workers';
