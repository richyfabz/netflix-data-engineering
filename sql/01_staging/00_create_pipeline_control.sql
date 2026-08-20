-- Active: 1787006541054@@127.0.0.1@5432@postgres@etl
-- Active: 1787006541054@@127.0.0.1@5432@postgres

-- Netflix Data Engineering
-- ETL pipeline control tables

-- Check which PostgreSQL user VS Code is currently using.
SELECT
    current_database() AS database_name,
    current_user AS connected_user;

-- ------------------------------------------------------------
-- TABLE 1: pipeline_control
-- ------------------------------------------------------------
-- Stores the latest successful state of each pipeline.
-- Airflow will use this table to determine where the next
-- incremental load should begin.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.pipeline_control (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_successful_load TIMESTAMPTZ,
    last_watermark TIMESTAMPTZ,
    last_file_name TEXT,
    rows_processed BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'NEVER_RUN',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    );


-- ------------------------------------------------------------
-- TABLE 2: batch_history
-- ------------------------------------------------------------
-- Stores every batch/file seen by the pipeline.
-- This gives us an audit history and helps prevent duplicates.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.batch_history (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    file_name TEXT NOT NULL,
    file_checksum TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processing_started_at TIMESTAMPTZ,
    processing_completed_at TIMESTAMPTZ,
    rows_received BIGINT,
    rows_processed BIGINT,
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
    error_message TEXT
);