-- Active: 1787006541054@@127.0.0.1@5432@postgres@etl
-- Active: 1787006541054@@127.0.0.1@5432@postgres
-- ============================================================
-- Netflix Data Engineering
-- ETL pipeline control tables
-- ============================================================

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
 -- Unique name identifying a pipeline.
    -- Example: netflix_titles_pipeline
    pipeline_name VARCHAR(100) PRIMARY KEY,
-- Time when the most recent successful pipeline run finished.
    last_successful_load TIMESTAMPTZ,
-- Latest source-data timestamp successfully processed.
    -- This becomes our incremental-loading checkpoint.
    last_watermark TIMESTAMPTZ,
 -- Name of the last successfully processed file.
    last_file_name TEXT,
-- Number of rows processed during the most recent successful run.
    rows_processed BIGINT NOT NULL DEFAULT 0,
 -- Current state of the pipeline.
    -- Typical values: NEVER_RUN, RUNNING, SUCCESS, FAILED.
    status VARCHAR(20) NOT NULL DEFAULT 'NEVER_RUN',
-- Automatically records when this control row was last modified.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP);


-- ------------------------------------------------------------
-- TABLE 2: batch_history
-- ------------------------------------------------------------
-- Stores every batch/file seen by the pipeline.
-- This gives us an audit history and helps prevent duplicates.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.batch_history (
 -- Automatically generated identifier for each batch.
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- Pipeline responsible for processing this batch.
    pipeline_name VARCHAR(100) NOT NULL,
  -- Original name of the incoming source file.
    file_name TEXT NOT NULL,
-- Hash/fingerprint of the source file.
    -- Later used to identify duplicate files.
    file_checksum TEXT,
-- Timestamp when the pipeline first discovered the batch.
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
-- Timestamp when processing of the batch began.
    processing_started_at TIMESTAMPTZ,
-- Timestamp when processing finished.
    processing_completed_at TIMESTAMPTZ,
-- Number of rows found in the incoming source.
    rows_received BIGINT,
 -- Number of rows successfully written/processed.
    rows_processed BIGINT,
-- Processing state for this specific batch.
    -- Example: RECEIVED, RUNNING, SUCCESS, FAILED.
    status VARCHAR(20) NOT NULL DEFAULT 'RECEIVED',
 -- Error details are preserved here if a batch fails.
    error_message TEXT
);