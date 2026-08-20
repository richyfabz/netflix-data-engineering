-- ============================================================
-- NETFLIX DATA ENGINEERING
-- RAW STAGING LAYER
-- ============================================================

-- Create a dedicated schema for raw incoming source data.
-- The ETL role owns this schema because Airflow will write here.
CREATE SCHEMA IF NOT EXISTS staging
AUTHORIZATION netflix_etl;


-- ============================================================
-- TABLE 1: staging.titles_raw
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.titles_raw (
    batch_id BIGINT NOT NULL,
    id VARCHAR(50),
    title VARCHAR(500),
    type VARCHAR(50),
    description TEXT,
    release_year VARCHAR(50),
    age_certification VARCHAR(50),
    runtime VARCHAR(50),
    genres TEXT,
    production_countries TEXT,
    seasons VARCHAR(50),
    imdb_id VARCHAR(50),
    imdb_score VARCHAR(50),
    imdb_votes VARCHAR(50),
    tmdb_popularity VARCHAR(50),
    tmdb_score VARCHAR(50),
    source_file VARCHAR(500) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- TABLE 2: staging.credits_raw
-- ============================================================


CREATE TABLE IF NOT EXISTS staging.credits_raw (
    batch_id BIGINT NOT NULL,
    person_id VARCHAR(50),
    id VARCHAR(50),
    name VARCHAR(500),
    character TEXT,
    role VARCHAR(100),
    source_file VARCHAR(500) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES
-- ============================================================

-- Improve batch-level lookups, cleanup and transformations
-- for staged title records.
CREATE INDEX IF NOT EXISTS idx_titles_raw_batch
ON staging.titles_raw(batch_id);

CREATE INDEX IF NOT EXISTS idx_credits_raw_batch
ON staging.credits_raw(batch_id);


CREATE INDEX IF NOT EXISTS idx_titles_raw_id
ON staging.titles_raw(id);

CREATE INDEX IF NOT EXISTS idx_credits_raw_title_id
ON staging.credits_raw(id);