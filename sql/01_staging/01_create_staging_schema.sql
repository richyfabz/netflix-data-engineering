-- ============================================================
-- NETFLIX DATA ENGINEERING
-- RAW STAGING LAYER
-- ============================================================

-- Create a dedicated schema for raw incoming source data.
-- The ETL role owns this schema because Airflow will write here.
CREATE SCHEMA IF NOT EXISTS staging
AUTHORIZATION netflix_etl;


-- ============================================================
-- TABLE 1: dev.titles_raw
-- ============================================================

CREATE TABLE IF NOT EXISTS dev.titles_raw (
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
-- TABLE 2: dev.credits_raw
-- ============================================================


CREATE TABLE IF NOT EXISTS dev.credits_raw (
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
ON dev.titles_raw(batch_id);

CREATE INDEX IF NOT EXISTS idx_credits_raw_batch
ON dev.credits_raw(batch_id);


CREATE INDEX IF NOT EXISTS idx_titles_raw_id
ON dev.titles_raw(id);

CREATE INDEX IF NOT EXISTS idx_credits_raw_title_id
ON dev.credits_raw(id);