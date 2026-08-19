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

-- Store incoming Netflix title records before cleaning
-- and transformation into typed analytics tables.
CREATE TABLE IF NOT EXISTS staging.titles_raw (
-- Batch responsible for loading this row.
    batch_id BIGINT NOT NULL,
 -- Source title identifier.
    id VARCHAR(50),
 -- Human-readable title.
    title VARCHAR(500),
 -- Expected values are usually MOVIE or SHOW,
    -- but staging remains tolerant.
    type VARCHAR(50),
 -- Long free-form description from the source.
    description TEXT,
 -- Stored as text in staging so malformed values
    -- do not cause the raw ingestion to fail.
    release_year VARCHAR(50),
 -- Source age/certification value.
    age_certification VARCHAR(50),
 -- Runtime kept as raw text before validation.
    runtime VARCHAR(50),
 -- Genres can arrive as array-like or semi-structured text.
    genres TEXT,
 -- Production countries can also arrive as structured-looking text.
    production_countries TEXT,
-- Number of seasons retained as raw text.
    seasons VARCHAR(50),
 -- External IMDb identifier.
    imdb_id VARCHAR(50),
 -- Metrics are kept as text in raw staging.
    imdb_score VARCHAR(50),
  -- IMDb votes retained exactly as received.
    imdb_votes VARCHAR(50),
 -- TMDb popularity retained as raw text.
    tmdb_popularity VARCHAR(50),
 -- TMDb score retained as raw text.
    tmdb_score VARCHAR(50),
-- Original file that produced this row.
    source_file VARCHAR(500) NOT NULL,
 -- Timestamp showing when the row entered staging.
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- TABLE 2: staging.credits_raw
-- ============================================================

-- Store incoming Netflix credits before cleaning
-- and transformation.
CREATE TABLE IF NOT EXISTS staging.credits_raw (
 -- Batch responsible for loading this row.
    batch_id BIGINT NOT NULL,
 -- Source person identifier.
    person_id VARCHAR(50),
 -- Source title identifier linking the person to a title.
    id VARCHAR(50),
 -- Actor/director/person name.
    name VARCHAR(500),
 -- Character values can be long or inconsistent,
    -- so TEXT is safer in the raw layer.
    character TEXT,
 -- Role such as ACTOR or DIRECTOR.
    role VARCHAR(100),
-- Original file that produced this row.
    source_file VARCHAR(500) NOT NULL,
 -- Timestamp showing when the row entered staging.
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES
-- ============================================================

-- Improve batch-level lookups, cleanup and transformations
-- for staged title records.
CREATE INDEX IF NOT EXISTS idx_titles_raw_batch
ON staging.titles_raw(batch_id);


-- Improve batch-level lookups, cleanup and transformations
-- for staged credit records.
CREATE INDEX IF NOT EXISTS idx_credits_raw_batch
ON staging.credits_raw(batch_id);


-- Improve title lookups inside the staged titles table.
CREATE INDEX IF NOT EXISTS idx_titles_raw_id
ON staging.titles_raw(id);


-- Improve title-to-credit joins in staging.
CREATE INDEX IF NOT EXISTS idx_credits_raw_title_id
ON staging.credits_raw(id);