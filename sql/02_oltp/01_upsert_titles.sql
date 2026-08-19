-- ============================================================================
-- NETFLIX DATA ENGINEERING
-- INCREMENTAL TITLE UPSERT
-- ============================================================================
-- Purpose:
-- Move one staged title batch into the normalised OLTP title table.
--
-- This script is incremental:
-- it processes only the batch_id supplied by Python/Airflow.
--
-- If a title does not exist, it is inserted.
-- If the same title already exists, its values are updated.
--
-- This makes the transformation safe to rerun.
-- ============================================================================


-- Insert transformed title records into the OLTP title table.
INSERT INTO title (

    -- Natural Netflix title identifier.
    title_id,

    -- Human-readable title.
    title_name,

    -- MOVIE or SHOW.
    type,

    -- Long-form source description.
    description,

    -- Age / certification label.
    age_certification,

    -- Runtime converted from staging text into INTEGER.
    runtime,

    -- Number of seasons converted into INTEGER.
    seasons,

    -- External IMDb identifier.
    imdb_id
)


-- Read the current batch from raw staging.
SELECT

    -- Map staging.id into the OLTP title primary key.
    id,

    -- Map the source title text.
    title,

    -- Preserve the validated title type.
    type,

    -- Preserve the description.
    description,

    -- Convert empty certification strings into NULL.
    NULLIF(age_certification, ''),

    -- Convert runtime from raw text into INTEGER.
    -- Empty strings become NULL instead of causing a cast error.
    CASE
        WHEN NULLIF(runtime, '') IS NULL
            THEN NULL
        ELSE runtime::NUMERIC::INTEGER
    END,

    -- Convert seasons from raw text into INTEGER.
    -- Movies usually have NULL seasons.
    CASE
        WHEN NULLIF(seasons, '') IS NULL
            THEN NULL
        ELSE seasons::NUMERIC::INTEGER
    END,

    -- Convert empty IMDb identifiers into NULL.
    NULLIF(imdb_id, '')


-- Read from the raw titles staging table.
FROM staging.titles_raw


-- Process only the batch currently being orchestrated.
-- Python/Airflow will replace %(batch_id)s with the real batch ID.
WHERE batch_id = %(batch_id)s


-- Prevent records without a source identifier from entering OLTP.
AND id IS NOT NULL


-- Prevent empty identifiers.
AND id <> ''


-- Prevent records without a usable title name.
AND title IS NOT NULL


-- Prevent empty title values.
AND title <> ''


-- ============================================================================
-- UPSERT BEHAVIOUR
-- ============================================================================
-- title_id is the primary key.
--
-- If PostgreSQL finds an existing row with the same title_id,
-- it updates the existing record rather than inserting a duplicate.
-- ============================================================================

ON CONFLICT (title_id)

DO UPDATE SET

    -- Replace the existing title name with the newest source value.
    title_name = EXCLUDED.title_name,

    -- Replace the title type when the source changes.
    type = EXCLUDED.type,

    -- Refresh the description.
    description = EXCLUDED.description,

    -- Refresh the age certification.
    age_certification = EXCLUDED.age_certification,

    -- Refresh runtime.
    runtime = EXCLUDED.runtime,

    -- Refresh seasons.
    seasons = EXCLUDED.seasons,

    -- Refresh the IMDb identifier.
    imdb_id = EXCLUDED.imdb_id;