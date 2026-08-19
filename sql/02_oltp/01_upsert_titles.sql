-- ============================================================================
-- NETFLIX DATA ENGINEERING
-- INCREMENTAL TITLE UPSERT
-- ============================================================================
-- Purpose:
-- Move one staged title batch into the normalised OLTP title table.

INSERT INTO title (
    title_id,
    title_name,
    type,
    description,
    age_certification,
    runtime,
    seasons,
    imdb_id
)
SELECT
    id,
    title,
    type,
    description,
    NULLIF(age_certification, ''),
    -- Convert runtime from raw text into INTEGER.
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
    NULLIF(imdb_id, '')
FROM staging.titles_raw
WHERE batch_id = %(batch_id)s
AND id IS NOT NULL
AND id <> ''
AND title IS NOT NULL
AND title <> ''
ON CONFLICT (title_id)
DO UPDATE SET
    title_name = EXCLUDED.title_name,
    type = EXCLUDED.type,
    description = EXCLUDED.description,
    age_certification = EXCLUDED.age_certification,
    runtime = EXCLUDED.runtime,
    seasons = EXCLUDED.seasons,
    imdb_id = EXCLUDED.imdb_id;