-- ============================================================================
-- NETFLIX DATA ENGINEERING
-- INCREMENTAL PERSON UPSERT
-- ============================================================================
-- Purpose:
-- Move people from one staged credits batch into the OLTP person table.
--
-- If a person does not exist, PostgreSQL inserts the row.
-- If the person already exists, PostgreSQL updates the name.
-- ============================================================================


INSERT INTO person (
    person_id,
    name
)
SELECT DISTINCT
    person_id::INTEGER,
    name
FROM staging.credits_raw
WHERE batch_id = %(batch_id)s
AND person_id IS NOT NULL
AND person_id <> ''
AND name IS NOT NULL
AND name <> ''
ON CONFLICT (person_id)
DO UPDATE SET
    name = EXCLUDED.name;