-- ============================================================================
-- Incrementally load unique title/person credit relationships.
-- Preserve the original DEV ingestion timestamp.
-- ============================================================================

INSERT INTO uat_oltp.credit (
    title_id,
    person_id,
    role,
    character,
    ingested_at
)

SELECT DISTINCT
    c.id AS title_id,
    c.person_id::INTEGER AS person_id,
    c.role,
    NULLIF(c.character, '') AS character,
    c.ingested_at

FROM dev.credits_raw AS c

INNER JOIN uat_oltp.title AS t
    ON t.title_id = c.id

INNER JOIN uat_oltp.person AS p
    ON p.person_id = c.person_id::INTEGER

WHERE c.batch_id = %(batch_id)s
  AND c.id IS NOT NULL
  AND c.id <> ''
  AND c.person_id IS NOT NULL
  AND c.person_id <> ''
  AND c.role IS NOT NULL
  AND c.role <> ''

-- Existing business-key relationships are not duplicated.
ON CONFLICT
DO NOTHING;