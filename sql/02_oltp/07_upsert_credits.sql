-- ============================================================================
-- Incrementally load unique title/person credit relationships.
-- ============================================================================

INSERT INTO public.credit (
    title_id,
    person_id,
    role,
    character
)

SELECT DISTINCT
    c.id AS title_id,
    c.person_id::INTEGER AS person_id,
    c.role,
    NULLIF(c.character, '') AS character

FROM staging.credits_raw AS c

INNER JOIN public.title AS t
    ON t.title_id = c.id

INNER JOIN public.person AS p
    ON p.person_id = c.person_id::INTEGER

WHERE c.batch_id = %(batch_id)s
  AND c.id IS NOT NULL
  AND c.id <> ''
  AND c.person_id IS NOT NULL
  AND c.person_id <> ''
  AND c.role IS NOT NULL
  AND c.role <> ''

-- The business-key unique index protects against duplicate credits.
ON CONFLICT
DO NOTHING;