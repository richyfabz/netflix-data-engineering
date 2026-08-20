-- ============================================================================
-- Incrementally upsert people from OLTP into the analytics dimension.
-- ============================================================================

INSERT INTO analytics.dim_person (
    person_sk,
    person_id,
    name
)

SELECT
    COALESCE(
        existing.person_sk,
        nextval('analytics.dim_person_sk_seq')
    ),
    p.person_id,
    p.name

FROM public.person AS p

LEFT JOIN analytics.dim_person AS existing
    ON existing.person_id = p.person_id

ON CONFLICT (person_id)

DO UPDATE SET
    name = EXCLUDED.name;