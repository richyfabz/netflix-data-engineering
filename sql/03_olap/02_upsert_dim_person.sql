-- ============================================================================
-- Incrementally upsert people from OLTP into the analytics dimension.
-- ============================================================================

INSERT INTO uat_olap.dim_person (
    person_sk,
    person_id,
    name
)

SELECT
    COALESCE(
        existing.person_sk,
        nextval('uat_olap.dim_person_sk_seq')
    ),
    p.person_id,
    p.name

FROM uat_oltp.person AS p

LEFT JOIN uat_olap.dim_person AS existing
    ON existing.person_id = p.person_id

ON CONFLICT (person_id)

DO UPDATE SET
    name = EXCLUDED.name;