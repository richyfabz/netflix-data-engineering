-- ============================================================================
-- Incrementally upsert countries from OLTP into the analytics dimension.
-- country_sk is generated automatically by PostgreSQL.
-- ============================================================================

INSERT INTO uat_olap.dim_country (
    country_id,
    name,
    country_code
)
SELECT
    c.country_id,
    CASE
        WHEN c.name = 'Lebanon' THEN 'LB'
        ELSE UPPER(TRIM(c.name))
    END AS name,
    CASE
        WHEN c.name = 'Lebanon' THEN 'LB'
        ELSE UPPER(TRIM(c.name))
    END AS country_code
FROM uat_oltp.country AS c

ON CONFLICT (country_id)
DO UPDATE SET
    name = EXCLUDED.name,
    country_code = EXCLUDED.country_code;