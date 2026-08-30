-- ============================================================================
-- Incrementally upsert countries from OLTP into the analytics dimension.
-- country_sk is generated automatically by PostgreSQL.
-- ============================================================================

INSERT INTO uat_olap.dim_country (
    country_id,
    name
)

SELECT
    c.country_id,
    c.name
FROM uat_oltp.country AS c

ON CONFLICT (country_id)

DO UPDATE SET
    name = EXCLUDED.name;