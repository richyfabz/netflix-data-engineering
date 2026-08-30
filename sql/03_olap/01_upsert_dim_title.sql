-- ============================================================================
-- Incrementally upsert the title dimension from OLTP + current staging batch.
-- ============================================================================

INSERT INTO uat_olap.dim_title (
    title_sk,
    title_id,
    title_name,
    type,
    age_certification,
    release_year
)

SELECT
    COALESCE(
        existing.title_sk,
        nextval('uat_olap.dim_title_sk_seq')
    ) AS title_sk,

    t.title_id,
    t.title_name,
    t.type,
    t.age_certification,

    CASE
        WHEN NULLIF(s.release_year, '') IS NULL
            THEN NULL
        ELSE s.release_year::INTEGER
    END AS release_year

FROM uat_oltp.title AS t

INNER JOIN dev.titles_raw AS s
    ON s.id = t.title_id
   AND s.batch_id = %(batch_id)s

LEFT JOIN uat_olap.dim_title AS existing
    ON existing.title_id = t.title_id

WHERE t.title_id IS NOT NULL
  AND t.title_name IS NOT NULL

ON CONFLICT (title_id)

DO UPDATE SET
    title_name = EXCLUDED.title_name,
    type = EXCLUDED.type,
    age_certification = EXCLUDED.age_certification,
    release_year = EXCLUDED.release_year;