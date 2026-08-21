-- ============================================================================
-- Core production quality rules.
-- These checks must return zero violations.
-- ============================================================================


-- Duplicate title business IDs.
SELECT COUNT(*)
FROM (
    SELECT title_id
    FROM public.title
    GROUP BY title_id
    HAVING COUNT(*) > 1
) AS duplicates;


-- Duplicate person business IDs.
SELECT COUNT(*)
FROM (
    SELECT person_id
    FROM public.person
    GROUP BY person_id
    HAVING COUNT(*) > 1
) AS duplicates;


-- Orphan OLAP genre bridges.
SELECT COUNT(*)
FROM analytics.bridge_title_genre AS bridge

LEFT JOIN analytics.dim_title AS dt
    ON dt.title_sk = bridge.title_sk

LEFT JOIN analytics.dim_genre AS dg
    ON dg.genre_sk = bridge.genre_sk

WHERE dt.title_sk IS NULL
   OR dg.genre_sk IS NULL;


-- Orphan OLAP country bridges.
SELECT COUNT(*)
FROM analytics.bridge_title_country AS bridge

LEFT JOIN analytics.dim_title AS dt
    ON dt.title_sk = bridge.title_sk

LEFT JOIN analytics.dim_country AS dc
    ON dc.country_sk = bridge.country_sk

WHERE dt.title_sk IS NULL
   OR dc.country_sk IS NULL;


-- Orphan credit facts.
SELECT COUNT(*)
FROM analytics.fact_credits AS fc

LEFT JOIN analytics.dim_title AS dt
    ON dt.title_sk = fc.title_sk

LEFT JOIN analytics.dim_person AS dp
    ON dp.person_sk = fc.person_sk

WHERE dt.title_sk IS NULL
   OR dp.person_sk IS NULL;


-- Titles missing metrics facts.
SELECT COUNT(*)
FROM analytics.dim_title AS dt

LEFT JOIN analytics.fact_title_metrics AS fm
    ON fm.title_sk = dt.title_sk

WHERE fm.title_sk IS NULL;