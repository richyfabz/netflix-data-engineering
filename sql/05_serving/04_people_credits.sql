-- People and credits performance serving view.
-- Provides one aggregated record per person and role, combining credit
-- activity with the performance of the Netflix titles they are associated
-- with.
--
-- Business questions supported:
--   - Which people appear in the most Netflix titles?
--   - Which actors/directors have the strongest average title ratings?
--   - How do actor and director portfolios compare?
--   - Which people are associated with the most popular titles?

CREATE OR REPLACE VIEW production.people_credits AS
SELECT
    dp.person_sk,
    dp.person_id,
    dp.name AS person_name,
    fc.role,
    COUNT(
        DISTINCT fc.title_sk
    ) AS title_count,

    COUNT(
        fc.credit_sk
    ) AS credit_count,

    ROUND(
        AVG(ftm.imdb_score),
        2
    ) AS avg_imdb_score,

    SUM(
        COALESCE(ftm.imdb_votes, 0)
    ) AS total_imdb_votes,

    ROUND(
        AVG(ftm.tmdb_score),
        2
    ) AS avg_tmdb_score,

    ROUND(
        AVG(ftm.tmdb_popularity),
        2
    ) AS avg_tmdb_popularity,

    COUNT(*) FILTER (
        WHERE ftm.imdb_score_imputed = TRUE
    ) AS imputed_imdb_score_count,

    COUNT(*) FILTER (
        WHERE ftm.tmdb_score_imputed = TRUE
    ) AS imputed_tmdb_score_count

FROM uat_olap.dim_person AS dp

INNER JOIN uat_olap.fact_credits AS fc
    ON fc.person_sk = dp.person_sk

LEFT JOIN uat_olap.fact_title_metrics AS ftm
    ON ftm.title_sk = fc.title_sk

GROUP BY
    dp.person_sk,
    dp.person_id,
    dp.name,
    fc.role;

  