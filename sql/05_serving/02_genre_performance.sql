-- Genre performance serving view.
--
-- Summarises Netflix catalogue size, ratings, and popularity by genre.
--
-- Business questions supported:
--   - Which genres contain the most titles?
--   - Which genres have the highest average IMDb scores?
--   - Which genres perform best according to TMDB?
--   - Which genres have the highest average popularity?

CREATE OR REPLACE VIEW production.genre_performance AS
SELECT
    dg.genre_sk,
    dg.genre_id,
    dg.name AS genre_name,

    COUNT(
        DISTINCT btg.title_sk
    ) AS title_count,

    ROUND(
        AVG(ftm.imdb_score),
        2
    ) AS avg_imdb_score,

    SUM(
        COALESCE(
            ftm.imdb_votes,
            0
        )
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

FROM uat_olap.dim_genre AS dg

LEFT JOIN uat_olap.bridge_title_genre AS btg
    ON btg.genre_sk = dg.genre_sk

LEFT JOIN uat_olap.fact_title_metrics AS ftm
    ON ftm.title_sk = btg.title_sk

GROUP BY
    dg.genre_sk,
    dg.genre_id,
    dg.name;