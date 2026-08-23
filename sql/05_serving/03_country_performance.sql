-- Country performance serving view.
-- Summarises Netflix catalogue size, ratings, and popularity by production
-- country for downstream Power BI reporting.
--
-- Business questions supported:
--   - Which countries contribute the most Netflix titles?
--   - Which countries have the strongest average IMDb ratings?
--   - Which countries perform best according to TMDB?
--   - Which countries have the highest average popularity?

CREATE OR REPLACE VIEW serving.country_performance AS
SELECT
    dc.country_sk,
    dc.country_id,
    dc.name AS country_name,
    COUNT(
        DISTINCT btc.title_sk
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

FROM analytics.dim_country AS dc

LEFT JOIN analytics.bridge_title_country AS btc
    ON btc.country_sk = dc.country_sk

LEFT JOIN analytics.fact_title_metrics AS ftm
    ON ftm.title_sk = btc.title_sk

GROUP BY
    dc.country_sk,
    dc.country_id,
    dc.name;