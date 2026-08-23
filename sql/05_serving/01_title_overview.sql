-- ============================================================================
-- Title overview serving view.
--
-- Provides one business-friendly record per Netflix title together with
-- classification, release information, ratings, popularity, and data-quality
-- indicators required for reporting and Power BI analysis.
-- ============================================================================

CREATE OR REPLACE VIEW serving.title_overview AS
SELECT
    dt.title_sk,
    dt.title_id,
    dt.title_name AS title,
    dt.type,
    dt.age_certification,
    dt.release_year,
    ftm.imdb_score,
    ftm.imdb_votes,
    ftm.tmdb_score,
    ftm.tmdb_popularity,
    ftm.imdb_score_imputed,
    ftm.imdb_votes_imputed,
    ftm.tmdb_score_imputed,
    ftm.tmdb_popularity_imputed
FROM analytics.dim_title AS dt
LEFT JOIN analytics.fact_title_metrics AS ftm
    ON ftm.title_sk = dt.title_sk;