-- ============================================================================
-- Incrementally upsert title-level metrics into the OLAP fact table.
-- Missing source metrics are imputed during the warehouse transformation.
-- ============================================================================

INSERT INTO analytics.fact_title_metrics (
    title_sk,
    imdb_score,
    imdb_votes,
    tmdb_popularity,
    tmdb_score,
    imdb_score_imputed,
    imdb_votes_imputed,
    tmdb_popularity_imputed,
    tmdb_score_imputed
)

SELECT
    dt.title_sk,

    COALESCE(
        NULLIF(TRIM(s.imdb_score), '')::NUMERIC,
        0
    ) AS imdb_score,

    COALESCE(
        NULLIF(TRIM(s.imdb_votes), '')::NUMERIC::INTEGER,
        0
    ) AS imdb_votes,

    COALESCE(
        NULLIF(TRIM(s.tmdb_popularity), '')::NUMERIC,
        0
    ) AS tmdb_popularity,

    COALESCE(
        NULLIF(TRIM(s.tmdb_score), '')::NUMERIC,
        0
    ) AS tmdb_score,

    NULLIF(TRIM(s.imdb_score), '') IS NULL
        AS imdb_score_imputed,

    NULLIF(TRIM(s.imdb_votes), '') IS NULL
        AS imdb_votes_imputed,

    NULLIF(TRIM(s.tmdb_popularity), '') IS NULL
        AS tmdb_popularity_imputed,

    NULLIF(TRIM(s.tmdb_score), '') IS NULL
        AS tmdb_score_imputed

FROM staging.titles_raw AS s

INNER JOIN analytics.dim_title AS dt
    ON dt.title_id = s.id

WHERE s.batch_id = %(batch_id)s

ON CONFLICT (title_sk)

DO UPDATE SET
    imdb_score = EXCLUDED.imdb_score,
    imdb_votes = EXCLUDED.imdb_votes,
    tmdb_popularity = EXCLUDED.tmdb_popularity,
    tmdb_score = EXCLUDED.tmdb_score,
    imdb_score_imputed = EXCLUDED.imdb_score_imputed,
    imdb_votes_imputed = EXCLUDED.imdb_votes_imputed,
    tmdb_popularity_imputed = EXCLUDED.tmdb_popularity_imputed,
    tmdb_score_imputed = EXCLUDED.tmdb_score_imputed;