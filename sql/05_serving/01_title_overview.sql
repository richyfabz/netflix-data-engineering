-- Title overview serving view.

CREATE OR REPLACE VIEW production.title_overview AS
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
    ftm.tmdb_popularity_imputed,
    dt.ingestion_date_sk,
    dd.full_date AS ingestion_date,
    dd.day_name AS ingestion_day_name,
    dd.week AS ingestion_week,
    dd.month AS ingestion_month,
    dd.month_name AS ingestion_month_name,
    dd.quarter AS ingestion_quarter,
    dd.year AS ingestion_year,
    dd.is_weekend AS ingestion_is_weekend,
    te.imdb_id,
    te.tmdb_id,
    te.tmdb_title,
    te.poster_url,
    te.match_status

FROM uat_olap.dim_title AS dt

LEFT JOIN uat_olap.fact_title_metrics AS ftm
    ON ftm.title_sk = dt.title_sk

LEFT JOIN uat_olap.dim_date AS dd
    ON dd.date_sk = dt.ingestion_date_sk

LEFT JOIN uat_olap.title_enrichment AS te
    ON te.title_sk = dt.title_sk;