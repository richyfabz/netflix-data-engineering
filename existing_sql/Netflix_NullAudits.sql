-- ============================================================================
-- STAGE 15: NULL AUDIT — dim_title and fact_title_metrics
-- ============================================================================
-- Counting NULLs per column before deciding how to handle each one.
-- Purpose: distinguish "isolated bad row, safe to exclude" from "normal,
-- widespread missing data that needs a deliberate, documented decision"
-- (same reasoning applied earlier to title_name vs age_certification).

SELECT
    COUNT(*) FILTER (WHERE age_certification IS NULL) AS null_age_cert,
    COUNT(*) FILTER (WHERE release_year IS NULL) AS null_release_year
FROM analytics.dim_title;

SELECT
    COUNT(*) FILTER (WHERE imdb_score IS NULL) AS null_imdb_score,
    COUNT(*) FILTER (WHERE imdb_votes IS NULL) AS null_imdb_votes,
    COUNT(*) FILTER (WHERE tmdb_popularity IS NULL) AS null_tmdb_pop,
    COUNT(*) FILTER (WHERE tmdb_score IS NULL) AS null_tmdb_score
FROM analytics.fact_title_metrics;



-- identify the current column present in the dim_title table
SELECT column_name FROM information_schema.columns WHERE table_schema = 'analytics' AND table_name = 'dim_title';

-- calculate both the mean and meadian for numeric columns in fact_title_metrics 
--if mean and median are close together for a column, either is defensible. 
--If they're far apart (which I'd genuinely expect for imdb_votes), that's strong evidence median is the more honest, 
--representative choice, and mean would overstate what a "typical" title looks like.
SELECT 
    MIN(imdb_score) AS min_score, MAX(imdb_score) AS max_score, 
    ROUND(AVG(imdb_score), 2) AS mean_score,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY imdb_score) AS median_score
FROM analytics.fact_title_metrics;

SELECT 
    MIN(imdb_votes) AS min_votes, MAX(imdb_votes) AS max_votes,
    ROUND(AVG(imdb_votes), 2) AS mean_votes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY imdb_votes) AS median_votes
FROM analytics.fact_title_metrics;


SELECT
    MIN(tmdb_popularity),
    MAX(tmdb_popularity),
    ROUND(AVG(tmdb_popularity),2) AS mean,
    PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY tmdb_popularity) AS median
FROM analytics.fact_title_metrics;

SELECT
    MIN(tmdb_score),
    MAX(tmdb_score),
    ROUND(AVG(tmdb_score),2) AS mean,
    PERCENTILE_CONT(0.5)
        WITHIN GROUP (ORDER BY tmdb_score) AS median
FROM analytics.fact_title_metrics;


-- ============================================================================
-- STAGE 16: TREAT NULL VALUES IN fact_title_metrics
-- ============================================================================
-- The original source values remain preserved in raw_title.
-- Missing-value flags are added before imputation so analysts can identify
-- which values were originally present and which values were later estimated.


-- Add Boolean flags to record whether each metric was originally NULL.
ALTER TABLE analytics.fact_title_metrics
ADD COLUMN imdb_score_imputed BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN imdb_votes_imputed BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN tmdb_popularity_imputed BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN tmdb_score_imputed BOOLEAN NOT NULL DEFAULT FALSE;

SELECT *
FROM analytics.fact_title_metrics
LIMIT 10;

-- Mark rows whose IMDb score was originally missing.
UPDATE analytics.fact_title_metrics
SET imdb_score_imputed = TRUE
WHERE imdb_score IS NULL;


-- Mark rows whose IMDb vote count was originally missing.
UPDATE analytics.fact_title_metrics
SET imdb_votes_imputed = TRUE
WHERE imdb_votes IS NULL;


-- Mark rows whose TMDb popularity value was originally missing.
UPDATE analytics.fact_title_metrics
SET tmdb_popularity_imputed = TRUE
WHERE tmdb_popularity IS NULL;


-- Mark rows whose TMDb score was originally missing.
UPDATE analytics.fact_title_metrics
SET tmdb_score_imputed = TRUE
WHERE tmdb_score IS NULL;

-- Replace missing IMDb scores with the observed median score of 6.6.
-- The median was selected because it is close to the mean of 6.51 and is
-- resistant to the influence of unusual observations.
UPDATE analytics.fact_title_metrics
SET imdb_score = 6.6
WHERE imdb_score IS NULL;


-- Replace missing IMDb vote counts with 2,234 votes.
-- The calculated median was 2,233.5, but imdb_votes is an integer column,
-- so the value is rounded to the nearest whole vote.
-- The median is preferred because the distribution is strongly right-skewed.
UPDATE analytics.fact_title_metrics
SET imdb_votes = 2234
WHERE imdb_votes IS NULL;


-- Replace missing TMDb popularity values with the observed median of 6.821.
-- The mean of 22.64 is substantially higher than the median because a small
-- number of highly popular titles pull the distribution upward.
UPDATE analytics.fact_title_metrics
SET tmdb_popularity = 6.821
WHERE tmdb_popularity IS NULL;


-- Replace missing TMDb scores with the observed median score of 6.9.
-- The mean of 6.83 and median of 6.9 are close, so either is defensible;
-- the median is used for consistency and greater resistance to outliers.
UPDATE analytics.fact_title_metrics
SET tmdb_score = 6.9
WHERE tmdb_score IS NULL;

-- Confirm that no NULL values remain in the four numeric metric columns.
SELECT
    COUNT(*) FILTER (WHERE imdb_score IS NULL) AS null_imdb_score,
    COUNT(*) FILTER (WHERE imdb_votes IS NULL) AS null_imdb_votes,
    COUNT(*) FILTER (WHERE tmdb_popularity IS NULL) AS null_tmdb_popularity,
    COUNT(*) FILTER (WHERE tmdb_score IS NULL) AS null_tmdb_score
FROM analytics.fact_title_metrics;


-- Confirm that the imputation flags match the original NULL counts.
-- Expected results:
-- imdb_score_imputed        = 481
-- imdb_votes_imputed        = 497
-- tmdb_popularity_imputed   = 90
-- tmdb_score_imputed        = 310
SELECT
    COUNT(*) FILTER (WHERE imdb_score_imputed) AS imputed_imdb_scores,
    COUNT(*) FILTER (WHERE imdb_votes_imputed) AS imputed_imdb_votes,
    COUNT(*) FILTER (WHERE tmdb_popularity_imputed) AS imputed_tmdb_popularity,
    COUNT(*) FILTER (WHERE tmdb_score_imputed) AS imputed_tmdb_scores
FROM analytics.fact_title_metrics;

-- Display sample records containing at least one imputed metric.
-- This verifies that the replacement values and missing-value flags align.
SELECT
    title_sk,
    imdb_score,
    imdb_score_imputed,
    imdb_votes,
    imdb_votes_imputed,
    tmdb_popularity,
    tmdb_popularity_imputed,
    tmdb_score,
    tmdb_score_imputed
FROM analytics.fact_title_metrics
WHERE imdb_score_imputed
   OR imdb_votes_imputed
   OR tmdb_popularity_imputed
   OR tmdb_score_imputed
LIMIT 20;

--check facts and dimension table
SELECT
    table_name
FROM information_schema.tables
WHERE table_schema = 'analytics'
  AND (
        table_name LIKE 'dim_%'
        OR table_name LIKE 'fact_%'
        OR table_name LIKE 'bridge_%'
      )
ORDER BY table_name;