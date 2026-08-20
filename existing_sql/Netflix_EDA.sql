
-- ============================================================================
-- STAGE 13: VALIDATION / BUSINESS QUERIES
-- ============================================================================
-- Proof the star schema pays off: each of these answers a real analytical
-- question with a small number of clean joins.

-- Fact + dimension joined directly, one row per title.
SELECT
    dt.title_name,
    dt.type,
    dt.age_certification,
    ftm.imdb_score,
    ftm.imdb_votes,
    ftm.tmdb_score,
    ftm.tmdb_popularity
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt ON ftm.title_sk = dt.title_sk
ORDER BY ftm.imdb_score DESC;

-- Average score by title type (movie vs show).
SELECT dt.type, ROUND(AVG(ftm.imdb_score), 2) AS avg_score, COUNT(*) AS title_count
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt ON ftm.title_sk = dt.title_sk
GROUP BY dt.type;

-- Top 10 most-credited people (acting + directing combined).
SELECT dp.name, COUNT(*) AS credit_count
FROM analytics.fact_credits fc
JOIN analytics.dim_person dp ON fc.person_sk = dp.person_sk
GROUP BY dp.name
ORDER BY credit_count DESC
LIMIT 10;

-- Average score, rated vs unrated titles (CASE WHEN buckets a NULL check
-- into a readable two-group label).
SELECT
    CASE WHEN dt.age_certification IS NULL THEN 'Unrated' ELSE 'Rated' END AS certification_status,
    ROUND(AVG(ftm.imdb_score), 2) AS avg_score,
    COUNT(*) AS title_count
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt ON ftm.title_sk = dt.title_sk
GROUP BY certification_status;

-- Top 10 genres by number of titles (OLTP-side; superseded by the
-- analytics-native version further down after dim_genre was added).
SELECT g.name, COUNT(*) AS title_count
FROM title_genre tg
JOIN genre g ON tg.genre_id = g.genre_id
GROUP BY g.name
ORDER BY title_count DESC
LIMIT 10;

-- Top 10 countries by number of titles produced.
SELECT c.name, COUNT(*) AS title_count
FROM title_country tc
JOIN country c ON tc.country_id = c.country_id
GROUP BY c.name
ORDER BY title_count DESC
LIMIT 10;

-- Average score by genre, fully within the analytics schema — the clean
-- version, made possible by Stage 12's dim_genre + bridge_title_genre.
SELECT dg.name, ROUND(AVG(ftm.imdb_score), 2) AS avg_score, COUNT(*) AS title_count
FROM analytics.fact_title_metrics ftm
JOIN analytics.bridge_title_genre btg ON ftm.title_sk = btg.title_sk
JOIN analytics.dim_genre dg ON btg.genre_sk = dg.genre_sk
GROUP BY dg.name
ORDER BY avg_score DESC;

-- Top 10 titles by score, with a minimum vote threshold so a title with
-- 3 votes and a fluke perfect score can't top the list.
SELECT dt.title_name, ftm.imdb_score, ftm.imdb_votes
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt ON ftm.title_sk = dt.title_sk
WHERE ftm.imdb_votes >= 1000
ORDER BY ftm.imdb_score DESC
LIMIT 10;

-- People who have both acted AND directed at least once (HAVING filters on
-- an aggregate, same pattern used to catch the "Alex Diaz" case earlier).
SELECT dp.name,
       COUNT(DISTINCT fc.role) AS distinct_roles,
       STRING_AGG(DISTINCT fc.role, ', ') AS roles_held
FROM analytics.fact_credits fc
JOIN analytics.dim_person dp ON fc.person_sk = dp.person_sk
GROUP BY dp.name
HAVING COUNT(DISTINCT fc.role) > 1;

-- Average score by type and release decade. Integer division buckets a
-- year like 1994 down to 1990 (truncate remainder, multiply back by 10).
SELECT
    dt.type,
    (r.release_year::INT / 10) * 10 AS decade,
    ROUND(AVG(ftm.imdb_score), 2) AS avg_score,
    COUNT(*) AS title_count
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt ON ftm.title_sk = dt.title_sk
JOIN raw_title r ON dt.title_id = r.id
GROUP BY dt.type, decade
ORDER BY decade, dt.type;


-- ============================================================================
-- SECTION A: Executive Overview
-- BUSINESS QUERY 1: NETFLIX CATALOGUE EXECUTIVE SUMMARY
-- ============================================================================
-- This query provides high-level indicators describing the overall Netflix
-- catalogue represented in the warehouse.
--
-- It returns:
--   1. total number of titles
--   2. total number of movies
--   3. total number of shows
--   4. percentage of the catalogue made up of movies
--   5. percentage of the catalogue made up of shows
--   6. earliest release year
--   7. latest release year
--
-- dim_title is used because its grain is one row per unique Netflix title.

SELECT
    COUNT(*) AS total_titles,

    COUNT(*) FILTER (
        WHERE type = 'MOVIE'
    ) AS total_movies,

    COUNT(*) FILTER (
        WHERE type = 'SHOW'
    ) AS total_shows,

    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'MOVIE')
        / NULLIF(COUNT(*), 0),
        2
    ) AS movie_percentage,

    ROUND(
        100.0 * COUNT(*) FILTER (WHERE type = 'SHOW')
        / NULLIF(COUNT(*), 0),
        2
    ) AS show_percentage,

    MIN(release_year) AS earliest_release_year,

    MAX(release_year) AS latest_release_year

FROM analytics.dim_title;


-- ============================================================================
-- SECTION A: Executive Overview
-- BUSINESS QUERY 2: NETFLIX CATALOGUE PERFORMANCE SUMMARY
-- ============================================================================
-- This query summarises the overall quality and popularity of the Netflix
-- catalogue using the numerical measures stored in fact_title_metrics.
--
-- It returns:
--   1. average IMDb score
--   2. average TMDb score
--   3. average IMDb votes
--   4. average TMDb popularity
--
-- fact_title_metrics is used because it stores one metrics record per title.

SELECT
    ROUND(AVG(imdb_score), 2) AS average_imdb_score,
    ROUND(AVG(tmdb_score), 2) AS average_tmdb_score,
    ROUND(AVG(imdb_votes), 0) AS average_imdb_votes,
    ROUND(AVG(tmdb_popularity), 2) AS average_tmdb_popularity
FROM analytics.fact_title_metrics;


-- ============================================================================
-- SECTION B: Content Trends
-- BUSINESS QUERY 3: CONTENT RELEASE TREND BY YEAR
-- ============================================================================
-- This query analyses the number of titles released each year.
--
-- It answers the business question:
-- "How has the Netflix catalogue evolved over time?"
--
-- dim_title is used because each row represents one unique title.
-- The result can be visualised as a line chart or column chart to show
-- periods of growth and decline in content production.

SELECT
    release_year,
    COUNT(*) AS total_titles
FROM analytics.dim_title
GROUP BY release_year
ORDER BY release_year;


-- ============================================================================
-- SECTION B: Content Trends
-- BUSINESS QUERY 4: MOVIES VS TV SHOWS RELEASED BY YEAR
-- ============================================================================
-- This query compares the annual release trend of Movies and TV Shows.
--
-- It answers the business question:
-- "How has the production of Movies and TV Shows changed over time?"
--
-- dim_title is used because each row represents one unique Netflix title.
--
-- The result is intended for a multi-line chart or stacked column chart
-- showing the evolution of both content types across the release years.

SELECT
    release_year,
    type,
    COUNT(*) AS total_titles
FROM analytics.dim_title
GROUP BY
    release_year,
    type
ORDER BY
    release_year,
    type;


-- ============================================================================
-- SECTION C: GENRE ANALYSIS
-- BUSINESS QUERY 5: MOST REPRESENTED GENRES IN THE NETFLIX CATALOGUE
-- ============================================================================
-- This query identifies the genres with the largest number of title
-- assignments in the Netflix catalogue.

-- bridge_title_genre is required because titles and genres have a many-to-many
-- relationship: one title may belong to several genres, and one genre may
-- contain many titles.
--
-- COUNT(*) measures the number of title-genre relationships for each genre.
--
-- SUM(COUNT(*)) OVER () calculates the total number of genre assignments across
-- the complete result without requiring a separate query.
--
-- Important:
-- percentage_of_genre_assignments is not the percentage of unique catalogue
-- titles. Because titles may have multiple genres, it represents each genre's
-- share of all title-genre assignments.
--
-- The result is suitable for a horizontal bar chart ordered from the most
-- represented genre to the least represented genre.

SELECT
    dg.name AS genre,
    COUNT(*) AS total_titles,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
        2
    ) AS percentage_of_genre_assignments
FROM analytics.bridge_title_genre btg
JOIN analytics.dim_genre dg
    ON btg.genre_sk = dg.genre_sk
GROUP BY
    dg.name
ORDER BY
    total_titles DESC,
    genre;


	-- ============================================================================
-- SECTION C: GENRE ANALYSIS
-- BUSINESS QUERY 6: HIGHEST-RATED GENRES BY AVERAGE IMDb SCORE
-- ============================================================================
-- This query compares genres according to their average IMDb score.
--
-- Business question:
-- "Which sufficiently represented genres receive the strongest IMDb ratings?"
--
-- fact_title_metrics supplies the IMDb score measure.
-- bridge_title_genre resolves the many-to-many title-genre relationship.
-- dim_genre supplies the readable genre name.
--
-- COUNT(DISTINCT btg.title_sk) measures the number of unique titles assigned
-- to each genre.
--
-- HAVING COUNT(DISTINCT btg.title_sk) >= 50 excludes genres represented by
-- fewer than 50 titles. This reduces the risk of ranking a very small genre
-- highly because of only a few unusually strong titles.
--
-- AVG(imdb_score) calculates the genre-level average after the NULL values
-- have already been treated in fact_title_metrics.
--
-- The result is suitable for a ranked horizontal bar chart.

SELECT
    dg.name AS genre,
    COUNT(DISTINCT btg.title_sk) AS total_titles,
    ROUND(AVG(ftm.imdb_score), 2) AS average_imdb_score
FROM analytics.bridge_title_genre btg
JOIN analytics.dim_genre dg
    ON btg.genre_sk = dg.genre_sk
JOIN analytics.fact_title_metrics ftm
    ON btg.title_sk = ftm.title_sk
GROUP BY
    dg.name
HAVING COUNT(DISTINCT btg.title_sk) >= 50
ORDER BY
    average_imdb_score DESC,
    total_titles DESC,
    genre;

	-- ============================================================================
-- SECTION C: GENRE ANALYSIS
-- BUSINESS QUERY 7: MOST POPULAR GENRES BY AVERAGE TMDb POPULARITY
-- ============================================================================
-- This query compares genres according to their average TMDb popularity.
--
-- Business question:
-- "Which sufficiently represented genres attract the highest average level
-- of audience attention and engagement?"
--
-- fact_title_metrics supplies the TMDb popularity measure.
-- bridge_title_genre resolves the many-to-many relationship between titles
-- and genres.
-- dim_genre supplies the readable genre name.
--
-- A minimum threshold of 50 unique titles is applied so that genres with only
-- a small number of titles do not dominate the ranking because of a few
-- exceptionally popular releases.
--
-- The result is suitable for a ranked horizontal bar chart.

SELECT
    dg.name AS genre,
    COUNT(DISTINCT btg.title_sk) AS total_titles,
    ROUND(AVG(ftm.tmdb_popularity), 2) AS average_tmdb_popularity
FROM analytics.bridge_title_genre btg
JOIN analytics.dim_genre dg
    ON btg.genre_sk = dg.genre_sk
JOIN analytics.fact_title_metrics ftm
    ON btg.title_sk = ftm.title_sk
GROUP BY
    dg.name
HAVING COUNT(DISTINCT btg.title_sk) >= 50
ORDER BY
    average_tmdb_popularity DESC,
    total_titles DESC,
    genre;


-- ============================================================================
-- SECTION D: TALENT ANALYSIS
-- BUSINESS QUERY 8: MOST FREQUENTLY CREDITED PEOPLE BY ROLE
-- ============================================================================
-- This query identifies the people with the highest number of title credits,
-- separated by their recorded role.
--
-- Business question:
-- "Which actors and directors appear most frequently across the catalogue?"
--
-- fact_credits contains one row per person-title-role-character relationship.
-- dim_person provides the readable person name.
--
-- COUNT(DISTINCT fc.title_sk) counts the number of unique titles associated
-- with each person and role, preventing multiple character entries for the
-- same title from inflating the title count.
--
-- ROW_NUMBER ranks people within each role so that the top 10 actors and top
-- 10 directors can be returned in one query.

WITH ranked_people AS (
    SELECT
        fc.role,
        dp.name AS person_name,
        COUNT(DISTINCT fc.title_sk) AS total_titles,
        ROW_NUMBER() OVER (
            PARTITION BY fc.role
            ORDER BY COUNT(DISTINCT fc.title_sk) DESC, dp.name
        ) AS role_rank
    FROM analytics.fact_credits fc
    JOIN analytics.dim_person dp
        ON fc.person_sk = dp.person_sk
    GROUP BY
        fc.role,
        dp.name
)
SELECT
    role,
    person_name,
    total_titles,
    role_rank
FROM ranked_people
WHERE role_rank <= 10
ORDER BY
    role,
    role_rank;

-- ============================================================================
-- SECTION E: AUDIENCE AND CERTIFICATION ANALYSIS
-- BUSINESS QUERY 9: AGE CERTIFICATION DISTRIBUTION BY CONTENT TYPE
-- ============================================================================
-- This query examines how age certifications are distributed across Movies
-- and Shows.
--
-- Business question:
-- "Which audience classifications are most common for each content type?"
--
-- dim_title is used because each row represents one unique title.
--
-- Titles without a recorded age certification are grouped as 'Unrated or
-- Unknown' rather than being excluded, because missing certification is a
-- meaningful characteristic of the source data.
--
-- The percentage is calculated within each content type, meaning Movie
-- percentages add to 100% and Show percentages add to 100% independently.
--
-- The result is suitable for a stacked column chart or 100% stacked bar chart.

WITH certification_counts AS (
    SELECT
        type,
        COALESCE(age_certification, 'Unrated or Unknown') AS certification,
        COUNT(*) AS total_titles
    FROM analytics.dim_title
    GROUP BY
        type,
        COALESCE(age_certification, 'Unrated or Unknown')
)
SELECT
    type,
    certification,
    total_titles,
    ROUND(
        100.0 * total_titles
        / SUM(total_titles) OVER (PARTITION BY type),
        2
    ) AS percentage_within_type
FROM certification_counts
ORDER BY
    type,
    total_titles DESC,
    certification;

	-- ============================================================================
-- SECTION F: ADVANCED TITLE INSIGHTS
-- BUSINESS QUERY 10: IDENTIFY HIGH-RATED TITLES WITH LIMITED IMDb EXPOSURE
-- ============================================================================
-- This query identifies potential "hidden gems": titles with strong IMDb
-- scores but relatively low vote counts.
--
-- Business question:
-- "Which highly rated titles may be under-recognised by the wider audience?"
--
-- A minimum IMDb score of 7.5 is used to define strong audience reception.
-- IMDb votes are restricted to between 100 and 5,000:
--   • at least 100 votes provides a basic level of rating credibility
--   • no more than 5,000 votes identifies titles with comparatively limited
--     audience exposure
--
-- dim_title supplies descriptive title information.
-- fact_title_metrics supplies rating, vote and popularity measures.
--
-- The query excludes imputed IMDb scores and vote counts so that hidden-gem
-- classification is based only on originally observed IMDb data.
--
-- Results are ranked first by IMDb score and then by IMDb votes.

SELECT
    dt.title_name,
    dt.type,
    dt.release_year,
    ftm.imdb_score,
    ftm.imdb_votes,
    ftm.tmdb_score,
    ftm.tmdb_popularity
FROM analytics.fact_title_metrics ftm
JOIN analytics.dim_title dt
    ON ftm.title_sk = dt.title_sk
WHERE ftm.imdb_score >= 7.5
  AND ftm.imdb_votes BETWEEN 100 AND 5000
  AND ftm.imdb_score_imputed = FALSE
  AND ftm.imdb_votes_imputed = FALSE
ORDER BY
    ftm.imdb_score DESC,
    ftm.imdb_votes ASC,
    dt.title_name
LIMIT 20;



/* ================================================================
   BUSINESS VALIDATION

   Return production countries ranked by the number of distinct
   titles associated with each country.

   This also verifies that:

       dim_title
           ↓
       bridge_title_country
           ↓
       dim_country

   are joining correctly.
   ================================================================ */

SELECT
    dc.name AS production_country,
    COUNT(DISTINCT btc.title_sk) AS total_titles
FROM analytics.bridge_title_country AS btc

INNER JOIN analytics.dim_country AS dc
    ON dc.country_sk = btc.country_sk

GROUP BY
    dc.name

ORDER BY
    total_titles DESC,
    production_country;

/* ================================================================
   MULTI-COUNTRY PRODUCTIONS

   Identify titles associated with more than one production country.

   This validates the decision to use a bridge table instead of
   storing a single country directly inside dim_title.
   ================================================================ */

SELECT
    dt.title_name,
    COUNT(DISTINCT btc.country_sk) AS production_country_count
FROM analytics.bridge_title_country AS btc

INNER JOIN analytics.dim_title AS dt
    ON dt.title_sk = btc.title_sk

GROUP BY
    dt.title_sk,
    dt.title_name

HAVING COUNT(DISTINCT btc.country_sk) > 1

ORDER BY
    production_country_count DESC,
    dt.title_name;

	