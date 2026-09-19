CREATE OR REPLACE VIEW production.country_top_titles AS

WITH ranked_titles AS (
    SELECT
        dc.country_name,
        dto.title_sk,
        dto.title,
        dto.type,
        dto.release_year,
        dto.imdb_score,
        dto.tmdb_score,
        dto.poster_url,

        ROW_NUMBER() OVER (
            PARTITION BY dc.country_name
            ORDER BY
                dto.imdb_score DESC NULLS LAST,
                dto.imdb_votes DESC NULLS LAST,
                dto.title ASC
        ) AS country_imdb_rank

    FROM production.title_overview AS dto

    JOIN uat_olap.bridge_title_country AS btc
        ON btc.title_sk = dto.title_sk

    JOIN uat_olap.dim_country AS dc
        ON dc.country_sk = btc.country_sk

    WHERE dto.poster_url IS NOT NULL
      AND dto.imdb_score IS NOT NULL
)

SELECT
    country_name,
    title_sk,
    title,
    type,
    release_year,
    imdb_score,
    tmdb_score,
    poster_url,
    country_imdb_rank

FROM ranked_titles

WHERE country_imdb_rank <= 5;