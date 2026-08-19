-- ============================================================================
-- Synchronise title-to-genre relationships for the current incremental batch.
-- ============================================================================

WITH parsed_genres AS (

    SELECT DISTINCT
        s.id AS title_id,

        TRIM(
            BOTH ' '
            FROM REPLACE(
                REPLACE(
                    REPLACE(genre_value, '[', ''),
                    ']', ''
                ),
                '''',
                ''
            )
        ) AS genre_name

    FROM staging.titles_raw AS s

    CROSS JOIN LATERAL
        UNNEST(
            STRING_TO_ARRAY(s.genres, ',')
        ) AS genre_value

    WHERE s.batch_id = %(batch_id)s
      AND s.genres IS NOT NULL
      AND s.genres <> ''
)

INSERT INTO title_genre (
    title_id,
    genre_id
)

SELECT
    t.title_id,
    g.genre_id

FROM parsed_genres AS pg

INNER JOIN title AS t
    ON t.title_id = pg.title_id

INNER JOIN genre AS g
    ON g.name = pg.genre_name

WHERE pg.genre_name IS NOT NULL
  AND pg.genre_name <> ''

ON CONFLICT (title_id, genre_id)
DO NOTHING;