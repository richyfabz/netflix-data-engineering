-- ============================================================================
-- Incrementally upsert unique genres from the current titles batch.
-- ============================================================================



WITH parsed_genres AS (

    SELECT DISTINCT
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

    FROM dev.titles_raw

    CROSS JOIN LATERAL
        UNNEST(
            STRING_TO_ARRAY(genres, ',')
        ) AS genre_value

    WHERE batch_id = %(batch_id)s
      AND genres IS NOT NULL
      AND genres <> ''
)

INSERT INTO uat_oltp.genre (
    name
)

SELECT
    genre_name

FROM parsed_genres

WHERE genre_name IS NOT NULL
  AND genre_name <> ''

ON CONFLICT (name)
DO NOTHING;