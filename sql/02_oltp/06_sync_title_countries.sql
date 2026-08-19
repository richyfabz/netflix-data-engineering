-- ============================================================================
-- Synchronise title-to-country relationships for the current incremental batch.
-- ============================================================================

WITH parsed_countries AS (

    SELECT DISTINCT
        s.id AS title_id,

        TRIM(
            BOTH ' '
            FROM REPLACE(
                REPLACE(
                    REPLACE(country_value, '[', ''),
                    ']', ''
                ),
                '''',
                ''
            )
        ) AS country_name

    FROM staging.titles_raw AS s

    CROSS JOIN LATERAL
        UNNEST(
            STRING_TO_ARRAY(s.production_countries, ',')
        ) AS country_value

    WHERE s.batch_id = %(batch_id)s
      AND s.production_countries IS NOT NULL
      AND s.production_countries <> ''
)

INSERT INTO title_country (
    title_id,
    country_id
)

SELECT
    t.title_id,
    c.country_id

FROM parsed_countries AS pc

INNER JOIN title AS t
    ON t.title_id = pc.title_id

INNER JOIN country AS c
    ON c.name = pc.country_name

WHERE pc.country_name IS NOT NULL
  AND pc.country_name <> ''

ON CONFLICT (title_id, country_id)
DO NOTHING;

