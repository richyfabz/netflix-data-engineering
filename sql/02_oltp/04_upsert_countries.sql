-- ============================================================================
-- Incrementally upsert unique production countries from the current batch.
-- ============================================================================

WITH parsed_countries AS (

    SELECT DISTINCT
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

    FROM staging.titles_raw

    CROSS JOIN LATERAL
        UNNEST(
            STRING_TO_ARRAY(production_countries, ',')
        ) AS country_value

    WHERE batch_id = %(batch_id)s
      AND production_countries IS NOT NULL
      AND production_countries <> ''
)

INSERT INTO country (
    name
)

SELECT
    country_name

FROM parsed_countries

WHERE country_name IS NOT NULL
  AND country_name <> ''

ON CONFLICT (name)
DO NOTHING;