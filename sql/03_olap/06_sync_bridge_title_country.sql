-- ============================================================================
-- Synchronise title-to-country relationships for the current titles batch.
-- ============================================================================

-- Remove existing relationships for titles being refreshed.
DELETE FROM analytics.bridge_title_country AS bridge
USING analytics.dim_title AS dt,
      staging.titles_raw AS staging

WHERE bridge.title_sk = dt.title_sk
  AND dt.title_id = staging.id
  AND staging.batch_id = %(batch_id)s;


-- Rebuild relationships from the current OLTP state.
INSERT INTO analytics.bridge_title_country (
    title_sk,
    country_sk
)

SELECT DISTINCT
    dt.title_sk,
    dc.country_sk

FROM public.title_country AS oltp_bridge

INNER JOIN analytics.dim_title AS dt
    ON dt.title_id = oltp_bridge.title_id

INNER JOIN analytics.dim_country AS dc
    ON dc.country_id = oltp_bridge.country_id

INNER JOIN staging.titles_raw AS staging
    ON staging.id = oltp_bridge.title_id
   AND staging.batch_id = %(batch_id)s

ON CONFLICT (title_sk, country_sk)
DO NOTHING;