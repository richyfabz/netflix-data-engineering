-- ============================================================================
-- Synchronise title-to-country relationships for the current titles batch.
-- ============================================================================

-- Remove existing relationships for titles being refreshed.
DELETE FROM uat_olap.bridge_title_country AS bridge
USING uat_olap.dim_title AS dt,
      dev.titles_raw AS staging

WHERE bridge.title_sk = dt.title_sk
  AND dt.title_id = staging.id
  AND staging.batch_id = %(batch_id)s;


-- Rebuild relationships from the current OLTP state.
INSERT INTO uat_olap.bridge_title_country (
    title_sk,
    country_sk
)

SELECT DISTINCT
    dt.title_sk,
    dc.country_sk

FROM uat_oltp.title_country AS oltp_bridge

INNER JOIN uat_olap.dim_title AS dt
    ON dt.title_id = oltp_bridge.title_id

INNER JOIN uat_olap.dim_country AS dc
    ON dc.country_id = oltp_bridge.country_id

INNER JOIN dev.titles_raw AS staging
    ON staging.id = oltp_bridge.title_id
   AND staging.batch_id = %(batch_id)s

ON CONFLICT (title_sk, country_sk)
DO NOTHING;