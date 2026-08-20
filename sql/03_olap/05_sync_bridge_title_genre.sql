-- ============================================================================
-- Synchronise title-to-genre relationships for the current titles batch.
-- ============================================================================

-- Remove existing relationships for titles being refreshed.
DELETE FROM analytics.bridge_title_genre AS bridge
USING analytics.dim_title AS dt,
      staging.titles_raw AS staging

WHERE bridge.title_sk = dt.title_sk
  AND dt.title_id = staging.id
  AND staging.batch_id = %(batch_id)s;


-- Rebuild relationships from the current OLTP state.
INSERT INTO analytics.bridge_title_genre (
    title_sk,
    genre_sk
)

SELECT DISTINCT
    dt.title_sk,
    dg.genre_sk

FROM public.title_genre AS oltp_bridge

INNER JOIN analytics.dim_title AS dt
    ON dt.title_id = oltp_bridge.title_id

INNER JOIN analytics.dim_genre AS dg
    ON dg.genre_id = oltp_bridge.genre_id

INNER JOIN staging.titles_raw AS staging
    ON staging.id = oltp_bridge.title_id
   AND staging.batch_id = %(batch_id)s

ON CONFLICT (title_sk, genre_sk)
DO NOTHING;