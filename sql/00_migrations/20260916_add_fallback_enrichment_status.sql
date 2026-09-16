-- Allow validated title/year/type fallback matches in title enrichment.
--
-- MATCHED:
--   TMDB matched directly using the source IMDb ID.
--
-- FALLBACK_MATCHED:
--   IMDb lookup failed, but a validated TMDB title/type/year
--   search produced a match.

ALTER TABLE uat_olap.title_enrichment
DROP CONSTRAINT IF EXISTS chk_title_enrichment_status;

ALTER TABLE uat_olap.title_enrichment
ADD CONSTRAINT chk_title_enrichment_status
CHECK (
    match_status IN (
        'MATCHED',
        'FALLBACK_MATCHED',
        'NOT_FOUND',
        'NO_IMDB_ID',
        'ERROR'
    )
);