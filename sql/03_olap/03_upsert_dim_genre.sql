-- ============================================================================
-- Incrementally upsert genres from OLTP into the analytics dimension.
-- ============================================================================

INSERT INTO uat_olap.dim_genre (
    genre_sk,
    genre_id,
    name
)

SELECT
    COALESCE(
        existing.genre_sk,
        nextval('uat_olap.dim_genre_sk_seq')
    ),
    g.genre_id,
    g.name

FROM uat_oltp.genre AS g

LEFT JOIN uat_olap.dim_genre AS existing
    ON existing.genre_id = g.genre_id

ON CONFLICT (genre_id)

DO UPDATE SET
    name = EXCLUDED.name;