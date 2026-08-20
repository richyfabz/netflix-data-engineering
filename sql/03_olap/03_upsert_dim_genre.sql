-- ============================================================================
-- Incrementally upsert genres from OLTP into the analytics dimension.
-- ============================================================================

INSERT INTO analytics.dim_genre (
    genre_sk,
    genre_id,
    name
)

SELECT
    COALESCE(
        existing.genre_sk,
        nextval('analytics.dim_genre_sk_seq')
    ),
    g.genre_id,
    g.name

FROM public.genre AS g

LEFT JOIN analytics.dim_genre AS existing
    ON existing.genre_id = g.genre_id

ON CONFLICT (genre_id)

DO UPDATE SET
    name = EXCLUDED.name;