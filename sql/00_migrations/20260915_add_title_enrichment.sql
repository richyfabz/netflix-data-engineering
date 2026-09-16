BEGIN;

CREATE TABLE IF NOT EXISTS uat_olap.title_enrichment (
    title_sk       BIGINT PRIMARY KEY,
    imdb_id        VARCHAR(20),
    tmdb_id        BIGINT,
    tmdb_title     TEXT,
    poster_path    TEXT,
    poster_url     TEXT,
    match_status   VARCHAR(20) NOT NULL,
    enriched_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_title_enrichment_title
        FOREIGN KEY (title_sk)
        REFERENCES uat_olap.dim_title(title_sk)
        ON DELETE CASCADE,

    CONSTRAINT chk_title_enrichment_status
        CHECK (match_status IN (
            'MATCHED',
            'NOT_FOUND',
            'NO_IMDB_ID',
            'ERROR'
        ))
);

CREATE INDEX IF NOT EXISTS idx_title_enrichment_imdb_id
    ON uat_olap.title_enrichment(imdb_id);
CREATE INDEX IF NOT EXISTS idx_title_enrichment_tmdb_id
    ON uat_olap.title_enrichment(tmdb_id);
COMMIT;