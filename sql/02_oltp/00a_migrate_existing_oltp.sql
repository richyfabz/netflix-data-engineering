-- Compatibility changes for the original OLTP database.
-- Required before running the automated incremental pipeline.


-- Allow ON CONFLICT (name) during genre upserts.
ALTER TABLE genre
ADD CONSTRAINT uq_genre_name UNIQUE (name);


-- Generate IDs automatically for genres arriving in future batches.
CREATE SEQUENCE IF NOT EXISTS genre_genre_id_seq;

SELECT setval(
    'genre_genre_id_seq',
    COALESCE((SELECT MAX(genre_id) FROM genre), 0) + 1,
    false
);

ALTER TABLE genre
ALTER COLUMN genre_id
SET DEFAULT nextval('genre_genre_id_seq');

ALTER SEQUENCE genre_genre_id_seq
OWNED BY genre.genre_id;