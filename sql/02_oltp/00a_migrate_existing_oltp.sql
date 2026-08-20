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

-- Ensure country names can be targeted by ON CONFLICT.
ALTER TABLE country
ADD CONSTRAINT uq_country_name UNIQUE (name);


-- Create automatic ID generation for future countries.
CREATE SEQUENCE IF NOT EXISTS country_country_id_seq;


-- Synchronise the sequence with the existing highest country_id.
SELECT setval(
    'country_country_id_seq',
    COALESCE((SELECT MAX(country_id) FROM country), 0) + 1,
    false
);


-- Generate country_id automatically when it is omitted.
ALTER TABLE country
ALTER COLUMN country_id
SET DEFAULT nextval('country_country_id_seq');


-- Associate the sequence with the country_id column.
ALTER SEQUENCE country_country_id_seq
OWNED BY country.country_id;

-- ============================================================================
-- Credit compatibility changes
-- ============================================================================

-- Generate credit IDs automatically for new incremental records.
CREATE SEQUENCE IF NOT EXISTS credit_credit_id_seq;

-- Synchronise the sequence with the highest existing credit ID.
SELECT setval(
    'credit_credit_id_seq',
    COALESCE((SELECT MAX(credit_id) FROM public.credit), 0) + 1,
    false
);

-- Use the sequence whenever credit_id is not explicitly supplied.
ALTER TABLE public.credit
ALTER COLUMN credit_id
SET DEFAULT nextval('credit_credit_id_seq');

-- Associate the sequence with the credit_id column.
ALTER SEQUENCE credit_credit_id_seq
OWNED BY public.credit.credit_id;


-- Prevent duplicate logical credits.
-- COALESCE makes NULL characters comparable for uniqueness purposes.
CREATE UNIQUE INDEX IF NOT EXISTS uq_credit_business_key
ON public.credit (
    title_id,
    person_id,
    role,
    COALESCE(character, '')
);