
-- ============================================================================
-- NETFLIX ANALYTICS WAREHOUSE
-- Stages: OLTP schema -> Staging load -> OLTP population -> OLAP star schema
-- ============================================================================


-- ============================================================================
-- STAGE 1: OLTP SCHEMA (3NF) — public schema
-- ============================================================================
-- These are the "real" normalized tables. Every non-key attribute depends on
-- the whole primary key and nothing but the key (3NF). This is the write-safe,
-- no-redundancy layer everything else is derived from.

-- Genre: simple lookup entity. Extracted from titles.csv's messy list column.
CREATE TABLE genre (
    genre_id INT PRIMARY KEY,
    name TEXT NOT NULL          -- NOT NULL: a genre with no name carries no information
);

-- Country: same shape as genre, same reasoning.
CREATE TABLE country (
    country_id INT PRIMARY KEY,
    name TEXT NOT NULL
);

-- Title: the hub entity. title_id is TEXT (not INT) because real-world ids
-- look like 'tm212909' — this required a migration mid-project once we
-- discovered the true format (see Stage 3).
CREATE TABLE title (
    title_id TEXT PRIMARY KEY,
    title_name TEXT NOT NULL,               -- no legitimate title has no name
    type TEXT NOT NULL CHECK (type IN ('MOVIE', 'SHOW')),
    description TEXT,                       -- nullable: not every row has one
    age_certification VARCHAR(10),          -- nullable: ~45% of titles have no rating (checked against real data)
    runtime INT,                            -- nullable: some rows missing in source
    seasons INT,                            -- nullable: N/A for movies
    imdb_id TEXT                            -- nullable: not every title has one
);

-- Person: one row per unique real-world person, keyed on the id already
-- present in the source data (verified trustworthy via a GROUP BY/HAVING
-- check — see Stage 3).
CREATE TABLE person (
    person_id INT PRIMARY KEY,
    name TEXT NOT NULL
);

-- Credit: associative entity resolving the Person <-> Title many-to-many
-- relationship. Carries role + character because those only make sense
-- attached to a SPECIFIC person-title pairing, not to Person or Title alone.
CREATE TABLE credit (
    credit_id INT PRIMARY KEY,
    title_id TEXT NOT NULL REFERENCES title(title_id),
    person_id INT NOT NULL REFERENCES person(person_id),
    role TEXT NOT NULL CHECK (role IN ('ACTOR', 'DIRECTOR')),
    character TEXT                          -- nullable: directors have no character
);

-- title_genre: pure junction table resolving Title <-> Genre (M:M).
-- No extra attributes needed, so no surrogate id — the pair IS the identity.
CREATE TABLE title_genre (
    title_id TEXT NOT NULL REFERENCES title(title_id),
    genre_id INT NOT NULL REFERENCES genre(genre_id),
    PRIMARY KEY (title_id, genre_id)
);

-- title_country: same pattern as title_genre, for Title <-> Country (M:M).
CREATE TABLE title_country (
    title_id TEXT NOT NULL REFERENCES title(title_id),
    country_id INT NOT NULL REFERENCES country(country_id),
    PRIMARY KEY (title_id, country_id)
);

-- ============================================================================
-- STAGE 4: POPULATE genre AND country FROM raw_title
-- ============================================================================
-- Pattern used for both: strip the surrounding [ ], split on ', ' into an
-- array, unnest the array into one row per element, strip the leftover
-- single quotes off each element, DISTINCT to dedupe, then ROW_NUMBER() to
-- generate a fresh sequential id (no natural id exists for genre/country in
-- the source data).

-- Preview before committing (always check the SELECT before wrapping it in
-- an INSERT).
SELECT ROW_NUMBER() OVER (ORDER BY genre_name) AS genre_id, genre_name
FROM (
    SELECT DISTINCT trim(both '''' from unnest(string_to_array(trim(both '[]' from genres), ', '))) AS genre_name
    FROM raw_title
) AS distinct_genres;

INSERT INTO genre (genre_id, name)
SELECT ROW_NUMBER() OVER (ORDER BY genre_name) AS genre_id, genre_name
FROM (
    SELECT DISTINCT trim(both '''' from unnest(string_to_array(trim(both '[]' from genres), ', '))) AS genre_name
    FROM raw_title
) AS distinct_genres;

SELECT * FROM genre ORDER BY genre_id;

-- Same pattern, production_countries -> country.
SELECT ROW_NUMBER() OVER (ORDER BY country_name) AS country_id, country_name
FROM (
    SELECT DISTINCT trim(both '''' from unnest(string_to_array(trim(both '[]' from production_countries), ', '))) AS country_name
    FROM raw_title
) AS distinct_countries;

INSERT INTO country (country_id, name)
SELECT ROW_NUMBER() OVER (ORDER BY country_name) AS country_id, country_name
FROM (
    SELECT DISTINCT trim(both '''' from unnest(string_to_array(trim(both '[]' from production_countries), ', '))) AS country_name
    FROM raw_title
) AS distinct_countries;

SELECT * FROM country ORDER BY country_id;


-- ============================================================================
-- STAGE 5: POPULATE person FROM raw_credit
-- ============================================================================
-- Unlike genre/country, we KEEP the existing person_id from the source data
-- (proven trustworthy in Stage 3), rather than generating a fresh id.
-- Dedupe on (person_id, name) together via DISTINCT — not on name alone,
-- since two different real people can share a name.

INSERT INTO person (person_id, name)
SELECT DISTINCT person_id::INT, name   -- ::INT casts the TEXT staging value to a real integer
FROM raw_credit;

SELECT COUNT(*) FROM person;


-- ============================================================================
-- STAGE 6: MIGRATION — title_id from INT to TEXT
-- ============================================================================
-- Discovered mid-project: real title ids look like 'tm212909', not clean
-- numbers, so the original INT design was wrong. Because title_id is
-- referenced by three foreign keys, the type change requires dropping those
-- constraints first, changing all four columns, then recreating them.

-- Step 1: find the exact auto-generated constraint names before touching anything.
SELECT conname, conrelid::regclass FROM pg_constraint WHERE contype = 'f';

-- Step 2: drop the FKs that reference title.title_id.
ALTER TABLE credit DROP CONSTRAINT credit_title_id_fkey;
ALTER TABLE title_genre DROP CONSTRAINT title_genre_title_id_fkey;
ALTER TABLE title_country DROP CONSTRAINT title_country_title_id_fkey;

-- Step 3: change the type on both sides of every relationship (order no longer matters).
ALTER TABLE title ALTER COLUMN title_id TYPE TEXT;
ALTER TABLE credit ALTER COLUMN title_id TYPE TEXT;
ALTER TABLE title_genre ALTER COLUMN title_id TYPE TEXT;
ALTER TABLE title_country ALTER COLUMN title_id TYPE TEXT;

-- Step 4: recreate the foreign keys now that both sides match again.
ALTER TABLE credit ADD CONSTRAINT credit_title_id_fkey FOREIGN KEY (title_id) REFERENCES title(title_id);
ALTER TABLE title_genre ADD CONSTRAINT title_genre_title_id_fkey FOREIGN KEY (title_id) REFERENCES title(title_id);
ALTER TABLE title_country ADD CONSTRAINT title_country_title_id_fkey FOREIGN KEY (title_id) REFERENCES title(title_id);

-- Verify the migration landed on all four tables.
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name IN ('title', 'credit', 'title_genre', 'title_country')
AND column_name IN ('title_id')
ORDER BY table_name;


-- ============================================================================
-- STAGE 7: POPULATE title FROM raw_title
-- ============================================================================

-- How many rows have no title at all? (checked before deciding whether to
-- exclude or relax the NOT NULL constraint — 1 row, negligible, excluded.)
SELECT COUNT(*) FROM raw_title WHERE title IS NULL OR title = '';

-- Final INSERT:
--   runtime/seasons cast through NUMERIC first, then INT, because some
--   values are stored as "1.0"-style decimal strings (a pandas export
--   artifact) which a direct ::INT cast rejects.
--   WHERE clause excludes the one row with no title name at all, since
--   title_name is NOT NULL and that row is genuinely bad data.
INSERT INTO title (title_id, title_name, type, description, age_certification, runtime, seasons, imdb_id)
SELECT
    id,
    title,
    type,
    description,
    age_certification,
    runtime::NUMERIC::INT,
    seasons::NUMERIC::INT,
    imdb_id
FROM raw_title
WHERE title IS NOT NULL AND title <> '';

SELECT COUNT(*) FROM title;

-- Row-count reconciliation audit: confirms no duplicate ids in the source,
-- and pins down exactly which raw rows never made it into title (should
-- only ever be the known nameless row).
SELECT id, COUNT(*)
FROM raw_title
GROUP BY id
HAVING COUNT(*) > 1;

SELECT r.id, r.title
FROM raw_title r
LEFT JOIN title t ON r.id = t.title_id
WHERE t.title_id IS NULL;

SELECT COUNT(*) FROM raw_title;
SELECT COUNT(DISTINCT id) FROM raw_title;
SELECT COUNT(*) FROM title;


-- ============================================================================
-- STAGE 8: POPULATE title_genre AND title_country (junction tables)
-- ============================================================================
-- Same unnest pattern as Stage 4, but this time we JOIN the exploded raw
-- text back against the already-clean genre/country tables to resolve each
-- name into its integer id.

INSERT INTO title_genre (title_id, genre_id)
SELECT raw.id, g.genre_id
FROM (
    SELECT id, trim(both '''' from unnest(string_to_array(trim(both '[]' from genres), ', '))) AS genre_name
    FROM raw_title
) AS raw
JOIN genre g ON raw.genre_name = g.name;

-- Preview version for title_country, checked before committing as INSERT.
SELECT raw.id, c.country_id
FROM (
    SELECT id, trim(both '''' from unnest(string_to_array(trim(both '[]' from production_countries), ', '))) AS country
    FROM raw_title
) AS raw
JOIN country c ON raw.country = c.name;

SELECT COUNT(*) FROM (
    SELECT raw.id, c.country_id
    FROM (
        SELECT id, trim(both '''' from unnest(string_to_array(trim(both '[]' from production_countries), ', '))) AS country
        FROM raw_title
    ) AS raw
    JOIN country c ON raw.country = c.name
) AS result;

INSERT INTO title_country (title_id, country_id)
SELECT raw.id, c.country_id
FROM (
    SELECT id, trim(both '''' from unnest(string_to_array(trim(both '[]' from production_countries), ', '))) AS country
    FROM raw_title
) AS raw
JOIN country c ON raw.country = c.name;

SELECT COUNT(*) FROM title_country;
SELECT COUNT(*) FROM title_genre;


-- ============================================================================
-- STAGE 9: POPULATE credit FROM raw_credit
-- ============================================================================

-- Referential-integrity check BEFORE inserting: find any credit rows whose
-- title id doesn't exist in `title` (e.g. the one nameless title we
-- excluded in Stage 7). Inserting these as-is would violate the FK.
SELECT rc.id, rc.name
FROM raw_credit rc
LEFT JOIN title t ON rc.id = t.title_id
WHERE t.title_id IS NULL;

-- Final INSERT: ROW_NUMBER() generates a fresh credit_id (no natural one
-- exists in the source). WHERE id IN (...) filters out the orphaned row(s)
-- found above so the FK to title is never violated.
INSERT INTO credit (credit_id, title_id, person_id, role, character)
SELECT
    ROW_NUMBER() OVER (ORDER BY id) AS credit_id,
    id,
    person_id::INT,
    role,
    character
FROM raw_credit
WHERE id IN (SELECT title_id FROM title);

SELECT COUNT(*) FROM credit;

-- Reconciliation: confirm exactly how many raw rows were excluded, and that
-- raw_credit's true row count matches (row counts can silently shift after
-- a TRUNCATE + reimport, so always re-verify rather than trust old numbers).
SELECT COUNT(*) FROM raw_credit WHERE id NOT IN (SELECT title_id FROM title);
SELECT COUNT(*) FROM raw_credit;

-- Spot check: confirm character is genuinely NULL for at least some rows
-- (directors), proving that column correctly allows NULL.
SELECT * FROM credit WHERE character IS NULL;

-- ============================================================================
-- STAGE 24: STANDARDISE DATA TYPES IN title
-- ============================================================================
-- The title table stores cleaned operational data rather than raw imported
-- values. Columns with well-defined maximum lengths are converted from TEXT
-- to VARCHAR to better reflect their business meaning.
--
-- description remains TEXT because movie and television descriptions are
-- free-form text with no practical maximum length.

ALTER TABLE public.title
ALTER COLUMN title_id TYPE VARCHAR(20),
ALTER COLUMN imdb_id TYPE VARCHAR(20),
ALTER COLUMN title_name TYPE VARCHAR(255),
ALTER COLUMN type TYPE VARCHAR(10);

-- Confirm that the title table now uses bounded VARCHAR types.

SELECT
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'title'
ORDER BY ordinal_position;


-- ============================================================================
-- STAGE 25: STANDARDISE OLTP LOOKUP TABLE DATA TYPES
-- ============================================================================
-- The lookup tables contain descriptive master data that have well-defined
-- maximum lengths. Converting these columns from TEXT to VARCHAR improves
-- schema readability and enforces appropriate length constraints while
-- preserving the existing data.
--
-- These changes apply only to the cleaned OLTP layer. The raw staging tables
-- remain unchanged because they are intended to preserve the imported CSVs
-- exactly as received.

ALTER TABLE public.person
ALTER COLUMN name TYPE VARCHAR(255);

ALTER TABLE public.genre
ALTER COLUMN name TYPE VARCHAR(100);

ALTER TABLE public.country
ALTER COLUMN name TYPE VARCHAR(100);

-- Confirm the lookup tables now use bounded VARCHAR types.

SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public'
AND table_name IN ('person', 'genre', 'country')
ORDER BY table_name, ordinal_position;


-- ============================================================================
-- STAGE 26: VALIDATE LENGTHS IN OLTP RELATIONSHIP TABLES
-- ============================================================================
-- Before converting TEXT columns to bounded VARCHAR types, verify that the
-- existing data fits within the proposed maximum lengths.
--
-- Proposed limits:
--   title_id   -> VARCHAR(20)
--   role       -> VARCHAR(20)
--   character  -> VARCHAR(255)
--
-- A returned value greater than its proposed limit means the limit must be
-- increased before altering the column.

SELECT
    MAX(LENGTH(title_id)) AS max_credit_title_id_length,
    MAX(LENGTH(role)) AS max_role_length,
    MAX(LENGTH(character)) AS max_character_length
FROM public.credit;

SELECT
    MAX(LENGTH(title_id)) AS max_title_genre_id_length
FROM public.title_genre;

SELECT
    MAX(LENGTH(title_id)) AS max_title_country_id_length
FROM public.title_country;


-- ============================================================================
-- STAGE 27: STANDARDISE OLTP RELATIONSHIP TABLE DATA TYPES
-- ============================================================================
-- Existing values were checked before applying bounded VARCHAR types.
--
-- title_id has a maximum observed length of 9 characters, so VARCHAR(20)
-- provides sufficient space while enforcing a sensible identifier limit.
--
-- role has a maximum observed length of 8 characters, so VARCHAR(20)
-- comfortably supports the current controlled values, such as ACTOR and
-- DIRECTOR, while leaving room for reasonable future additions.
--
-- character has a maximum observed length of 298 characters. It remains TEXT
-- because character descriptions are free-form and do not have a reliable
-- business-defined maximum length.
--
-- The junction-table title_id columns are converted to the same VARCHAR(20)
-- type as public.title.title_id to keep related key columns consistent.

ALTER TABLE public.credit
ALTER COLUMN title_id TYPE VARCHAR(20),
ALTER COLUMN role TYPE VARCHAR(20);

ALTER TABLE public.title_genre
ALTER COLUMN title_id TYPE VARCHAR(20);

ALTER TABLE public.title_country
ALTER COLUMN title_id TYPE VARCHAR(20);

-- Confirm that the selected OLTP relationship columns now use VARCHAR(20)
-- while character remains TEXT.

SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('credit', 'title_genre', 'title_country')
  AND column_name IN ('title_id', 'role', 'character')
ORDER BY table_name, ordinal_position;


-- Verify that the title relationships still reference public.title after the
-- compatible data-type changes.

SELECT
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
   AND tc.table_schema = ccu.table_schema
WHERE tc.table_schema = 'public'
  AND tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name IN ('credit', 'title_genre', 'title_country')
ORDER BY tc.table_name, kcu.column_name;

-- ============================================================================
-- STAGE 23: IDENTIFY TEXT COLUMNS IN THE WAREHOUSE
-- ============================================================================
-- Review every TEXT column in the OLTP and analytics schemas before deciding
-- which columns should use bounded VARCHAR data types.
--
-- PostgreSQL stores TEXT and VARCHAR similarly, but VARCHAR allows explicit
-- length constraints where appropriate.

SELECT
    table_schema,
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema IN ('public', 'analytics')
  AND data_type = 'text'
ORDER BY
    table_schema,
    table_name,
    ordinal_position;

	