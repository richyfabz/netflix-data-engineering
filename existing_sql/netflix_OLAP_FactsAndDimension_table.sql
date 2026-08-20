-- ============================================================================
-- STAGE 10: OLAP STAR SCHEMA — analytics schema
-- ============================================================================
-- Kept in a separate schema from the OLTP tables so the two layers stay
-- visually and organizationally distinct. Designed HONESTLY: no SCD2, no
-- periodic snapshot facts, because the source dataset is a single static
-- extract with no repeated historical pulls to actually version. Fabricating
-- synthetic history to force those patterns in would misrepresent the data.

CREATE SCHEMA IF NOT EXISTS analytics;

-- dim_title: current-state dimension. title_sk is a SURROGATE key (Postgres
-- auto-generates it via SERIAL) — deliberately meaningless, decoupled from
-- the real title_id, which is kept alongside as the natural key. This is
-- what would allow future SCD2 versioning without a redesign, even though
-- we're not implementing it now.
CREATE TABLE analytics.dim_title(
    title_sk SERIAL PRIMARY KEY,
    title_id TEXT NOT NULL UNIQUE,   -- migrated from INT to TEXT, see below
    title_name TEXT,
    type TEXT NOT NULL,
    age_certification TEXT           -- NOT NULL dropped after finding ~45% of titles are unrated
);

SELECT * FROM analytics.fact_title_metrics;

CREATE TABLE analytics.dim_person (
    person_sk SERIAL PRIMARY KEY,
    person_id INT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

-- fact_credits: FACTLESS fact table. No measure column — the presence of a
-- row IS the fact (this person was credited on this title, in this role).
-- Analytical value comes from COUNT(*) grouped by dimension, not from
-- summing anything.
CREATE TABLE analytics.fact_credits(
    title_sk INT NOT NULL REFERENCES analytics.dim_title(title_sk),
    person_sk INT NOT NULL REFERENCES analytics.dim_person(person_sk),
    role TEXT NOT NULL,
    character TEXT
);

-- fact_title_metrics: current-state fact (NOT a periodic snapshot — that
-- would require multiple real observations over time, which we don't have).
-- One row per title, holding the actual numeric measures.
CREATE TABLE analytics.fact_title_metrics(
    title_sk INT NOT NULL REFERENCES analytics.dim_title(title_sk),
    imdb_score NUMERIC,
    imdb_votes INT,
    tmdb_popularity NUMERIC,
    tmdb_score NUMERIC
);

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'analytics';


-- ============================================================================
-- STAGE 11: POPULATE THE STAR SCHEMA FROM OLTP
-- ============================================================================

INSERT INTO analytics.dim_person (person_id, name)
SELECT person_id, name FROM person;

-- dim_title.title_id was originally created as INT by mistake (the earlier
-- OLTP migration lesson didn't carry over to this brand-new table) —
-- corrected the same way, but simpler here since nothing referenced it yet.
ALTER TABLE analytics.dim_title ALTER COLUMN title_id TYPE TEXT;

-- Relaxed after discovering ~45% of titles (2,618 / 5,849) have no
-- age_certification — that's normal missing data, not a data-quality bug,
-- so the constraint (not the data) was wrong.
ALTER TABLE analytics.dim_title ALTER COLUMN age_certification DROP NOT NULL;

INSERT INTO analytics.dim_title (title_id, title_name, type, age_certification)
SELECT title_id, title_name, type, age_certification
FROM title;

SELECT COUNT(*) FROM title;

-- fact_credits: JOIN credit back to both dimensions to translate the OLTP's
-- natural keys (title_id, person_id) into the OLAP's surrogate keys
-- (title_sk, person_sk).
INSERT INTO analytics.fact_credits (title_sk, person_sk, role, character)
SELECT dt.title_sk, dp.person_sk, c.role, c.character
FROM credit c
JOIN analytics.dim_title dt ON c.title_id = dt.title_id
JOIN analytics.dim_person dp ON c.person_id = dp.person_id;

SELECT COUNT(*) FROM analytics.fact_credits;

-- fact_title_metrics: sourced from raw_title (staging), NOT the OLTP title
-- table — the score/vote columns were deliberately never loaded into OLTP,
-- since they only belong in the analytical layer.
INSERT INTO analytics.fact_title_metrics (title_sk, imdb_score, imdb_votes, tmdb_popularity, tmdb_score)
SELECT dt.title_sk,
       r.imdb_score::NUMERIC,
       r.imdb_votes::NUMERIC::INT,
       r.tmdb_popularity::NUMERIC,
       r.tmdb_score::NUMERIC
FROM raw_title r
JOIN analytics.dim_title dt ON r.id = dt.title_id;

SELECT COUNT(*) FROM analytics.fact_title_metrics;


-- ============================================================================
-- STAGE 12: SCHEMA FIX — add dim_genre + bridge, add release_year
-- ============================================================================
-- Discovered while testing "average score by genre": the query needed to
-- reach out of the analytics schema and into public + staging, defeating
-- the whole point of a star schema. Patched by adding the missing pieces
-- properly rather than leaving the gap undocumented.

ALTER TABLE analytics.dim_title ADD COLUMN release_year INT;

UPDATE analytics.dim_title dt
SET release_year = r.release_year::NUMERIC::INT
FROM raw_title r
WHERE dt.title_id = r.id;

SELECT COUNT(*) FROM analytics.dim_title WHERE release_year IS NULL;

-- dim_genre + bridge_title_genre: same M:M -> bridge-table pattern used
-- throughout the OLTP layer, now mirrored inside the analytics schema.
CREATE TABLE analytics.dim_genre (
    genre_sk SERIAL PRIMARY KEY,
    genre_id INT NOT NULL UNIQUE,
    name TEXT NOT NULL
);

CREATE TABLE analytics.bridge_title_genre (
    title_sk INT NOT NULL REFERENCES analytics.dim_title(title_sk),
    genre_sk INT NOT NULL REFERENCES analytics.dim_genre(genre_sk)
);

INSERT INTO analytics.dim_genre (genre_id, name)
SELECT genre_id, name FROM genre;

INSERT INTO analytics.bridge_title_genre (title_sk, genre_sk)
SELECT dt.title_sk, dg.genre_sk
FROM title_genre tg
JOIN analytics.dim_title dt ON tg.title_id = dt.title_id
JOIN analytics.dim_genre dg ON tg.genre_id = dg.genre_id;

SELECT COUNT(*) FROM analytics.dim_genre;
SELECT COUNT(*) FROM analytics.bridge_title_genre;

-- ============================================================================
-- STAGE 17: ADD A SURROGATE KEY TO fact_credits
-- ============================================================================
-- Each row in fact_credits represents one credit relationship between a title
-- and a person. The table currently has no independent row identifier.
--
-- credit_sk is introduced as a warehouse-generated surrogate key.
-- GENERATED ALWAYS AS IDENTITY automatically assigns a sequential integer to
-- every existing row and to every new row inserted in the future.
--
-- This does not replace title_sk or person_sk. Those columns remain foreign
-- keys used to connect the fact table to dim_title and dim_person.

ALTER TABLE analytics.fact_credits
ADD COLUMN credit_sk BIGINT GENERATED ALWAYS AS IDENTITY;

-- Confirm that credit_sk was added and automatically populated.

SELECT
    credit_sk,
    title_sk,
    person_sk,
    role,
    character
FROM analytics.fact_credits
ORDER BY credit_sk
LIMIT 10;

-- ============================================================================
-- STAGE 18: MAKE credit_sk THE PRIMARY KEY OF fact_credits
-- ============================================================================
-- credit_sk now uniquely identifies each row in fact_credits.
--
-- Defining it as the primary key enforces:
--   1. uniqueness — no two fact rows can share the same credit_sk
--   2. NOT NULL — every fact row must have a credit_sk
--   3. indexing — PostgreSQL automatically creates a unique index for the key
--
-- title_sk and person_sk remain foreign keys used for dimensional joins.

ALTER TABLE analytics.fact_credits
ADD CONSTRAINT fact_credits_pkey
PRIMARY KEY (credit_sk);

-- Confirm that credit_sk is now the primary key of fact_credits.

SELECT
    tc.table_name,
    kcu.column_name,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'analytics'
  AND tc.table_name = 'fact_credits'
  AND tc.constraint_type = 'PRIMARY KEY';

  -- ============================================================================
-- STAGE 19: CHECK fact_credits FOR DUPLICATE BUSINESS RECORDS
-- ============================================================================
-- credit_sk makes every physical row unique, but two different credit_sk values
-- could still represent the same title-person-role-character relationship.
--
-- This query checks the actual business grain of the fact table before a
-- uniqueness rule is added.
--
-- COALESCE converts a NULL character into a temporary comparison value so that
-- repeated director records with NULL characters can also be detected.

SELECT
    title_sk,
    person_sk,
    role,
    COALESCE(character, '<NO_CHARACTER>') AS character_value,
    COUNT(*) AS duplicate_count
FROM analytics.fact_credits
GROUP BY
    title_sk,
    person_sk,
    role,
    COALESCE(character, '<NO_CHARACTER>')
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- ============================================================================
-- STAGE 20: ENFORCE BUSINESS UNIQUENESS IN fact_credits
-- ============================================================================
-- credit_sk is the technical primary key, but it does not stop two different
-- rows from representing the same credit relationship.
--
-- This unique index prevents duplicate combinations of:
--   title_sk
--   person_sk
--   role
--   character
--
-- COALESCE converts NULL character values into a comparison value so repeated
-- director credits with NULL characters are treated as duplicates.

CREATE UNIQUE INDEX ux_fact_credits_business_grain
ON analytics.fact_credits (
    title_sk,
    person_sk,
    role,
    COALESCE(character, '<NO_CHARACTER>')
);

-- Confirm that the unique business index was created successfully.

SELECT
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'analytics'
  AND tablename = 'fact_credits'
  AND indexname = 'ux_fact_credits_business_grain';


-- ============================================================================
-- STAGE 21: VERIFY THE GRAIN OF fact_title_metrics
-- ============================================================================
-- fact_title_metrics stores one metrics record for each title.
--
-- Before enforcing title_sk as the primary key, confirm that each title_sk
-- appears exactly once in the table.
--
-- A valid result returns no rows.

SELECT
    title_sk,
    COUNT(*) AS record_count
FROM analytics.fact_title_metrics
GROUP BY title_sk
HAVING COUNT(*) > 1

ORDER BY record_count DESC;


-- ============================================================================
-- STAGE 22: MAKE title_sk THE PRIMARY KEY OF fact_title_metrics
-- ============================================================================
-- fact_title_metrics has a grain of one row per title.
--
-- Since each title_sk appears only once, title_sk can safely serve as:
--   1. the primary key of fact_title_metrics
--   2. the foreign key linking the fact table to dim_title
--
-- This creates a one-to-one relationship between dim_title and
-- fact_title_metrics and prevents duplicate metric rows for the same title.

ALTER TABLE analytics.fact_title_metrics
ADD CONSTRAINT fact_title_metrics_pkey
PRIMARY KEY (title_sk);

-- Confirm that title_sk is now the primary key of fact_title_metrics.

SELECT
    tc.table_name,
    kcu.column_name,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'analytics'
  AND tc.table_name = 'fact_title_metrics'
  AND tc.constraint_type = 'PRIMARY KEY';


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

 
-- ============================================================================
-- STAGE 28: VALIDATE TEXT LENGTHS IN ANALYTICS TABLES
-- ============================================================================
-- Before converting analytics TEXT columns to bounded VARCHAR types, inspect
-- the maximum observed length of each candidate column.
--
-- character is included only for verification. Based on the OLTP findings, it
-- is expected to remain TEXT because it is free-form and may exceed 255
-- characters.

SELECT
    MAX(LENGTH(title_name)) AS max_title_name_length,
    MAX(LENGTH(type)) AS max_type_length,
    MAX(LENGTH(age_certification)) AS max_age_certification_length
FROM analytics.dim_title;

SELECT
    MAX(LENGTH(name)) AS max_person_name_length
FROM analytics.dim_person;

SELECT
    MAX(LENGTH(name)) AS max_genre_name_length
FROM analytics.dim_genre;

SELECT
    MAX(LENGTH(role)) AS max_role_length,
    MAX(LENGTH(character)) AS max_character_length
FROM analytics.fact_credits;


-- ============================================================================
-- STAGE 29: STANDARDISE ANALYTICS TABLE DATA TYPES
-- ============================================================================
-- Existing values were checked before applying bounded VARCHAR types.
--
-- dim_title:
--   title_name has a maximum observed length of 104 characters, so
--   VARCHAR(255) provides sufficient capacity.
--
--   type has a maximum observed length of 5 characters, so VARCHAR(10)
--   supports the current MOVIE and SHOW values while allowing reasonable room.
--
--   age_certification has a maximum observed length of 5 characters, so
--   VARCHAR(10) is sufficient for the current certification values.
--
-- dim_person:
--   name has a maximum observed length of 73 characters, so VARCHAR(255)
--   provides sufficient capacity for existing and future person names.
--
-- dim_genre:
--   name has a maximum observed length of 13 characters, so VARCHAR(100)
--   comfortably supports the existing genre descriptions.
--
-- fact_credits:
--   role has a maximum observed length of 8 characters, so VARCHAR(20)
--   is sufficient for controlled values such as ACTOR and DIRECTOR.
--
--   character has a maximum observed length of 298 characters and remains
--   TEXT because it is free-form data without a reliable business-defined
--   maximum length.

ALTER TABLE analytics.dim_title
ALTER COLUMN title_name TYPE VARCHAR(255),
ALTER COLUMN type TYPE VARCHAR(10),
ALTER COLUMN age_certification TYPE VARCHAR(10);

ALTER TABLE analytics.dim_person
ALTER COLUMN name TYPE VARCHAR(255);

ALTER TABLE analytics.dim_genre
ALTER COLUMN name TYPE VARCHAR(100);

ALTER TABLE analytics.fact_credits
ALTER COLUMN role TYPE VARCHAR(20);

-- Confirm that the selected analytics columns now use bounded VARCHAR types
-- while fact_credits.character remains TEXT.

SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name IN (
      'dim_title',
      'dim_person',
      'dim_genre',
      'fact_credits'
  )
  AND column_name IN (
      'title_name',
      'type',
      'age_certification',
      'name',
      'role',
      'character'
  )
ORDER BY table_name, ordinal_position;


-- Verify that changing descriptive column types did not affect the foreign-key
-- relationships between analytics fact, bridge and dimension tables.

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
WHERE tc.table_schema = 'analytics'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name, kcu.column_name;

-- ============================================================================
-- STAGE 30: INSPECT dim_title.title_id DATA TYPE
-- ============================================================================
-- dim_title.title_id stores the original Netflix title identifier as the
-- business key, while title_sk remains the warehouse surrogate primary key.
--
-- Before altering the column, inspect its current datatype and length setting.

SELECT
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'dim_title'
  AND column_name = 'title_id';

  -- Confirm the maximum observed length of dim_title.title_id before applying
-- a VARCHAR limit.

SELECT
    MAX(LENGTH(title_id)) AS max_title_id_length
FROM analytics.dim_title;

-- ============================================================================
-- STAGE 31: FINAL OLAP SCHEMA VALIDATION
-- ============================================================================
-- The structural changes are complete. This stage performs a final audit of
-- the analytics schema to verify:
--   1. all expected fact, dimension and bridge tables exist
--   2. primary keys are correctly defined
--   3. foreign-key relationships remain intact
--   4. surrogate and business key columns use the intended data types
--   5. table populations remain unchanged
--   6. treated metric columns contain no remaining NULL values

-- List all fact, dimension and bridge tables in the analytics schema.

SELECT
    table_name
FROM information_schema.tables
WHERE table_schema = 'analytics'
  AND table_type = 'BASE TABLE'
  AND (
      table_name LIKE 'dim_%'
      OR table_name LIKE 'fact_%'
      OR table_name LIKE 'bridge_%'
  )
ORDER BY table_name;

-- Display every primary-key column currently defined in the analytics schema.

SELECT
    tc.table_name,
    kcu.column_name,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'analytics'
  AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY tc.table_name, kcu.ordinal_position;

-- Review every analytics table column, including data type, maximum length,
-- nullability and identity status.


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
WHERE tc.table_schema = 'analytics'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name, kcu.column_name;


-- Review every analytics table column, including data type, maximum length,
-- nullability and identity status.

SELECT
    table_name,
    ordinal_position,
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    is_identity
FROM information_schema.columns
WHERE table_schema = 'analytics'
ORDER BY table_name, ordinal_position;

-- Confirm that schema modifications did not remove or duplicate records.

SELECT 'dim_title' AS table_name, COUNT(*) AS row_count
FROM analytics.dim_title

UNION ALL

SELECT 'dim_person', COUNT(*)
FROM analytics.dim_person

UNION ALL

SELECT 'dim_genre', COUNT(*)
FROM analytics.dim_genre

UNION ALL

SELECT 'bridge_title_genre', COUNT(*)
FROM analytics.bridge_title_genre

UNION ALL

SELECT 'fact_credits', COUNT(*)
FROM analytics.fact_credits

UNION ALL

SELECT 'fact_title_metrics', COUNT(*)
FROM analytics.fact_title_metrics

ORDER BY table_name;

-- Confirm that no NULL values remain in the four treated metric columns.

SELECT
    COUNT(*) FILTER (WHERE imdb_score IS NULL) AS null_imdb_score,
    COUNT(*) FILTER (WHERE imdb_votes IS NULL) AS null_imdb_votes,
    COUNT(*) FILTER (WHERE tmdb_popularity IS NULL) AS null_tmdb_popularity,
    COUNT(*) FILTER (WHERE tmdb_score IS NULL) AS null_tmdb_score
FROM analytics.fact_title_metrics;

-- Confirm that the imputation flags preserve the number of values that were
-- originally missing before median replacement.

SELECT
    COUNT(*) FILTER (WHERE imdb_score_imputed) AS imputed_imdb_scores,
    COUNT(*) FILTER (WHERE imdb_votes_imputed) AS imputed_imdb_votes,
    COUNT(*) FILTER (WHERE tmdb_popularity_imputed) AS imputed_tmdb_popularity,
    COUNT(*) FILTER (WHERE tmdb_score_imputed) AS imputed_tmdb_scores
FROM analytics.fact_title_metrics;

-- ============================================================================
-- STAGE 32: ENFORCE THE BUSINESS GRAIN OF bridge_title_genre
-- ============================================================================
-- bridge_title_genre resolves the many-to-many relationship between
-- dim_title and dim_genre within the analytics schema.
--
-- Each (title_sk, genre_sk) pair should appear exactly once.
-- Before defining the composite primary key, verify that no duplicate
-- relationships currently exist.


SELECT
    title_sk,
    genre_sk,
    COUNT(*) AS relationship_count
FROM analytics.bridge_title_genre
GROUP BY
    title_sk,
    genre_sk
HAVING COUNT(*) > 1
ORDER BY relationship_count DESC;

-- ============================================================================
-- DEFINE THE COMPOSITE PRIMARY KEY
-- ============================================================================
-- The business grain of bridge_title_genre is the combination of
-- title_sk and genre_sk.
--
-- Neither column alone uniquely identifies a row, but together they
-- uniquely identify one title-to-genre relationship.
--
-- Defining the composite primary key:
--   • prevents duplicate title-genre relationships
--   • automatically enforces NOT NULL on both columns
--   • creates a unique index for efficient joins

ALTER TABLE analytics.bridge_title_genre
ADD CONSTRAINT bridge_title_genre_pkey
PRIMARY KEY (title_sk, genre_sk);

-- ============================================================================
-- VERIFY THE COMPOSITE PRIMARY KEY
-- ============================================================================
-- Confirm that bridge_title_genre now uses the composite primary key
-- consisting of title_sk and genre_sk.

SELECT
    tc.table_name,
    kcu.column_name,
    kcu.ordinal_position,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'analytics'
  AND tc.table_name = 'bridge_title_genre'
  AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY kcu.ordinal_position;