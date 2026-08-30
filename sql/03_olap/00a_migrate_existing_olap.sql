-- ============================================================================
-- Existing OLAP compatibility migration
-- Adds constraints required for incremental and idempotent warehouse loads.
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Dimension business-key uniqueness
-- ---------------------------------------------------------------------------

ALTER TABLE uat_olap.dim_title
ADD CONSTRAINT uq_dim_title_title_id
UNIQUE (title_id);


ALTER TABLE uat_olap.dim_person
ADD CONSTRAINT uq_dim_person_person_id
UNIQUE (person_id);


ALTER TABLE uat_olap.dim_genre
ADD CONSTRAINT uq_dim_genre_genre_id
UNIQUE (genre_id);


ALTER TABLE uat_olap.dim_country
ADD CONSTRAINT uq_dim_country_country_id
UNIQUE (country_id);


-- ---------------------------------------------------------------------------
-- Bridge uniqueness
-- Prevent duplicate dimensional relationships during pipeline retries.
-- ---------------------------------------------------------------------------

ALTER TABLE uat_olap.bridge_title_genre
ADD CONSTRAINT uq_bridge_title_genre
UNIQUE (title_sk, genre_sk);


ALTER TABLE uat_olap.bridge_title_country
ADD CONSTRAINT uq_bridge_title_country
UNIQUE (title_sk, country_sk);


-- ---------------------------------------------------------------------------
-- Title metrics grain
-- One metrics row should exist per title.
-- ---------------------------------------------------------------------------

ALTER TABLE uat_olap.fact_title_metrics
ADD CONSTRAINT uq_fact_title_metrics_title_sk
UNIQUE (title_sk);

-- Surrogate-key generation for dim_title.

CREATE SEQUENCE IF NOT EXISTS uat_olap.dim_title_sk_seq;

SELECT setval(
    'uat_olap.dim_title_sk_seq',
    COALESCE(
        (SELECT MAX(title_sk) FROM uat_olap.dim_title),
        0
    ) + 1,
    false
);

ALTER TABLE uat_olap.dim_title
ALTER COLUMN title_sk
SET DEFAULT nextval('uat_olap.dim_title_sk_seq');

ALTER SEQUENCE uat_olap.dim_title_sk_seq
OWNED BY uat_olap.dim_title.title_sk;

-- ============================================================================
-- Surrogate-key generation for remaining dimensions.
-- ============================================================================

-- Person dimension.
CREATE SEQUENCE IF NOT EXISTS uat_olap.dim_person_sk_seq;

SELECT setval(
    'uat_olap.dim_person_sk_seq',
    COALESCE(
        (SELECT MAX(person_sk) FROM uat_olap.dim_person),
        0
    ) + 1,
    false
);

ALTER TABLE uat_olap.dim_person
ALTER COLUMN person_sk
SET DEFAULT nextval('uat_olap.dim_person_sk_seq');

ALTER SEQUENCE uat_olap.dim_person_sk_seq
OWNED BY uat_olap.dim_person.person_sk;


-- Genre dimension.
CREATE SEQUENCE IF NOT EXISTS uat_olap.dim_genre_sk_seq;

SELECT setval(
    'uat_olap.dim_genre_sk_seq',
    COALESCE(
        (SELECT MAX(genre_sk) FROM uat_olap.dim_genre),
        0
    ) + 1,
    false
);

ALTER TABLE uat_olap.dim_genre
ALTER COLUMN genre_sk
SET DEFAULT nextval('uat_olap.dim_genre_sk_seq');

ALTER SEQUENCE uat_olap.dim_genre_sk_seq
OWNED BY uat_olap.dim_genre.genre_sk;


