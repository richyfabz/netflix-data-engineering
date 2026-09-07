
-- Enhancement A: ingestion timestamp propagation + dim_date
BEGIN;


-- 1. Propagate the DEV ingestion timestamp into UAT_OLTP

ALTER TABLE uat_oltp.title
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;

ALTER TABLE uat_oltp.credit
ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;


-- 2. Create the fifth analytical dimension

CREATE TABLE IF NOT EXISTS uat_olap.dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_week INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);


-- 3. Add ingestion date key to the title dimension

ALTER TABLE uat_olap.dim_title
ADD COLUMN IF NOT EXISTS ingestion_date_sk INTEGER;



-- 4. Add foreign-key relationship to the date dimension

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_dim_title_ingestion_date'
          AND conrelid = 'uat_olap.dim_title'::regclass
    ) THEN
        ALTER TABLE uat_olap.dim_title
        ADD CONSTRAINT fk_dim_title_ingestion_date
        FOREIGN KEY (ingestion_date_sk)
        REFERENCES uat_olap.dim_date(date_sk);
    END IF;
END $$;

COMMIT;