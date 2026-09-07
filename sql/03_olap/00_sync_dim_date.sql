-- ============================================================================
-- NETFLIX DATA ENGINEERING
-- DATE DIMENSION SYNC
-- ============================================================================
-- Purpose:
-- Populate the date dimension from actual title ingestion dates.

INSERT INTO uat_olap.dim_date (
    date_sk,
    full_date,
    day,
    day_name,
    day_of_week,
    week,
    month,
    month_name,
    quarter,
    year,
    is_weekend
)
SELECT DISTINCT
    TO_CHAR(ingested_at::DATE, 'YYYYMMDD')::INTEGER AS date_sk,
    ingested_at::DATE AS full_date,
    EXTRACT(DAY FROM ingested_at)::INTEGER AS day,
    TO_CHAR(ingested_at, 'FMDay') AS day_name,
    EXTRACT(ISODOW FROM ingested_at)::INTEGER AS day_of_week,
    EXTRACT(WEEK FROM ingested_at)::INTEGER AS week,
    EXTRACT(MONTH FROM ingested_at)::INTEGER AS month,
    TO_CHAR(ingested_at, 'FMMonth') AS month_name,
    EXTRACT(QUARTER FROM ingested_at)::INTEGER AS quarter,
    EXTRACT(YEAR FROM ingested_at)::INTEGER AS year,
    EXTRACT(ISODOW FROM ingested_at) IN (6, 7) AS is_weekend
FROM uat_oltp.title
WHERE ingested_at IS NOT NULL

ON CONFLICT (date_sk)
DO NOTHING;