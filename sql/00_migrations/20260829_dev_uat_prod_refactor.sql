BEGIN;

-- Raw ingestion layer
ALTER SCHEMA staging RENAME TO dev;

-- Transformation/testing layer
CREATE SCHEMA IF NOT EXISTS uat_oltp;

ALTER TABLE IF EXISTS public.title SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.person SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.genre SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.country SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.credit SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.title_genre SET SCHEMA uat_oltp;
ALTER TABLE IF EXISTS public.title_country SET SCHEMA uat_oltp;

-- Legacy raw tables belong in DEV
ALTER TABLE IF EXISTS public.raw_title SET SCHEMA dev;
ALTER TABLE IF EXISTS public.raw_credit SET SCHEMA dev;

ALTER SCHEMA analytics RENAME TO uat_olap;

-- Consumer-ready layer
ALTER SCHEMA serving RENAME TO production;

COMMIT;