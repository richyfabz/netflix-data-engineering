
-- Create the serving schema.
-- This schema exposes business-ready datasets for reporting tools such as
-- Power BI without requiring those tools to query the internal OLAP model
-- directly.

CREATE SCHEMA IF NOT EXISTS serving;