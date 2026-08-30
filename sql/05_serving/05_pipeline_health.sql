-- Pipeline health serving view.
--
-- Provides a business-friendly operational summary of the production ETL
-- pipeline for monitoring and Power BI reporting.
--
-- Questions supported:
--   - Is the pipeline currently healthy?
--   - When did it last complete successfully?
--   - What watermark has been processed?
--   - How many batches succeeded or failed?
--   - Are there recent quality-check failures?

CREATE OR REPLACE VIEW production.pipeline_health AS
WITH batch_summary AS (
    SELECT
        pipeline_name,

        COUNT(*) AS total_batches,

        COUNT(*) FILTER (
            WHERE status = 'SUCCESS'
        ) AS successful_batches,

        COUNT(*) FILTER (
            WHERE status = 'FAILED'
        ) AS failed_batches,

        COUNT(*) FILTER (
            WHERE status = 'RUNNING'
        ) AS running_batches,

        SUM(
            COALESCE(rows_received, 0)
        ) AS total_rows_received,

        SUM(
            COALESCE(rows_processed, 0)
        ) AS total_rows_processed,

        MAX(received_at) AS latest_batch_received_at,

        MAX(processing_completed_at) FILTER (
            WHERE status = 'SUCCESS'
        ) AS latest_batch_success_at

    FROM etl.batch_history

    GROUP BY pipeline_name
),

quality_summary AS (

    SELECT
        pipeline_name,

        COUNT(*) AS total_quality_checks,

        COUNT(*) FILTER (
            WHERE status = 'PASS'
        ) AS passed_quality_checks,

        COUNT(*) FILTER (
            WHERE status = 'FAIL'
        ) AS failed_quality_checks,

        MAX(checked_at) AS latest_quality_check_at

    FROM etl.quality_results

    GROUP BY pipeline_name
)
SELECT
    pc.pipeline_name,
    pc.status AS pipeline_status,
    pc.last_successful_load,
    pc.last_watermark,
    pc.last_file_name,
    pc.rows_processed AS checkpoint_rows_processed,
    pc.updated_at AS pipeline_updated_at,
    COALESCE(
        bs.total_batches,
        0
    ) AS total_batches,

    COALESCE(
        bs.successful_batches,
        0
    ) AS successful_batches,

    COALESCE(
        bs.failed_batches,
        0
    ) AS failed_batches,

    COALESCE(
        bs.running_batches,
        0
    ) AS running_batches,

    COALESCE(
        bs.total_rows_received,
        0
    ) AS total_rows_received,

    COALESCE(
        bs.total_rows_processed,
        0
    ) AS total_rows_processed,

    bs.latest_batch_received_at,
    bs.latest_batch_success_at,
    COALESCE(
        qs.total_quality_checks,
        0
    ) AS total_quality_checks,

    COALESCE(
        qs.passed_quality_checks,
        0
    ) AS passed_quality_checks,

    COALESCE(
        qs.failed_quality_checks,
        0
    ) AS failed_quality_checks,

    qs.latest_quality_check_at

FROM etl.pipeline_control AS pc

LEFT JOIN batch_summary AS bs
    ON bs.pipeline_name = pc.pipeline_name

LEFT JOIN quality_summary AS qs
    ON qs.pipeline_name = pc.pipeline_name;

   
   