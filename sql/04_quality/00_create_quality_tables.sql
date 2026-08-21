-- ============================================================================
-- Data-quality audit table.
-- Stores the result of every production quality check.
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl.quality_results (
    quality_result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    pipeline_name VARCHAR(100) NOT NULL,

    batch_id BIGINT,

    layer VARCHAR(30) NOT NULL,

    check_name VARCHAR(150) NOT NULL,

    status VARCHAR(20) NOT NULL
        CHECK (status IN ('PASS', 'FAIL')),

    observed_value BIGINT,

    expected_value BIGINT,

    details TEXT,

    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);