"""
Watermark utilities for the Netflix incremental pipeline.

This module reads and updates the pipeline watermark used to control
incremental extraction windows.
"""

# Import timedelta so we can calculate the lookback window.
from datetime import timedelta


# Import the shared PostgreSQL connection helper.
from src.database import get_etl_connection


# Read the latest successful watermark for a pipeline.
def get_last_watermark(pipeline_name):
    """
    Return the most recent successful source watermark.

    Returns None when the pipeline has never completed successfully.
    """

    # Open a PostgreSQL connection using the ETL account.
    with get_etl_connection() as connection:

        # Open a cursor for the watermark lookup.
        with connection.cursor() as cursor:

            # Retrieve the current watermark for this pipeline.
            cursor.execute(
                """
                SELECT last_watermark
                FROM etl.pipeline_control
                WHERE pipeline_name = %s;
                """,
                (pipeline_name,),
            )

            # Fetch the matching row.
            result = cursor.fetchone()

    # Return None if the pipeline does not yet exist.
    if result is None:
        return None

    # Return the watermark stored in the first column.
    return result[0]


# Calculate where the next incremental extraction should begin.
def get_extraction_start(
    pipeline_name,
    lookback_minutes=10,
):
    """
    Return the timestamp from which the next incremental load should read.

    The extraction window intentionally begins before the saved watermark
    so late-arriving records can still be recovered.
    """

    # Read the last successful watermark from PostgreSQL.
    last_watermark = get_last_watermark(pipeline_name)

    # If no watermark exists, this is the first pipeline run.
    if last_watermark is None:
        return None

    # Move backwards by the configured lookback interval.
    extraction_start = last_watermark - timedelta(
        minutes=lookback_minutes
    )

    # Return the calculated extraction boundary.
    return extraction_start


# Advance the watermark after a successful batch.
def update_watermark(
    pipeline_name,
    new_watermark,
    file_name,
    rows_processed,
):
    """
    Advance the successful pipeline checkpoint.

    This function should only be called after the batch and quality
    checks have completed successfully.
    """

    # Open the ETL database connection.
    with get_etl_connection() as connection:

        # Open a cursor for the pipeline-control update.
        with connection.cursor() as cursor:

            # Store the latest successful source position.
            cursor.execute(
                """
                UPDATE etl.pipeline_control
                SET
                    last_successful_load = CURRENT_TIMESTAMP,
                    last_watermark = %s,
                    last_file_name = %s,
                    rows_processed = %s,
                    status = 'SUCCESS',
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %s;
                """,
                (
                    new_watermark,
                    file_name,
                    rows_processed,
                    pipeline_name,
                ),
            )

        # Commit the successful checkpoint update.
        connection.commit()