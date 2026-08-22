"""Pipeline-level control and checkpoint management."""

from src.database import get_etl_connection


def mark_pipeline_running(pipeline_name):
    """Mark the pipeline as actively processing."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Update operational state without advancing any checkpoint.
            cursor.execute(
                """
                UPDATE etl.pipeline_control
                SET
                    status = 'RUNNING',
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %(pipeline_name)s;
                """,
                {
                    "pipeline_name": pipeline_name,
                },
            )

        connection.commit()


def mark_pipeline_success(
    pipeline_name,
    watermark,
    file_name,
    rows_processed,
):
    """Advance the pipeline checkpoint after a successful run."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Advance the checkpoint only after all pipeline stages pass.
            cursor.execute(
                """
                UPDATE etl.pipeline_control
                SET
                    last_successful_load = CURRENT_TIMESTAMP,
                    last_watermark = %(watermark)s,
                    last_file_name = %(file_name)s,
                    rows_processed = %(rows_processed)s,
                    status = 'SUCCESS',
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %(pipeline_name)s;
                """,
                {
                    "pipeline_name": pipeline_name,
                    "watermark": watermark,
                    "file_name": file_name,
                    "rows_processed": rows_processed,
                },
            )

        connection.commit()


def mark_pipeline_failed(pipeline_name):
    """
    Mark the pipeline as failed without changing its successful checkpoint.

    The previous watermark remains available for the next retry.
    """

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                UPDATE etl.pipeline_control
                SET
                    status = 'FAILED',
                    updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %(pipeline_name)s;
                """,
                {
                    "pipeline_name": pipeline_name,
                },
            )

        connection.commit()


def get_pipeline_control(pipeline_name):
    """Return the current pipeline checkpoint and execution state."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT
                    pipeline_name,
                    last_successful_load,
                    last_watermark,
                    last_file_name,
                    rows_processed,
                    status,
                    updated_at
                FROM etl.pipeline_control
                WHERE pipeline_name = %(pipeline_name)s;
                """,
                {
                    "pipeline_name": pipeline_name,
                },
            )

            return cursor.fetchone()