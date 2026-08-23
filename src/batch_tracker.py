"""
Batch tracking utilities for the Netflix incremental pipeline.

This module handles batch registration, duplicate detection
and processing-state changes in etl.batch_history.
"""

# Import the shared database connection function.
from src.database import get_etl_connection


# Check whether a successfully processed file has already been seen.
def find_successful_batch_by_checksum(file_checksum):
    """
    Return an existing successful batch when the checksum already exists.

    Returns None when the file has not previously completed successfully.
    """

    # Open the ETL database connection.
    with get_etl_connection() as connection:

        # Open a cursor for the duplicate lookup.
        with connection.cursor() as cursor:

            # Search only successful batches with the same checksum.
            cursor.execute(
                """
                SELECT
                    batch_id,
                    pipeline_name,
                    file_name,
                    file_checksum,
                    status
                FROM etl.batch_history
                WHERE file_checksum = %s
                  AND status = 'SUCCESS'
                LIMIT 1;
                """,
                (file_checksum,),
            )

            # Return the match or None.
            return cursor.fetchone()


# Register a newly discovered incoming batch.
def register_batch(
    pipeline_name,
    file_name,
    file_checksum,
    rows_received,
):
    """
    Register a new batch and return its generated batch ID.
    """

    # Open the ETL database connection.
    with get_etl_connection() as connection:

        # Open a cursor for the insert.
        with connection.cursor() as cursor:

            # Insert the incoming batch before processing begins.
            cursor.execute(
                """
                INSERT INTO etl.batch_history (
                    pipeline_name,
                    file_name,
                    file_checksum,
                    rows_received,
                    status
                )
                VALUES (%s, %s, %s, %s, 'RECEIVED')
                RETURNING batch_id;
                """,
                (
                    pipeline_name,
                    file_name,
                    file_checksum,
                    rows_received,
                ),
            )

            # Retrieve PostgreSQL's generated batch identifier.
            batch_id = cursor.fetchone()[0]

        # Commit the new batch registration.
        connection.commit()

    # Return the identifier so downstream tasks know which batch they own.
    return batch_id

# Mark a registered batch as actively processing.

"""Batch tracking utilities for the Netflix ETL pipeline."""

def mark_batch_running(batch_id):
    """Mark a registered batch as actively processing."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Record when processing begins and clear any old error.
            cursor.execute(
                """
                UPDATE etl.batch_history

                SET
                    status = 'RUNNING',
                    processing_started_at = COALESCE(
                        processing_started_at,
                        CURRENT_TIMESTAMP
                    ),
                    processing_completed_at = NULL,
                    error_message = NULL

                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id,
                },
            )

        connection.commit()


def mark_batch_success(batch_id):
    """Mark a batch as successfully processed."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Complete the batch lifecycle only after all pipeline stages pass.
            cursor.execute(
                """
                UPDATE etl.batch_history

                SET
                    status = 'SUCCESS',
                    processing_completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL

                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id,
                },
            )

        connection.commit()


def mark_batch_failed(batch_id, error_message):
    """Record a failed batch and preserve the failure reason."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Persist the failure so the batch can be diagnosed later.
            cursor.execute(
                """
                UPDATE etl.batch_history

                SET
                    status = 'FAILED',
                    processing_completed_at = CURRENT_TIMESTAMP,
                    error_message = %(error_message)s

                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id,
                    "error_message": str(error_message),
                },
            )

        connection.commit()

def get_batch_metadata(batch_id):
    """Return operational metadata for one registered source batch."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Retrieve the batch fields required by pipeline control.
            cursor.execute(
                """
                SELECT
                    batch_id,
                    file_name,
                    received_at,
                    rows_received,
                    rows_processed,
                    status
                FROM etl.batch_history
                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id,
                },
            )

            return cursor.fetchone()

def validate_batch(batch_id):
    """Validate that a registered batch exists and contains source rows."""

    batch_metadata = get_batch_metadata(
        batch_id
    )

    # Reject batch IDs that are not registered.
    if batch_metadata is None:
        raise ValueError(
            f"Batch {batch_id} does not exist."
        )

    rows_received = batch_metadata[3]

    # Reject empty source batches before transformations begin.
    if rows_received is None or rows_received <= 0:
        raise ValueError(
            f"Batch {batch_id} contains no source rows."
        )

    return batch_metadata


def recover_stale_batches(
    pipeline_name,
    stale_after_minutes=60,
):
    """
    Mark abandoned RUNNING batches as FAILED.

    A batch is considered stale when it has remained in RUNNING state
    longer than the configured threshold without a completion timestamp.

    Active or recently started batches are left unchanged.
    """

    # Validate the threshold before using it in the database query.
    if stale_after_minutes <= 0:
        raise ValueError(
            "stale_after_minutes must be greater than zero."
        )

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Identify and fail batches that exceeded the allowed
            # RUNNING duration without completing.
            cursor.execute(
                """
                UPDATE etl.batch_history

                SET
                    status = 'FAILED',
                    processing_completed_at = CURRENT_TIMESTAMP,
                    error_message = (
                        'Batch automatically marked FAILED because it '
                        'remained RUNNING beyond the stale threshold.'
                    )

                WHERE pipeline_name = %(pipeline_name)s
                  AND status = 'RUNNING'
                  AND processing_completed_at IS NULL
                  AND processing_started_at
                      < CURRENT_TIMESTAMP
                        - (%(stale_after_minutes)s * INTERVAL '1 minute')

                RETURNING
                    batch_id,
                    file_name,
                    processing_started_at;
                """,
                {
                    "pipeline_name": pipeline_name,
                    "stale_after_minutes": stale_after_minutes,
                },
            )

            recovered_batches = cursor.fetchall()

        # Persist the recovery changes.
        connection.commit()

    return recovered_batches