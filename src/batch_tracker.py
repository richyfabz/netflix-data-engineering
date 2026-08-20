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
def mark_batch_running(batch_id):
    """
    Change a batch from RECEIVED to RUNNING.
    """

    # Connect to PostgreSQL using the ETL account.
    with get_etl_connection() as connection:

        # Open a cursor for the state update.
        with connection.cursor() as cursor:

            # Record that processing has started.
            cursor.execute(
                """
                UPDATE etl.batch_history
                SET
                    status = 'RUNNING',
                    processing_started_at = CURRENT_TIMESTAMP
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

        # Save the state change.
        connection.commit()