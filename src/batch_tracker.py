"""
Batch tracking utilities for the Netflix incremental pipeline.

This module handles batch registration, duplicate detection
and processing-state changes in etl.batch_history.
"""

# Import the shared database connection function.
from src.database import get_etl_connection


# Check whether a successfully processed file has already been seen.
def find_successful_batch_by_checksum(file_checksum):                       #  Return an existing successful batch when the checksum already exists.
  

    with get_etl_connection() as connection:

        with connection.cursor() as cursor:

           
            cursor.execute(                                  # Search only successful batches with the same checksum.
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


def register_batches(batches):                          # Register multiple source batches in one transaction
    query = """
        INSERT INTO etl.batch_history (
            pipeline_name,
            file_name,
            file_checksum,
            rows_received,
            status
        )
        VALUES (%s, %s, %s, %s, 'RECEIVED')
        RETURNING batch_id;
    """

    batch_ids = []

    with get_etl_connection() as conn, conn.cursor() as cursor:
        for batch in batches:
            cursor.execute(
                query,
                (
                    batch["pipeline_name"],
                    batch["file_name"],
                    batch["file_checksum"],
                    batch["rows_received"],
                ),
            )

            batch_ids.append(cursor.fetchone()[0])

        conn.commit()

    return batch_ids



                                                                # Mark a registered batch as actively processing.
def mark_batch_running(batch_id):                               # Batch tracking utilities for the Netflix ETL pipeline
    """Mark a registered batch as actively processing."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

         
            cursor.execute(                                    # Record when processing begins and clear any old error.
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
                    "batch_id": batch_id
                },
            )

        connection.commit()


def mark_batch_success(batch_id,rows_processed=None):       # Mark a batch as successfully ingested.rows_processed records
                                                             # how many source rows were successfully written into dev.
     
    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

                                                            # Complete the ingestion lifecycle and retain the existing
                                                            # row count when no replacement value is supplied.
            cursor.execute(
                """
                UPDATE etl.batch_history

                SET
                    status = 'SUCCESS',
                    rows_processed = COALESCE(
                        %(rows_processed)s,
                        rows_processed
                    ),
                    processing_completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL

                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id,
                    "rows_processed": rows_processed
                },
            )

        connection.commit()
    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(                                 # Complete the batch lifecycle only after all pipeline stages pass.
                """
                UPDATE etl.batch_history

                SET
                    status = 'SUCCESS',
                    processing_completed_at = CURRENT_TIMESTAMP,
                    error_message = NULL

                WHERE batch_id = %(batch_id)s;
                """,
                {
                    "batch_id": batch_id
                },
            )

        connection.commit()


def mark_batch_failed(batch_id, error_message):             # Record a failed batch and preserve the failure reason

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            
            cursor.execute(                             # Persist the failure so the batch can be diagnosed later.
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
                    "error_message": str(error_message)
                },
            )

        connection.commit()

def get_batch_metadata(batch_id):
    """Return operational metadata for one registered source batch."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:
            
            cursor.execute(                                     # Retrieve the batch fields required by pipeline control.
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
                    "batch_id": batch_id
                },
            )

            return cursor.fetchone()

def validate_batch(batch_id):                               # Validate that a registered batch exists and contains source rows

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


def recover_stale_batches(pipeline_name, stale_after_minutes=60):
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
                    "stale_after_minutes": stale_after_minutes
                },
            )

            recovered_batches = cursor.fetchall()

        # Persist the recovery changes.
        connection.commit()

    return recovered_batches