"""
Automatic source-batch ingestion for the Netflix pipeline.

This module detects a new titles.csv + credits.csv pair, validates the files,
prevents duplicate ingestion using checksums, registers ETL batches, and loads
the source records into PostgreSQL staging.
"""

from pathlib import Path
import shutil

from src.batch_tracker import (
    find_successful_batch_by_checksum,
    register_batch,
    mark_batch_running,
    mark_batch_success,
    mark_batch_failed,
)

from src.file_utils import calculate_file_checksum

from src.ingestion import (
    TITLE_COLUMNS,
    CREDIT_COLUMNS,
    read_source_csv,
    load_titles_to_staging,
    load_credits_to_staging,
)


PIPELINE_NAME = "netflix_incremental_pipeline"

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INCOMING_DIR = (
    PROJECT_ROOT
    / "data"
    / "incoming"
)

ARCHIVE_DIR = (
    PROJECT_ROOT
    / "data"
    / "archive"
)


def ingest_incoming_batch():
    """
    Ingest one titles.csv + credits.csv source pair.

    Returns the exact batch IDs created by this ingestion run.

    Returns None when there is no new source batch to process.
    """

    titles_path = INCOMING_DIR / "titles.csv"
    credits_path = INCOMING_DIR / "credits.csv"

    # ---------------------------------------------------------------------
    # Validate file arrival
    # ---------------------------------------------------------------------

    titles_exists = titles_path.exists()
    credits_exists = credits_path.exists()

    # An empty incoming directory is normal for scheduled Airflow checks.
    if not titles_exists and not credits_exists:
        return None

    # Never process a partial source pair.
    if not titles_exists or not credits_exists:
        raise FileNotFoundError(
            "A complete source batch requires both "
            "titles.csv and credits.csv."
        )

    # ---------------------------------------------------------------------
    # Validate source structure before registering batches
    # ---------------------------------------------------------------------

    title_rows = read_source_csv(
        titles_path,
        TITLE_COLUMNS,
    )

    credit_rows = read_source_csv(
        credits_path,
        CREDIT_COLUMNS,
    )

    if not title_rows:
        raise ValueError(
            "titles.csv contains no source records."
        )

    if not credit_rows:
        raise ValueError(
            "credits.csv contains no source records."
        )

    # ---------------------------------------------------------------------
    # Calculate source fingerprints
    # ---------------------------------------------------------------------

    titles_checksum = calculate_file_checksum(
        titles_path
    )

    credits_checksum = calculate_file_checksum(
        credits_path
    )

    # Check whether either source file was already processed successfully.
    existing_titles = find_successful_batch_by_checksum(
        titles_checksum
    )

    existing_credits = find_successful_batch_by_checksum(
        credits_checksum
    )

    # If both files were previously processed, there is no new work.
    if existing_titles and existing_credits:
        return None

    # Reject mixed batches where only one file is new.
    #
    # This protects titles and credits from being paired with source files
    # from different ingestion cycles.
    if bool(existing_titles) != bool(existing_credits):
        raise ValueError(
            "Incoming source pair is inconsistent: one file has already "
            "been processed successfully while the other is new."
        )

    # ---------------------------------------------------------------------
    # Register both source batches
    # ---------------------------------------------------------------------

    titles_batch_id = register_batch(
        pipeline_name=PIPELINE_NAME,
        file_name="titles.csv",
        file_checksum=titles_checksum,
        rows_received=len(title_rows),
    )

    credits_batch_id = register_batch(
        pipeline_name=PIPELINE_NAME,
        file_name="credits.csv",
        file_checksum=credits_checksum,
        rows_received=len(credit_rows),
    )

    # Mark both batches as actively ingesting.
    mark_batch_running(
        titles_batch_id
    )

    mark_batch_running(
        credits_batch_id
    )

    try:

        # -----------------------------------------------------------------
        # Load the source pair into staging
        # -----------------------------------------------------------------

        titles_loaded = load_titles_to_staging(
            titles_path,
            titles_batch_id,
        )

        credits_loaded = load_credits_to_staging(
            credits_path,
            credits_batch_id,
        )

        # Only mark the pair successful after BOTH files load correctly.
        mark_batch_success(
            titles_batch_id,
            rows_processed=titles_loaded,
        )

        mark_batch_success(
            credits_batch_id,
            rows_processed=credits_loaded,
        )

        return {
            "titles_batch_id": titles_batch_id,
            "credits_batch_id": credits_batch_id,
            "titles_rows": titles_loaded,
            "credits_rows": credits_loaded,
        }

    except Exception as error:

        # The two files represent one logical source delivery.
        # If ingestion fails, record the failure against both batches.
        mark_batch_failed(
            titles_batch_id,
            error,
        )

        mark_batch_failed(
            credits_batch_id,
            error,
        )

        raise


def archive_incoming_batch(
    titles_batch_id,
    credits_batch_id,
):
    """
    Archive successfully transformed source files.

    Files are archived only after downstream transformation succeeds,
    allowing failed pipeline executions to retain their original inputs.
    """

    titles_path = INCOMING_DIR / "titles.csv"
    credits_path = INCOMING_DIR / "credits.csv"

    ARCHIVE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Preserve batch lineage in the archived filenames.
    titles_archive_path = (
        ARCHIVE_DIR
        / f"titles_batch_{titles_batch_id}.csv"
    )

    credits_archive_path = (
        ARCHIVE_DIR
        / f"credits_batch_{credits_batch_id}.csv"
    )

    # Move the processed files out of the incoming directory.
    if titles_path.exists():
        shutil.move(
            titles_path,
            titles_archive_path,
        )

    if credits_path.exists():
        shutil.move(
            credits_path,
            credits_archive_path,
        )

    return {
        "titles_archive": str(
            titles_archive_path
        ),
        "credits_archive": str(
            credits_archive_path
        ),
    }