"""End-to-end production transformation pipeline coordinator."""

from src.batch_tracker import (
    get_batch_metadata,
    validate_batch,
)
from src.pipeline_control import (
    mark_pipeline_running,
    mark_pipeline_success,
    mark_pipeline_failed,
)

from src.oltp_runner import run_oltp_transformations
from src.olap_runner import run_olap_transformations
from src.quality import run_core_quality_checks


PIPELINE_NAME = "netflix_incremental_pipeline"


def run_transformation_pipeline(
    titles_batch_id,
    credits_batch_id,
):
    """
    Run the complete production transformation pipeline.

    Pipeline control is advanced only after OLTP, OLAP, quality validation,
    and both source batches complete successfully.
    """

    results = {
        "titles_batch_id": titles_batch_id,
        "credits_batch_id": credits_batch_id,
    }
     # Validate both source batches before starting transformations.
    validate_batch(
        titles_batch_id
    )

    validate_batch(
        credits_batch_id
    )
    # Mark the transformation pipeline as active.
#
# Source batch status is owned by the ingestion layer and must not be
# rewritten by downstream OLTP, OLAP, or quality processing.
    mark_pipeline_running(
    PIPELINE_NAME
    )

  

    try:

        # Transform staged source data into the operational model.
        oltp_results = run_oltp_transformations(
            titles_batch_id=titles_batch_id,
            credits_batch_id=credits_batch_id,
        )

        results["oltp"] = oltp_results


        # Build the analytical warehouse from the updated OLTP state.
        olap_results = run_olap_transformations(
            titles_batch_id=titles_batch_id,
        )

        results["olap"] = olap_results


        # Validate warehouse integrity before committing pipeline success.
        quality_results = run_core_quality_checks(
            pipeline_name=PIPELINE_NAME,
            batch_id=titles_batch_id,
        )

        results["quality"] = quality_results


        # Retrieve source metadata for checkpoint advancement.
        titles_metadata = get_batch_metadata(
            titles_batch_id
        )

        credits_metadata = get_batch_metadata(
            credits_batch_id
        )


        # Extract metadata used by the pipeline checkpoint.
        titles_received_at = titles_metadata[2]

        titles_rows_processed = (
            titles_metadata[4] or 0
        )

        credits_rows_processed = (
            credits_metadata[4] or 0
        )

        total_rows_processed = (
            titles_rows_processed
            + credits_rows_processed
        )


       


        # Advance the pipeline checkpoint only after every stage succeeds.
        mark_pipeline_success(
            pipeline_name=PIPELINE_NAME,
            watermark=titles_received_at,
            file_name="titles.csv + credits.csv",
            rows_processed=total_rows_processed,
        )


        # Return the final production result to the caller.
        results["status"] = "SUCCESS"
        results["watermark"] = titles_received_at
        results["rows_processed"] = total_rows_processed

        return results


    except Exception as error:
        """
        Preserve the successful source-ingestion history when a downstream
        transformation fails.

        The pipeline control record owns transformation failures. The source
        batches remain unchanged so they can be diagnosed or safely retried.
        """

        # Record the transformation failure without advancing the
        # last-successful pipeline checkpoint.
        mark_pipeline_failed(
            PIPELINE_NAME
        )

        # Re-raise the original exception so Airflow also marks the
        # current task and DAG run as failed.
        raise