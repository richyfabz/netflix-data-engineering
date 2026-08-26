"""
Production Airflow DAG for the Netflix incremental data pipeline.

The DAG automatically detects and ingests a complete source batch from
data/incoming/, then executes the production transformation pipeline.

The pipeline handles:
- stale batch recovery
- automatic source ingestion
- OLTP transformation
- OLAP transformation
- quality validation
- checkpoint management
- source-file archival after successful processing
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

from src.batch_tracker import recover_stale_batches
from src.pipeline_runner import run_transformation_pipeline
from src.source_batch_runner import (
    ingest_incoming_batch,
    archive_incoming_batch,
)

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAG_ID = "netflix_incremental_pipeline_continuous"

PIPELINE_NAME = "netflix_incremental_pipeline"


# ============================================================================
# Stale batch recovery
# ============================================================================

def recover_abandoned_batches():
    """
    Recover ingestion batches that were left in RUNNING state.

    A batch older than 60 minutes with no completion timestamp is treated
    as abandoned and marked FAILED before new work begins.
    """

    recovered_batches = recover_stale_batches(
        pipeline_name=PIPELINE_NAME,
        stale_after_minutes=60,
    )

    print(
        "Recovered stale batch count:",
        len(recovered_batches),
    )

    for batch in recovered_batches:
        print(
            "Recovered stale batch:",
            batch,
        )

    return len(recovered_batches)


# ============================================================================
# Automatic source ingestion
# ============================================================================

def ingest_new_source_batch():
    """
    Detect and ingest one complete titles.csv + credits.csv source pair.

    Returns the exact batch IDs created by this ingestion run.

    If no new source files exist, the Airflow task is skipped cleanly
    rather than treated as a pipeline failure.
    """

    ingestion_result = ingest_incoming_batch()

    # No incoming files or a previously processed duplicate is not a failure.
    if ingestion_result is None:
        raise AirflowSkipException(
            "No new source batch is available for ingestion."
        )

    print(
        "Automatic ingestion result:",
        ingestion_result,
    )

    return ingestion_result


# ============================================================================
# Production transformation execution
# ============================================================================

def execute_transformation(**context):
    """
    Execute the production transformation pipeline using the exact batch IDs
    created by the ingestion task.
    """

    # Pull the dictionary returned by ingest_new_source_batch.
    batch_ids = context["ti"].xcom_pull(
        task_ids="ingest_new_batch"
    )

    # Fail clearly if the expected XCom payload is missing.
    if not batch_ids:
        raise ValueError(
            "No ingestion metadata was returned by ingest_new_batch."
        )

    titles_batch_id = batch_ids["titles_batch_id"]
    credits_batch_id = batch_ids["credits_batch_id"]

    print(
        "Running transformation with titles batch:",
        titles_batch_id,
    )

    print(
        "Running transformation with credits batch:",
        credits_batch_id,
    )

    # Reuse the production pipeline coordinator that already handles:
    # OLTP, OLAP, quality validation, batch states, and checkpoint management.
    results = run_transformation_pipeline(
        titles_batch_id=titles_batch_id,
        credits_batch_id=credits_batch_id,
    )

    print(
        "Transformation pipeline result:",
        results,
    )

    return results


# ============================================================================
# Source archival
# ============================================================================

def archive_processed_source(**context):
    """
    Archive the successfully processed source files.

    Files are only moved after the complete transformation pipeline succeeds.
    """

    batch_ids = context["ti"].xcom_pull(
        task_ids="ingest_new_batch"
    )

    if not batch_ids:
        raise ValueError(
            "No ingestion metadata was returned for source archival."
        )

    archive_result = archive_incoming_batch(
        titles_batch_id=batch_ids["titles_batch_id"],
        credits_batch_id=batch_ids["credits_batch_id"],
    )

    print(
        "Archived source files:",
        archive_result,
    )

    return archive_result



def resolve_latest_batches(**context):
    """Return the batch IDs created by the current ingestion task."""

    ingestion_result = context["ti"].xcom_pull(
        task_ids="ingest_new_batch"
    )

    if not ingestion_result:
        raise AirflowSkipException(
            "No new source batch was ingested."
        )

    batch_ids = {
        "titles_batch_id": ingestion_result["titles_batch_id"],
        "credits_batch_id": ingestion_result["credits_batch_id"],
    }

    print(
        "Resolved current batch IDs:",
        batch_ids,
    )

    return batch_ids

def source_batch_available():
    """Check whether both required source files are available."""

    incoming_dir = Path("/opt/airflow/project/data/incoming")

    titles_path = incoming_dir / "titles.csv"
    credits_path = incoming_dir / "credits.csv"

    return titles_path.exists() and credits_path.exists()

# ============================================================================
# DAG definition
# ============================================================================

with DAG(
    dag_id=DAG_ID,

    # Airflow requires a fixed start date.
    start_date=datetime(2026, 8, 23),

    # Run continuously, with only one active pipeline execution at a time.
    schedule="@continuous",

    # Do not create historical runs for missed scheduling periods.
    catchup=False,

    # Only one pipeline run may modify the warehouse at a time.
    max_active_runs=1,

    # Prevent abnormal DAG executions from remaining active indefinitely.
    dagrun_timeout=timedelta(minutes=35),

    tags=[
        "netflix",
        "etl",
        "incremental",
        "data-engineering",
    ],
) as dag:

    
    wait_for_batch = PythonSensor(
    task_id="wait_for_source_batch",
    python_callable=source_batch_available,
    poke_interval=10,
    mode="reschedule",
    timeout=60 * 60 * 24,
)

    # ------------------------------------------------------------------------
    # Recover abandoned ingestion batches
    # ------------------------------------------------------------------------

    recover_stale = PythonOperator(
        task_id="recover_stale_batches",
        python_callable=recover_abandoned_batches,

        retries=2,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),

        execution_timeout=timedelta(minutes=3),
    )

    # ------------------------------------------------------------------------
    # Detect and ingest a new source batch
    # ------------------------------------------------------------------------

    ingest_batch = PythonOperator(
        task_id="ingest_new_batch",
        python_callable=ingest_new_source_batch,

        # Temporary database failures can safely be retried.
        retries=2,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),

        execution_timeout=timedelta(minutes=10),
    )

        # Resolve the batch IDs created by the current ingestion run.
    resolve_batches = PythonOperator(
        task_id="resolve_latest_batches",
        python_callable=resolve_latest_batches,

        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),

        execution_timeout=timedelta(minutes=3),
    )
    # ------------------------------------------------------------------------
    # Execute the production transformation pipeline
    # ------------------------------------------------------------------------

    run_pipeline = PythonOperator(
        task_id="run_transformation_pipeline",
        python_callable=execute_transformation,

        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),

        execution_timeout=timedelta(minutes=20),
    )

    # ------------------------------------------------------------------------
    # Archive the source files only after a successful pipeline run
    # ------------------------------------------------------------------------

    archive_source = PythonOperator(
        task_id="archive_source_files",
        python_callable=archive_processed_source,

        retries=2,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),

        execution_timeout=timedelta(minutes=5),
    )

    # Production dependency chain.
wait_for_batch >> recover_stale >> ingest_batch >> resolve_batches >> run_pipeline >> archive_source