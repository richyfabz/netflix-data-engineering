"""
Production Airflow DAG for the Netflix incremental data pipeline.

The DAG resolves the latest successful source batches and passes those
batch identifiers into the production transformation pipeline.

The transformation runner handles OLTP, OLAP, quality validation,
and checkpoint management using the production modules in src/.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.database import get_etl_connection
from src.batch_tracker import recover_stale_batches
from src.pipeline_runner import run_transformation_pipeline


# Stale batch recovery

def recover_abandoned_batches():
    """
    Recover ingestion batches that were left in RUNNING state.

    A batch older than 60 minutes with no completion timestamp is treated
    as abandoned and marked FAILED before new work is resolved.
    """

    recovered_batches = recover_stale_batches(
        pipeline_name="netflix_incremental_pipeline",
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

# Batch resolution

def resolve_latest_batches():
    """
    Retrieve the latest successful titles and credits ingestion batches.

    Only completed batches are eligible for downstream transformation.
    The returned identifiers are passed to the transformation task
    through Airflow XCom.
    """

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Retrieve the most recent successful titles.csv batch.
            cursor.execute(
                """
                SELECT batch_id
                FROM etl.batch_history
                WHERE pipeline_name = 'netflix_incremental_pipeline'
                  AND file_name = 'titles.csv'
                  AND status = 'SUCCESS'
                ORDER BY batch_id DESC
                LIMIT 1;
                """
            )

            titles_row = cursor.fetchone()

            # Retrieve the most recent successful credits.csv batch.
            cursor.execute(
                """
                SELECT batch_id
                FROM etl.batch_history
                WHERE pipeline_name = 'netflix_incremental_pipeline'
                  AND file_name = 'credits.csv'
                  AND status = 'SUCCESS'
                ORDER BY batch_id DESC
                LIMIT 1;
                """
            )

            credits_row = cursor.fetchone()

    # Stop the DAG before transformation if either required source batch
    # cannot be resolved.
    if titles_row is None:
        raise ValueError(
            "No successful titles.csv batch was found."
        )

    if credits_row is None:
        raise ValueError(
            "No successful credits.csv batch was found."
        )

    # Store both identifiers in one dictionary so Airflow can pass them
    # to the downstream transformation task through XCom.
    batch_ids = {
        "titles_batch_id": titles_row[0],
        "credits_batch_id": credits_row[0],
    }

    print(
        "Resolved batch IDs:",
        batch_ids,
    )

    return batch_ids


# ============================================================================
# Production transformation execution
# ============================================================================

def execute_transformation(**context):
    """
    Execute the production transformation pipeline using the batch IDs
    resolved by the previous Airflow task.
    """

    # Pull the dictionary returned by resolve_latest_batches.
    batch_ids = context["ti"].xcom_pull(
        task_ids="resolve_latest_batches"
    )

    # Fail clearly if XCom did not return the expected payload.
    if not batch_ids:
        raise ValueError(
            "No batch metadata was returned by resolve_latest_batches."
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

    # Reuse the production transformation coordinator already validated
    # during notebook testing.
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
# DAG definition
# ============================================================================

with DAG(
    dag_id="netflix_incremental_pipeline",

    # Airflow requires a start date even though the DAG
    # is currently triggered manually.
    start_date=datetime(2026, 8, 23),

    # Keep execution manual until scheduling is introduced later.
    schedule=None,

    # Do not create historical runs automatically.
    catchup=False,

    # Only one transformation run may operate against the warehouse
    # at a time.
    max_active_runs=1,

    # Prevent an abnormal DAG run from remaining active indefinitely.
    dagrun_timeout=timedelta(minutes=30),

    tags=[
        "netflix",
        "etl",
        "incremental",
        "data-engineering",
    ],
) as dag:

    

    # Recover abandoned ingestion batches
    # This prevents interrupted ingestion jobs from remaining permanently
    # recorded as RUNNING.
    recover_stale = PythonOperator(
        task_id="recover_stale_batches",
        python_callable=recover_abandoned_batches,

        # Metadata cleanup is safe to retry if the database connection
        # experiences a temporary interruption.
        retries=2,

        retry_delay=timedelta(minutes=1),

        retry_exponential_backoff=True,

        max_retry_delay=timedelta(minutes=5),

        # Recovery should remain a lightweight metadata operation.
        execution_timeout=timedelta(minutes=3),
    )

    # Resolve the latest valid source batches    #
    # Batch resolution is lightweight and read-only, so retrying this task
    # is safe if PostgreSQL is temporarily unavailable.
    resolve_batches = PythonOperator(
        task_id="resolve_latest_batches",
        python_callable=resolve_latest_batches,

        # Retry temporary infrastructure failures before declaring
        # the task failed.
        retries=3,

        # Begin with a short delay between retry attempts.
        retry_delay=timedelta(minutes=1),

        # Increase the delay after repeated failures.
        retry_exponential_backoff=True,

        # Prevent retry delays from increasing without limit.
        max_retry_delay=timedelta(minutes=5),

        # Batch lookup should never be a long-running operation.
        execution_timeout=timedelta(minutes=3),
    )

    # ------------------------------------------------------------------------
    # Execute the production transformation pipeline
    # ------------------------------------------------------------------------
    #
    # The transformation pipeline has already been validated as idempotent,
    # so Airflow may retry a temporary execution failure without creating
    # duplicate analytical records.
    run_pipeline = PythonOperator(
        task_id="run_transformation_pipeline",
        python_callable=execute_transformation,

        # Allow recovery from temporary database or infrastructure failures.
        retries=2,

        # Avoid immediately retrying against the same temporary failure.
        retry_delay=timedelta(minutes=2),

        # Increase the delay if multiple retries are required.
        retry_exponential_backoff=True,

        # Keep the retry interval within a predictable upper bound.
        max_retry_delay=timedelta(minutes=10),

        # Stop a transformation that runs significantly longer than expected.
        execution_timeout=timedelta(minutes=20),
    )

    # Transformation cannot begin until valid source batches are resolved.
recover_stale >> resolve_batches >> run_pipeline