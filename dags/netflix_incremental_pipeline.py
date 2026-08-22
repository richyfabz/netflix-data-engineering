"""
Airflow DAG for the Netflix incremental data engineering pipeline.

The DAG coordinates ingestion, transformation, quality validation,
and checkpoint management using the production modules in src/.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


# ---------------------------------------------------------------------------
# DAG configuration
# ---------------------------------------------------------------------------

with DAG(
    dag_id="netflix_incremental_pipeline",

    # Start date is required for Airflow scheduling.
    start_date=datetime(2026, 8, 22),

    # Run the pipeline once per day.
    schedule="@daily",

    # Prevent Airflow from creating historical runs automatically.
    catchup=False,

    # Only allow one active pipeline run at a time.
    max_active_runs=1,

    tags=[
        "netflix",
        "data-engineering",
        "incremental-etl",
    ],
) as dag:

    # Temporary boundary task used to verify DAG discovery.
    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    # Temporary completion task.
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete"
    )

    # Define the initial execution dependency.
    start_pipeline >> pipeline_complete