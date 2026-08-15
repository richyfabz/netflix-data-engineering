# Import Python's datetime class so we can define when this DAG becomes valid.
from datetime import datetime

# Import DAG, which represents an Airflow workflow.
# Import task, which converts a Python function into an Airflow task.
from airflow.sdk import DAG, task


# Create the Airflow DAG that will contain our tasks.
with DAG(

    # Give the DAG a unique name inside Airflow.
    dag_id="00_airflow_healthcheck",

    # Describe the purpose of this DAG in the Airflow UI.
    description="Validate the Netflix Airflow environment.",

    # Disable automatic scheduling because this is currently a manual test.
    schedule=None,

    # Define the date from which Airflow considers this DAG valid.
    start_date=datetime(2026, 8, 1),

    # Prevent Airflow from creating historical runs between the start date and today.
    catchup=False,

    # Add searchable labels to help organise DAGs in the Airflow UI.
    tags=["netflix", "healthcheck"],

# Enter the DAG context so tasks created below belong to this DAG.
) as dag:

    # Convert the Python function below into an Airflow task.
    @task

    # Define the function that our Airflow worker will execute.
    def verify_airflow_environment():

        # Store the project name so we can identify this pipeline in the logs.
        project = "netflix-data-engineering"

        # Print the project name into the Airflow task logs.
        print(f"Project: {project}")

        # Print confirmation that Python execution reached this point successfully.
        print("Airflow task executed successfully.")

        # Return structured information about the result of the health check.
        return {

            # Include the project that was tested.
            "project": project,

            # Mark the environment as healthy when execution reaches this point.
            "status": "healthy",
        }

    # Instantiate the task and attach it to this DAG.
    verify_airflow_environment()