# Use the Airflow runtime as the orchestration environment.
FROM apache/airflow:3.3.1-python3.10
# Install only the additional libraries required by our ETL code.
COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir \
    -r /requirements-airflow.txt

# Make the mounted project modules importable from Airflow DAGs.
ENV PYTHONPATH="/opt/airflow/project:${PYTHONPATH}"