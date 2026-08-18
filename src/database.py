"""
PostgreSQL connection utilities for the Netflix data engineering pipeline.

This module centralises database connection logic so notebooks,
Airflow DAGs and production modules do not duplicate credentials
or connection code.
"""

# Import os so environment variables can be read securely.
import os

# Import psycopg so Python can connect to PostgreSQL.
import psycopg

# Import load_dotenv for local development with a .env file.
from dotenv import load_dotenv


# Load environment variables from the local .env file.
# override=True prevents stale notebook/terminal variables from taking priority.
load_dotenv(override=True)


# Define a reusable function for opening the ETL database connection.
def get_etl_connection():
    """
    Return a PostgreSQL connection using the dedicated netflix_etl account.
    """

    # Read the PostgreSQL host from the environment.
    db_host = os.getenv("NETFLIX_DB_HOST")

    # Read the PostgreSQL port from the environment.
    db_port = os.getenv("NETFLIX_DB_PORT")

    # Read the PostgreSQL database name.
    db_name = os.getenv("NETFLIX_DB_NAME")

    # Read the ETL username.
    db_user = os.getenv("NETFLIX_ETL_USER")

    # Read the ETL password.
    db_password = os.getenv("NETFLIX_ETL_PASSWORD")

    # Store required settings so we can validate them before connecting.
    required_values = {
        "NETFLIX_DB_HOST": db_host,
        "NETFLIX_DB_PORT": db_port,
        "NETFLIX_DB_NAME": db_name,
        "NETFLIX_ETL_USER": db_user,
        "NETFLIX_ETL_PASSWORD": db_password,
    }

    # Find any configuration values that are missing.
    missing_values = [
        name
        for name, value in required_values.items()
        if not value
    ]

    # Stop immediately if database configuration is incomplete.
    if missing_values:
        raise ValueError(
            f"Missing database environment variables: {missing_values}"
        )

    # Open and return the PostgreSQL connection.
    return psycopg.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password,
    )