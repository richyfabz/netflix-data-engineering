"""
Raw source ingestion for the Netflix data engineering pipeline.

This module validates incoming CSV structure and loads source records
into PostgreSQL staging tables.
"""

# Import csv for reading CSV source files.
import csv

# Import Path for filesystem operations.
from pathlib import Path

# Import the shared PostgreSQL connection helper.
from src.database import get_etl_connection


# Define the source columns expected from titles.csv.
TITLE_COLUMNS = [
    "id",
    "title",
    "type",
    "description",
    "release_year",
    "age_certification",
    "runtime",
    "genres",
    "production_countries",
    "seasons",
    "imdb_id",
    "imdb_score",
    "imdb_votes",
    "tmdb_popularity",
    "tmdb_score",
]

# Define the source columns expected from credits.csv.
CREDIT_COLUMNS = [
    "person_id",
    "id",
    "name",
    "character",
    "role",
]
# Read and validate a CSV source file.
def read_source_csv(file_path, expected_columns):
    """
    Read a CSV file after validating its expected columns.
    """

    # Convert the supplied file location into a Path object.
    file_path = Path(file_path)

    # Stop if the source file cannot be found.
    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file does not exist: {file_path}"
        )

    # Open the CSV using UTF-8 encoding.
    with file_path.open(
        mode="r",
        encoding="utf-8",
        newline="",
    ) as source_file:

        # Read each CSV row as a dictionary.
        reader = csv.DictReader(source_file)

        # Retrieve the actual source columns.
        actual_columns = reader.fieldnames or []

        # Identify any required columns missing from the source.
        missing_columns = [
            column
            for column in expected_columns
            if column not in actual_columns
        ]

        # Prevent ingestion when the source schema is unexpected.
        if missing_columns:
            raise ValueError(
                f"Missing source columns: {missing_columns}"
            )

        # Materialise the validated records.
        rows = list(reader)

    # Return all validated source records.
    return rows


# Load one titles batch into raw staging.
def load_titles_to_staging(file_path, batch_id):
    """
    Load titles.csv into staging.titles_raw.

    Returns the number of staged records.
    """

    # Read and validate the source data.
    rows = read_source_csv(
        file_path,
        TITLE_COLUMNS,
    )

    # Preserve the source filename for lineage.
    source_file = Path(file_path).name

    # Connect using the dedicated ETL account.
    with get_etl_connection() as connection:

        # Open a PostgreSQL cursor.
        with connection.cursor() as cursor:

            # Remove an incomplete previous staging attempt for this batch.
            cursor.execute(
                """
                DELETE FROM staging.titles_raw
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

            # Insert each validated source row.
            for row in rows:

                cursor.execute(
                    """
                    INSERT INTO staging.titles_raw (
                        batch_id,
                        id,
                        title,
                        type,
                        description,
                        release_year,
                        age_certification,
                        runtime,
                        genres,
                        production_countries,
                        seasons,
                        imdb_id,
                        imdb_score,
                        imdb_votes,
                        tmdb_popularity,
                        tmdb_score,
                        source_file
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s
                    );
                    """,
                    (
                        batch_id,
                        row["id"],
                        row["title"],
                        row["type"],
                        row["description"],
                        row["release_year"],
                        row["age_certification"],
                        row["runtime"],
                        row["genres"],
                        row["production_countries"],
                        row["seasons"],
                        row["imdb_id"],
                        row["imdb_score"],
                        row["imdb_votes"],
                        row["tmdb_popularity"],
                        row["tmdb_score"],
                        source_file,
                    ),
                )
    
        # Save the complete staging transaction.
        connection.commit()

    # Return the number of records loaded.
    return len(rows)
# Load one credits batch into raw staging.
def load_credits_to_staging(file_path, batch_id):
    """
    Load credits.csv into staging.credits_raw.

    Returns the number of staged records.
    """

    # Read and validate the source data.
    rows = read_source_csv(
        file_path,
        CREDIT_COLUMNS,
    )

    # Preserve the original source filename for lineage.
    source_file = Path(file_path).name

    # Connect using the dedicated ETL account.
    with get_etl_connection() as connection:

        # Open a PostgreSQL cursor.
        with connection.cursor() as cursor:

            # Remove a previous incomplete load for this same batch.
            cursor.execute(
                """
                DELETE FROM staging.credits_raw
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

            # Insert each validated credit record.
            for row in rows:

                cursor.execute(
                    """
                    INSERT INTO staging.credits_raw (
                        batch_id,
                        person_id,
                        id,
                        name,
                        character,
                        role,
                        source_file
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s
                    );
                    """,
                    (
                        batch_id,
                        row["person_id"],
                        row["id"],
                        row["name"],
                        row["character"],
                        row["role"],
                        source_file,
                    ),
                )

        # Save the full batch.
        connection.commit()

    # Return the number of staged records.
    return len(rows)