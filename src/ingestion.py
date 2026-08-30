"""
Raw source ingestion for the Netflix data engineering pipeline.

This module validates incoming CSV structure and loads source records
into PostgreSQL staging tables.
"""

import csv

from pathlib import Path

from src.database import get_etl_connection

from src.schema_contract import (TITLE_SCHEMA, CREDIT_SCHEMA, TITLE_SCHEMA_VERSION, CREDIT_SCHEMA_VERSION,validate_schema_contract, detect_datatype_drift, coerce_expected_datatypes, calculate_file_checksum)


def read_source_csv(file_path, expected_schema, schema_version):            # Read and validate a CSV source file before dev.

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file does not exist: {file_path}"
        )

    file_checksum = calculate_file_checksum(file_path)

    with file_path.open(mode="r", encoding="utf-8",newline="") as source_file:

        reader = csv.DictReader(source_file)

        actual_columns = reader.fieldnames or []

        validate_schema_contract(
            file_name=file_path.name,
            actual_columns=actual_columns,
            expected_schema=expected_schema,
            file_checksum=file_checksum,
            schema_version=schema_version,
        )

        rows = list(reader)

    rows = coerce_expected_datatypes(file_name=file_path.name,rows=rows,expected_schema=expected_schema)

    detect_datatype_drift(
        file_name=file_path.name,
        rows=rows,
        expected_schema=expected_schema,
        file_checksum=file_checksum,
        schema_version=schema_version,
    )

    return rows


def load_titles_to_staging(file_path,batch_id):                                # Load titles.csv into dev.titles_raw.

    rows = read_source_csv( file_path, TITLE_SCHEMA, TITLE_SCHEMA_VERSION)     # Validate the source before dev. 
    


    source_file = Path(file_path).name

    with get_etl_connection() as connection:

        with connection.cursor() as cursor:

            
            cursor.execute(                               # Remove any incomplete previous load for this batch.
                """
                DELETE FROM dev.titles_raw
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

            for row in rows:

                cursor.execute(
                    """
                    INSERT INTO dev.titles_raw (
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

        connection.commit()

    return len(rows)


def load_credits_to_staging(file_path,batch_id):                            # Load credits.csv into dev.credits_raw.
    
   
    rows = read_source_csv( file_path, CREDIT_SCHEMA, CREDIT_SCHEMA_VERSION)   # Validate the source before dev.

    source_file = Path(file_path).name

    with get_etl_connection() as connection:

        with connection.cursor() as cursor:

           
            cursor.execute(                      # Remove any incomplete previous load for this batch.              
                """
                DELETE FROM dev.credits_raw
                WHERE batch_id = %s;
                """,
                (batch_id,),
            )

            for row in rows:

                cursor.execute(
                    """
                    INSERT INTO dev.credits_raw (
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

        connection.commit()

    return len(rows)