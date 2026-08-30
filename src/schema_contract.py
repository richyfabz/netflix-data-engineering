"""
Schema contract and schema drift detection for the Netflix pipeline.

This module compares incoming source schemas against the expected
source contract and classifies schema changes before ingestion.
"""

import pandas as pd
from src.database import get_etl_connection
import hashlib

PIPELINE_NAME = "netflix_incremental_pipeline"

TITLE_SCHEMA_VERSION = "titles_v1"
CREDIT_SCHEMA_VERSION = "credits_v1"

TITLE_SCHEMA = {
    "id": {"required": True, "type": "string"},
    "title": {"required": True, "type": "string"},
    "type": {"required": True, "type": "string"},
    "description": {"required": True, "type": "string"},
    "release_year": {"required": True, "type": "integer"},
    "age_certification": {"required": True, "type": "string"},
    "runtime": {"required": True, "type": "integer"},
    "genres": {"required": True, "type": "string"},
    "production_countries": {"required": True, "type": "string"},
    "seasons": {"required": True, "type": "integer"},
    "imdb_id": {"required": True, "type": "string"},
    "imdb_score": {"required": True, "type": "numeric"},
    "imdb_votes": {"required": True, "type": "integer"},
    "tmdb_popularity": {"required": True, "type": "numeric"},
    "tmdb_score": {"required": True, "type": "numeric"},
}


CREDIT_SCHEMA = {
    "person_id": {"required": True, "type": "integer"},
    "id": {"required": True, "type": "string"},
    "name": {"required": True, "type": "string"},
    "character": {"required": True, "type": "string"},
    "role": {"required": True, "type": "string"},
}

def calculate_file_checksum(file_path):
    """Return the SHA-256 checksum of a source file."""

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as source_file:
        for chunk in iter(lambda: source_file.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()

def schema_change_already_logged(
    file_name,
    change_type,
    column_name,
    file_checksum,
    schema_version,
):
    """Check whether this exact schema event has already been recorded."""

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT 1
                FROM etl.schema_change_history
                WHERE pipeline_name = %s
                  AND file_name = %s
                  AND change_type = %s
                  AND column_name = %s
                  AND file_checksum = %s
                  AND schema_version = %s
                LIMIT 1;
                """,
                (
                    PIPELINE_NAME,
                    file_name,
                    change_type,
                    column_name,
                    file_checksum,
                    schema_version,
                ),
            )

            return cursor.fetchone() is not None

def log_schema_change(
    file_name,
    change_type,
    column_name,
    file_checksum,
    schema_version,
    old_definition=None,
    new_definition=None,
    is_breaking=False,
    batch_id=None,
    details=None,
):
    """Persist one detected schema change when it has not already been logged."""

    # Avoid recording the same event for the same file version repeatedly.
    if schema_change_already_logged(
        file_name=file_name,
        change_type=change_type,
        column_name=column_name,
        file_checksum=file_checksum,
        schema_version=schema_version,
    ):
        return

    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO etl.schema_change_history (
                    pipeline_name,
                    file_name,
                    batch_id,
                    change_type,
                    column_name,
                    old_definition,
                    new_definition,
                    is_breaking,
                    status,
                    details,
                    file_checksum,
                    schema_version
                )
                VALUES (
                    %(pipeline_name)s,
                    %(file_name)s,
                    %(batch_id)s,
                    %(change_type)s,
                    %(column_name)s,
                    %(old_definition)s,
                    %(new_definition)s,
                    %(is_breaking)s,
                    %(status)s,
                    %(details)s,
                    %(file_checksum)s,
                    %(schema_version)s
                );
                """,
                {
                    "pipeline_name": PIPELINE_NAME,
                    "file_name": file_name,
                    "batch_id": batch_id,
                    "change_type": change_type,
                    "column_name": column_name,
                    "old_definition": old_definition,
                    "new_definition": new_definition,
                    "is_breaking": is_breaking,
                    "status": (
                        "BLOCKED"
                        if is_breaking
                        else "ACCEPTED"
                    ),
                    "details": details,
                    "file_checksum": file_checksum,
                    "schema_version": schema_version,
                },
            )

        connection.commit()


def validate_schema_contract(
    file_name,
    actual_columns,
    expected_schema,
    file_checksum,
    schema_version,
    batch_id=None,
):
    expected_columns = set(expected_schema.keys())
    actual_columns = set(actual_columns)

    missing_columns = sorted(
        expected_columns - actual_columns
    )

    added_columns = sorted(
        actual_columns - expected_columns
    )

    # ---------------------------------------------------------
    # Breaking schema drift
    # ---------------------------------------------------------

    for column in missing_columns:
        log_schema_change(
            file_name=file_name,
            batch_id=batch_id,
            change_type="MISSING_COLUMN",
            column_name=column,
            file_checksum=file_checksum,
            schema_version=schema_version,
            old_definition=str(expected_schema[column]),
            new_definition="missing",
            is_breaking=True,
            details="Required source column disappeared.",
        )

    # ---------------------------------------------------------
    # Non-breaking additive drift
    # ---------------------------------------------------------

    for column in added_columns:
        log_schema_change(
            file_name=file_name,
            batch_id=batch_id,
            change_type="ADDED_COLUMN",
            column_name=column,
            file_checksum=file_checksum,
            schema_version=schema_version,
            old_definition=None,
            new_definition="additional source column",
            is_breaking=False,
            details=(
                "New source column detected. "
                "Pipeline accepted the file but does not yet model this field."
            ),
        )

    # ---------------------------------------------------------
    # Visible operational alert
    # ---------------------------------------------------------

    if added_columns:
        print(
            "SCHEMA DRIFT ALERT:",
            file_name,
            "new columns detected:",
            added_columns,
        )

    if missing_columns:
        print(
            "BREAKING SCHEMA ALERT:",
            file_name,
            "required columns missing:",
            missing_columns,
        )

        raise ValueError(
            f"Breaking schema change detected in {file_name}. "
            f"Missing required columns: {missing_columns}"
        )

    return {
        "file_name": file_name,
        "missing_columns": missing_columns,
        "added_columns": added_columns,
        "schema_valid": True,
    }

def value_matches_type(value, expected_type):
    """Check whether a source value matches the expected logical datatype."""

    if value is None or str(value).strip() == "":
        return True

    value = str(value).strip()

    if expected_type == "string":
        return True

    try:
        if expected_type == "integer":
            int(value)

        elif expected_type == "numeric":
            float(value)

        else:
            raise ValueError(
                f"Unsupported schema contract type: {expected_type}"
            )

        return True

    except (ValueError, TypeError):
        return False
 
                                                            

def coerce_expected_datatypes(file_name,rows, expected_schema):             # Safely convert source values to their expected logical datatypes

    for row_number, row in enumerate(rows, start=2):

        for column_name, schema_rule in expected_schema.items():

            if column_name not in row:
                continue

            value = row[column_name]

            if value is None or value == "":
                continue

            expected_type = (
                schema_rule.get("type")
                if isinstance(schema_rule, dict)
                else schema_rule
            )

            converted_value = value

            try:

                if expected_type == "integer":
                    numeric_value = float(value)

                    if not numeric_value.is_integer():
                        continue

                    converted_value = int(numeric_value)

                elif expected_type == "float":
                    converted_value = float(value)

                elif expected_type == "string":
                    converted_value = str(value)

                elif expected_type == "boolean":
                    normalised = str(value).strip().lower()

                    if normalised in {"true", "1", "yes"}:
                        converted_value = True

                    elif normalised in {"false", "0", "no"}:
                        converted_value = False

                    else:
                        continue

            except (ValueError, TypeError):
                continue

            if str(value) != str(converted_value):

                print(
                    "DATATYPE COERCION:",
                    file_name,
                    f"row={row_number}",
                    f"column={column_name}",
                    f"original={value!r}",
                    f"converted={converted_value!r}",
                    f"expected_type={expected_type}",
                )

            row[column_name] = converted_value

    return rows

def detect_datatype_drift(
    file_name,
    rows,
    expected_schema,
    file_checksum,
    schema_version,
    batch_id=None
):
    """
    Detect incoming source values that violate the expected logical
    datatypes defined by the schema contract.

    Breaking datatype changes are written to the ETL schema-change
    history and prevent the batch from continuing downstream.
    """

    # Store every column that contains incompatible source values.
   
    drift_events = []

    # Validate every column defined by the source contract.
    for column_name, definition in expected_schema.items():

       
        expected_type = definition["type"]

        
        invalid_examples = []

       
        # This makes reported row numbers correspond to what someone
        # would see when opening the original source file.
        for row_number, row in enumerate(
            rows,
            start=2,
        ):

            # Retrieve this column's source value from the current row.
            value = row.get(column_name)

            # Test the source value against the logical datatype defined
            # by the schema contract.
            if not value_matches_type(
                value,
                expected_type,
            ):

                # Preserve enough information to identify where the
                # incompatible value occurred.
                invalid_examples.append(
                    {
                        "row_number": row_number,
                        "value": value,
                    }
                )

            
            if len(invalid_examples) >= 5:
                break

        if not invalid_examples:
            continue

        # Build a human-readable explanation that can be stored in the
        # schema-change audit table and inspected during troubleshooting.
        details = (
            f"Expected logical datatype '{expected_type}'. "
            f"Invalid examples: {invalid_examples}"
        )

       
        log_schema_change(
            file_name=file_name,
            batch_id=batch_id,
            change_type="DATATYPE_DRIFT",
            column_name=column_name,
            old_definition=expected_type,
            new_definition="incompatible source value",
            file_checksum=file_checksum,
            schema_version=schema_version,
            is_breaking=True,
            details=details,
        )

        
        drift_events.append(
            {
                "column_name": column_name,
                "expected_type": expected_type,
                "invalid_examples": invalid_examples,
            }
        )

    # Stop the batch when one or more incompatible datatypes were found.
    if drift_events:

        print(
            "BREAKING DATATYPE DRIFT ALERT:",
            file_name,
            drift_events,
        )

        
        raise ValueError(
            f"Datatype drift detected in {file_name}: "
            f"{drift_events}"
        )

    return drift_events