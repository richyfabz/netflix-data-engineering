"""Utilities for executing production SQL transformation files."""

from pathlib import Path

import sqlparse

from src.database import get_etl_connection


def read_sql_file(sql_path):
    """Read a production SQL file from disk."""

    path = Path(sql_path)

    if not path.exists():
        raise FileNotFoundError(
            f"SQL file does not exist: {path}"
        )

    return path.read_text(
        encoding="utf-8"
    )


def split_sql_statements(sql_text):
    """Split a SQL script into executable statements."""

    statements = sqlparse.split(sql_text)

    return [
        statement.strip()
        for statement in statements
        if statement.strip()
    ]


def execute_sql_file(
    sql_path,
    parameters=None,
):
    """
    Execute every statement in a SQL file as one transaction.

    If any statement fails, the transaction is rolled back.
    """

    sql_text = read_sql_file(sql_path)

    statements = split_sql_statements(
        sql_text
    )

    with get_etl_connection() as connection:

        try:
            with connection.cursor() as cursor:

                for statement in statements:
                    cursor.execute(
                        statement,
                        parameters or {},
                    )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

    return len(statements)