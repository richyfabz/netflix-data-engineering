"""Production OLTP transformation runner."""

from pathlib import Path

from src.sql_runner import execute_sql_file


PROJECT_ROOT = Path(__file__).resolve().parent.parent

OLTP_SQL_DIR = (
    PROJECT_ROOT
    / "sql"
    / "02_oltp"
)


OLTP_TRANSFORMATIONS = [
    "01_upsert_titles.sql",
    "02_upsert_people.sql",
    "03_upsert_genres.sql",
    "04_upsert_countries.sql",
    "05_sync_title_genres.sql",
    "06_sync_title_countries.sql",
    "07_upsert_credits.sql",
]


def run_oltp_transformations(
    titles_batch_id,
    credits_batch_id,
):
    """Run the complete production OLTP transformation sequence."""

    results = []

    for sql_filename in OLTP_TRANSFORMATIONS:

        sql_path = (
            OLTP_SQL_DIR
            / sql_filename
        )

        # Credits are sourced from the credits batch.
        if sql_filename == "07_upsert_credits.sql":
            parameters = {
                "batch_id": credits_batch_id,
            }

        # Remaining OLTP transformations use the titles batch.
        else:
            parameters = {
                "batch_id": titles_batch_id,
            }

        statements_executed = execute_sql_file(
            sql_path=sql_path,
            parameters=parameters,
        )

        results.append(
            {
                "sql_file": sql_filename,
                "statements_executed": statements_executed,
            }
        )

    return results