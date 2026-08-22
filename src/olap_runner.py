"""Production OLAP transformation runner."""

from pathlib import Path

from src.sql_runner import execute_sql_file


PROJECT_ROOT = Path(__file__).resolve().parent.parent

OLAP_SQL_DIR = (
    PROJECT_ROOT
    / "sql"
    / "03_olap"
)


OLAP_TRANSFORMATIONS = [
    "01_upsert_dim_title.sql",
    "02_upsert_dim_person.sql",
    "03_upsert_dim_genre.sql",
    "04_upsert_dim_country.sql",
    "05_sync_bridge_title_genre.sql",
    "06_sync_bridge_title_country.sql",
    "07_upsert_fact_credits.sql",
    "08_upsert_fact_title_metrics.sql",
]


def run_olap_transformations(
    titles_batch_id,
):
    """Run the complete production OLAP transformation sequence."""

    results = []

    for sql_filename in OLAP_TRANSFORMATIONS:

        sql_path = (
            OLAP_SQL_DIR
            / sql_filename
        )

        # Only scripts containing batch placeholders need parameters.
        if sql_filename in {
            "01_upsert_dim_title.sql",
            "05_sync_bridge_title_genre.sql",
            "06_sync_bridge_title_country.sql",
            "08_upsert_fact_title_metrics.sql",
        }:
            parameters = {
                "batch_id": titles_batch_id,
            }

        else:
            parameters = None

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