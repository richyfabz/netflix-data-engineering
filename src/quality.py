"""Production data-quality checks for the Netflix pipeline."""

# Import the shared ETL database connection.
from src.database import get_etl_connection


# Define the quality checks that must return zero violations.
ZERO_VIOLATION_CHECKS = {
    "duplicate_titles": """
        SELECT COUNT(*)
        FROM (
            SELECT title_id
            FROM uat_oltp.title
            GROUP BY title_id
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "duplicate_people": """
        SELECT COUNT(*)
        FROM (
            SELECT person_id
            FROM uat_oltp.person
            GROUP BY person_id
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """,

    "orphan_genre_bridges": """
        SELECT COUNT(*)
        FROM uat_olap.bridge_title_genre AS bridge

        LEFT JOIN uat_olap.dim_title AS dt
            ON dt.title_sk = bridge.title_sk

        LEFT JOIN uat_olap.dim_genre AS dg
            ON dg.genre_sk = bridge.genre_sk

        WHERE dt.title_sk IS NULL
           OR dg.genre_sk IS NULL;
    """,

    "orphan_country_bridges": """
        SELECT COUNT(*)
        FROM uat_olap.bridge_title_country AS bridge

        LEFT JOIN uat_olap.dim_title AS dt
            ON dt.title_sk = bridge.title_sk

        LEFT JOIN uat_olap.dim_country AS dc
            ON dc.country_sk = bridge.country_sk

        WHERE dt.title_sk IS NULL
           OR dc.country_sk IS NULL;
    """,

    "orphan_credit_facts": """
        SELECT COUNT(*)
        FROM uat_olap.fact_credits AS fc

        LEFT JOIN uat_olap.dim_title AS dt
            ON dt.title_sk = fc.title_sk

        LEFT JOIN uat_olap.dim_person AS dp
            ON dp.person_sk = fc.person_sk

        WHERE dt.title_sk IS NULL
           OR dp.person_sk IS NULL;
    """,

    "missing_title_metrics": """
        SELECT COUNT(*)
        FROM uat_olap.dim_title AS dt

        LEFT JOIN uat_olap.fact_title_metrics AS fm
            ON fm.title_sk = dt.title_sk

        WHERE fm.title_sk IS NULL;
    """, 
    
    "missing_title_ingestion_dates": """
    SELECT COUNT(*)
    FROM uat_olap.dim_title
    WHERE ingestion_date_sk IS NULL;
        """,

        "orphan_ingestion_date_keys": """
            SELECT COUNT(*)
            FROM uat_olap.dim_title AS dt

            LEFT JOIN uat_olap.dim_date AS dd
                ON dd.date_sk = dt.ingestion_date_sk

            WHERE dt.ingestion_date_sk IS NOT NULL
            AND dd.date_sk IS NULL;
        """
}


def record_quality_result(
    pipeline_name,
    batch_id,
    layer,
    check_name,
    status,
    observed_value,
    expected_value=0,
    details=None,
):
    """Store one quality-check result in PostgreSQL."""

    with get_etl_connection() as connection:

        with connection.cursor() as cursor:

            # Write the check result to the audit table.
            cursor.execute(
                """
                INSERT INTO etl.quality_results (
                    pipeline_name,
                    batch_id,
                    layer,
                    check_name,
                    status,
                    observed_value,
                    expected_value,
                    details
                )
                VALUES (
                    %(pipeline_name)s,
                    %(batch_id)s,
                    %(layer)s,
                    %(check_name)s,
                    %(status)s,
                    %(observed_value)s,
                    %(expected_value)s,
                    %(details)s
                );
                """,
                {
                    "pipeline_name": pipeline_name,
                    "batch_id": batch_id,
                    "layer": layer,
                    "check_name": check_name,
                    "status": status,
                    "observed_value": observed_value,
                    "expected_value": expected_value,
                    "details": details,
                },
            )

        # Persist the audit record.
        connection.commit()


def run_zero_violation_check(
    check_name,
    sql_query,
    pipeline_name,
    batch_id=None,
    layer="OLAP",
):
    """Run a check where zero violations means PASS."""

    # Execute the requested validation query.
    with get_etl_connection() as connection:
        with connection.cursor() as cursor:

            # Run the quality rule.
            cursor.execute(sql_query)

            # Read the violation count.
            observed_value = cursor.fetchone()[0]

    # Zero violations means the rule passed.
    status = "PASS" if observed_value == 0 else "FAIL"

    # Record the result for auditability.
    record_quality_result(
        pipeline_name=pipeline_name,
        batch_id=batch_id,
        layer=layer,
        check_name=check_name,
        status=status,
        observed_value=observed_value,
        expected_value=0,
    )

    # Return the result to the caller.
    return {
        "check_name": check_name,
        "status": status,
        "observed_value": observed_value,
    }


def run_core_quality_checks(
    pipeline_name,
    batch_id=None,
):
    """Run all critical pipeline quality checks."""

    # Hold the result of every executed rule.
    results = []

    # Run each registered quality rule.
    for check_name, sql_query in ZERO_VIOLATION_CHECKS.items():

        # Execute and audit the current rule.
        result = run_zero_violation_check(
            check_name=check_name,
            sql_query=sql_query,
            pipeline_name=pipeline_name,
            batch_id=batch_id,
        )

        # Keep the result for the final pipeline decision.
        results.append(result)

    # Collect any failed rules.
    failed_checks = [
        result
        for result in results
        if result["status"] == "FAIL"
    ]

    # Stop the pipeline if any critical quality check failed.
    if failed_checks:
        failed_names = [
            result["check_name"]
            for result in failed_checks
        ]

        raise RuntimeError(
            "Data-quality checks failed: "
            + ", ".join(failed_names)
        )

    # Return all successful results.
    return results