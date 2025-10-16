"""
Simple DAG to test Snowflake connection.

This DAG performs basic connectivity tests to validate the Snowflake connection
by running simple queries to verify database access and functionality.
"""

from datetime import datetime
from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    doc_md=__doc__,
    tags=["snowflake", "test", "connectivity"],
)
def test_snowflake_connection():
    """
    Test Snowflake connection with basic queries.
    """

    # Test 1: Basic connection test
    test_connection = SQLExecuteQueryOperator(
        task_id="test_basic_connection",
        conn_id="snowflake",  # Using the main Snowflake connection
        sql="SELECT 1 as test_result",
        show_return_value_in_logs=True,
    )

    # Test 2: Check current database and schema
    check_context = SQLExecuteQueryOperator(
        task_id="check_database_context",
        conn_id="snowflake",
        sql="""
            SELECT
                CURRENT_DATABASE() as current_database,
                CURRENT_SCHEMA() as current_schema,
                CURRENT_USER() as current_user,
                CURRENT_ROLE() as current_role,
                CURRENT_WAREHOUSE() as current_warehouse
        """,
        show_return_value_in_logs=True,
    )

    # Test 3: Simple query with current timestamp
    test_timestamp = SQLExecuteQueryOperator(
        task_id="test_timestamp_query",
        conn_id="snowflake",
        sql="""
            SELECT
                CURRENT_TIMESTAMP() as current_time,
                'Connection successful!' as status_message
        """,
        show_return_value_in_logs=True,
    )

    # Test 4: Check system information
    system_info = SQLExecuteQueryOperator(
        task_id="check_system_info",
        conn_id="snowflake",
        sql="""
            SELECT
                CURRENT_VERSION() as snowflake_version
        """,
        show_return_value_in_logs=True,
    )

    # Set task dependencies
    test_connection >> check_context >> test_timestamp >> system_info


# Instantiate the DAG
test_snowflake_connection()
