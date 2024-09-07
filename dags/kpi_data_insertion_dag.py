from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "data_warehouse_conn_id"
SCHEMA = "business_metrics"

def get_postgres_connection():
    """Gets a connection to the PostgreSQL database."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=SCHEMA)
    return pg_hook.get_conn()

def execute_query(connection, query):
    """Executes a query on the database."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        connection.commit()

def insert_kpi_transaction_data(**kwargs):
    """
    Inserts KPI data related to transaction times across different systems.

    This function calculates the elapsed time in minutes between the most recent transaction 
    and the current system time (or a specific timezone) for multiple transaction sources. 
    The results are inserted into the `data_warehouse.load_times` table for further analysis.
    """
    try:
        connection = get_postgres_connection()
        query = """
        INSERT INTO data_warehouse.load_times (time_elapsed)
        VALUES (
            (SELECT DATEDIFF(minutes, MAX(T.created_at), SYSDATE) AS elapsed_time FROM business_metrics.transactions T),
            (SELECT DATEDIFF(minutes, MAX(T.created_at), CONVERT_TIMEZONE('America/Sao_Paulo', SYSDATE)) AS elapsed_time FROM ecommerce_transactions.transactions T),
            (SELECT DATEDIFF(minutes, MAX(T.created_at), CONVERT_TIMEZONE('America/Sao_Paulo', SYSDATE)) AS elapsed_time FROM api_transactions.transactions T)
        )
        """
        execute_query(connection, query)
        connection.close()
    except Exception as e:
        print(f"Error entering transaction KPI data: {e}")

def insert_kpi_storage_usage(**kwargs):
    """
    Inserts KPI data related to storage usage.

    This function calculates the percentage of used storage by comparing the average 
    storage utilization with the available capacity. The results are inserted into the 
    `data_warehouse.storage_usage` table for monitoring storage health.
    """
    try:
        connection = get_postgres_connection()
        query = """
        INSERT INTO data_warehouse.storage_usage (percentage_used)
        VALUES (
            SELECT (CAST(avg_used AS DECIMAL(10, 2)) / CAST(avg_capacity AS DECIMAL(10, 2)) * 100) AS percentage_used
            FROM (
                SELECT
                    AVG(used) AS avg_used,
                    AVG(capacity) AS avg_capacity
                FROM system_storage_capacity
            )
        )
        """
        execute_query(connection, query)
        connection.close()
    except Exception as e:
        print(f"Error entering storage usage KPI data: {e}")

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Defining the DAG
with DAG(
    "data_warehouse_kpis_insert",
    start_date=datetime(2024, 9, 6),
    max_active_runs=1,
    schedule_interval='0 * * * *',
    default_args=default_args,
    catchup=False,
) as dag:

    # Defining tasks
    insert_transaction_kpi = PythonOperator(
        task_id='insert_kpi_transaction_data',
        python_callable=insert_kpi_transaction_data,
    )

    insert_storage_kpi = PythonOperator(
        task_id='insert_kpi_storage_usage',
        python_callable=insert_kpi_storage_usage,
    )

    # Task dependencies
    insert_transaction_kpi >> insert_storage_kpi