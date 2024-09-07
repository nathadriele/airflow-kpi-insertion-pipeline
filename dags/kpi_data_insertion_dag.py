from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "data_warehouse_conn_id"
SCHEMA = "business_metrics"

def insert_kpi_transaction_data():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=SCHEMA)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    
    query = """
    INSERT INTO data_warehouse.load_times (time_elapsed)
    VALUES (
        (SELECT DATEDIFF(minutes, MAX(T.created_at), SYSDATE) AS elapsed_time FROM business_metrics.transactions T),
        (SELECT DATEDIFF(minutes, MAX(T.created_at), CONVERT_TIMEZONE('America/Sao_Paulo', SYSDATE)) AS elapsed_time FROM ecommerce_transactions.transactions T),
        (SELECT DATEDIFF(minutes, MAX(T.created_at), CONVERT_TIMEZONE('America/Sao_Paulo', SYSDATE)) AS elapsed_time FROM api_transactions.transactions T)
    )
    """
    
    cursor.execute(query)
    cursor.close()
    connection.commit()
    connection.close()

def insert_kpi_storage_usage():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=SCHEMA)
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

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
    
    cursor.execute(query)
    cursor.close()
    connection.commit()
    connection.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "data_warehouse_kpis_insert",
    start_date=datetime(2024, 9, 6),
    schedule_interval='0 * * * *',
    default_args=default_args,
) as dag:

    insert_transaction_kpi = PythonOperator(
        task_id='insert_kpi_transaction_data',
        python_callable=insert_kpi_transaction_data,
    )

    insert_storage_kpi = PythonOperator(
        task_id='insert_kpi_storage_usage',
        python_callable=insert_kpi_storage_usage,
    )

    insert_transaction_kpi >> insert_storage_kpi
