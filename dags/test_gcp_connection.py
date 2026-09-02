from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

PROJECT_ID = "gcp-globalmarkets-dataplatform"
RAW_BUCKET = f"{PROJECT_ID}-raw"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='test_gcp_connection',
    default_args=default_args,
    schedule_interval=None,  # Disparo manual
    catchup=False,
    tags=['test', 'gcp'],
) as dag:

    # 1. Probar acceso a GCS (Bronze Layer)
    test_gcs = GCSListObjectsOperator(
        task_id='test_gcs_connection',
        bucket=RAW_BUCKET,
        gcp_conn_id='google_cloud_default',
    )

    # 2. Probar acceso a BigQuery (Silver Layer)
    test_bigquery = BigQueryExecuteQueryOperator(
        task_id='test_bigquery_connection',
        sql='SELECT 1 AS test_col',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    test_gcs >> test_bigquery