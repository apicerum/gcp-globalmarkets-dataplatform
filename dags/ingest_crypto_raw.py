import json
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = "gcp-globalmarkets-dataplatform"
BUCKET_NAME = f"{PROJECT_ID}-raw"


def extract_and_upload_binance(**kwargs):
    """Extrae tickers de Binance API y sube el archivo JSON a GCS Raw."""
    url = "https://api.binance.com/api/v3/ticker/24hr"
    params = {"symbols": '["BTCUSDT","ETHUSDT","SOLUSDT"]'}

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Formatear ruta en GCS: crypto/binance/YYYY/MM/DD/execution_time.json
    execution_date = kwargs["ds"]
    execution_ts = kwargs["ts_nodash"]
    gcs_object_path = f"crypto/binance/{execution_date.replace('-', '/')}/binance_{execution_ts}.json"

    # Subir a GCS usando el GCSHook
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_object_path,
        data=json.dumps(data, indent=2),
        mime_type="application/json",
    )
    print(f"Uploaded Binance data to gs://{BUCKET_NAME}/{gcs_object_path}")


def extract_and_upload_coingecko(**kwargs):
    """Extrae datos de CoinGecko API y sube el archivo JSON a GCS Raw."""
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
        "include_last_updated_at": "true",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Formatear ruta en GCS: crypto/coingecko/YYYY/MM/DD/execution_time.json
    execution_date = kwargs["ds"]
    execution_ts = kwargs["ts_nodash"]
    gcs_object_path = f"crypto/coingecko/{execution_date.replace('-', '/')}/coingecko_{execution_ts}.json"

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_object_path,
        data=json.dumps(data, indent=2),
        mime_type="application/json",
    )
    print(
        f"Uploaded CoinGecko data to gs://{BUCKET_NAME}/{gcs_object_path}"
    )


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
}

with DAG(
    dag_id="ingest_crypto_raw",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["bronze", "ingestion", "crypto"],
) as dag:

    ingest_binance = PythonOperator(
        task_id="extract_binance_to_gcs",
        python_callable=extract_and_upload_binance,
    )

    ingest_coingecko = PythonOperator(
        task_id="extract_coingecko_to_gcs",
        python_callable=extract_and_upload_coingecko,
    )

    [ingest_binance, ingest_coingecko]