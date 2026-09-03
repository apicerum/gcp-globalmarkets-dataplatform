import json
from datetime import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT_ID = "gcp-globalmarkets-dataplatform"
BUCKET_NAME = f"{PROJECT_ID}-raw"


def extract_and_upload_binance(**kwargs):
    """Extrae TODOS los tickers de Binance API y sube el archivo NDJSON a GCS Raw."""
    # Al no pasar el parámetro 'symbols', la API retorna todos los pares activos
    url = "https://api.binance.com/api/v3/ticker/24hr"

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Formatear a NDJSON (un objeto JSON por línea)
    ndjson_data = "\n".join(json.dumps(item) for item in data) + "\n"

    execution_date = kwargs["ds"]
    execution_ts = kwargs["ts_nodash"]
    gcs_object_path = f"crypto/binance/{execution_date.replace('-', '/')}/binance_{execution_ts}.json"

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_object_path,
        data=ndjson_data,
        mime_type="application/x-ndjson",
    )
    print(f"Uploaded Binance full market data ({len(data)} records) to gs://{BUCKET_NAME}/{gcs_object_path}")


def extract_and_upload_coingecko(**kwargs):
    """Extrae el Top 250 de monedas de CoinGecko por capitalización (endpoint /coins/markets)."""
    # /coins/markets entrega precio, volumen 24h, % cambio 24h, ath, market cap, etc.
    # El tier gratuito permite hasta 250 monedas por página.
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    headers = {"accept": "application/json"}
    
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    # La API ya devuelve una lista de diccionarios planos. Convertimos directamente a NDJSON.
    ndjson_data = "\n".join(json.dumps(item) for item in data) + "\n"

    execution_date = kwargs["ds"]
    execution_ts = kwargs["ts_nodash"]
    gcs_object_path = f"crypto/coingecko/{execution_date.replace('-', '/')}/coingecko_{execution_ts}.json"

    hook = GCSHook(gcp_conn_id="google_cloud_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_object_path,
        data=ndjson_data,
        mime_type="application/x-ndjson",
    )
    print(f"Uploaded CoinGecko market data ({len(data)} records) to gs://{BUCKET_NAME}/{gcs_object_path}")


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