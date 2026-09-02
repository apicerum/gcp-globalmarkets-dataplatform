terraform {
  required_version = ">= 1.5.0"

  # Configuración del Backend Remoto en GCS   
  backend "gcs" {
    bucket = "gcp-globalmarkets-dataplatform-tfstate"
    prefix = "terraform/state"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# 1. Bucket GCS - Layer Raw / Bronze
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "raw_bucket" {
  name                        = "${var.project_id}-raw"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90 # Días para auto-archivar o limpiar datos antiguos si se requiere
    }
    action {
      type = "Delete"
    }
  }
}

# -----------------------------------------------------------------------------
# 2. Datasets de BigQuery (Silver & Gold Layers)
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "globalmarkets_staging"
  friendly_name              = "Global Markets Staging Layer (Silver)"
  description                = "Capa intermedia para limpieza y tipado de datos"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = "globalmarkets_analytics"
  friendly_name              = "Global Markets Analytics Layer (Gold)"
  description                = "Data Marts y agregaciones preparadas para consumo"
  location                   = var.region
  delete_contents_on_destroy = true
}

# -----------------------------------------------------------------------------
# 3. Service Account para Airflow
# -----------------------------------------------------------------------------
resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-orchestrator"
  display_name = "Airflow Orchestrator Service Account"
}

# Permisos para GCS (Lectura/Escritura en el bucket Raw)
resource "google_storage_bucket_iam_member" "gcs_admin" {
  bucket = google_storage_bucket.raw_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# Permisos para BigQuery (Crear/Modificar datos)
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# Permisos para BigQuery (Ejecutar Jobs/Queries)
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# -----------------------------------------------------------------------------
# 4. Tablas Externas en BigQuery (Capa Raw / Bronze)
# -----------------------------------------------------------------------------

# Tabla externa para Binance
resource "google_bigquery_table" "ext_binance_raw" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "ext_binance_raw"
  description = "Tabla externa que mapea los JSONs crudos de Binance desde GCS Raw"

  external_data_configuration {
    autodetect    = false
    source_format = "JSON"
    source_uris   = ["gs://${google_storage_bucket.raw_bucket.name}/crypto/binance/*/*/*/*.json"]

    schema = <<EOF
[
  { "name": "symbol", "type": "STRING", "mode": "NULLABLE" },
  { "name": "lastPrice", "type": "STRING", "mode": "NULLABLE" },
  { "name": "volume", "type": "STRING", "mode": "NULLABLE" },
  { "name": "priceChangePercent", "type": "STRING", "mode": "NULLABLE" }
]
EOF
  }
}

# Tabla externa para CoinGecko (usando tipo JSON de BQ para estructuras anidadas)
resource "google_bigquery_table" "ext_coingecko_raw" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "ext_coingecko_raw"
  description = "Tabla externa que mapea los JSONs anidados de CoinGecko desde GCS Raw"

  external_data_configuration {
    autodetect    = false
    source_format = "NEWLINE_DELIMITED_JSON"
    source_uris   = ["gs://${google_storage_bucket.raw_bucket.name}/crypto/coingecko/*/*/*/*.json"]

    schema = <<EOF
[
  { "name": "bitcoin", "type": "RECORD", "mode": "NULLABLE", "fields": [
      { "name": "usd", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_vol", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_change", "type": "FLOAT", "mode": "NULLABLE" }
    ]
  },
  { "name": "ethereum", "type": "RECORD", "mode": "NULLABLE", "fields": [
      { "name": "usd", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_vol", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_change", "type": "FLOAT", "mode": "NULLABLE" }
    ]
  },
  { "name": "solana", "type": "RECORD", "mode": "NULLABLE", "fields": [
      { "name": "usd", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_vol", "type": "FLOAT", "mode": "NULLABLE" },
      { "name": "usd_24h_change", "type": "FLOAT", "mode": "NULLABLE" }
    ]
  }
]
EOF
  }
}

# -----------------------------------------------------------------------------
# 5. Vistas de Transformación en BigQuery (Capa Staging / Silver)
# -----------------------------------------------------------------------------

# Vista normalizada para Binance
resource "google_bigquery_table" "stg_binance_prices" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "stg_binance_prices"
  description = "Vista procesada y tipada con métricas de Binance"

  view {
    sql            = <<EOF
SELECT
  symbol,
  CAST(lastPrice AS NUMERIC) AS price_usd,
  CAST(volume AS NUMERIC) AS volume_24h,
  CAST(priceChangePercent AS NUMERIC) AS change_24h_percent,
  'binance' AS source,
  CURRENT_TIMESTAMP() AS processed_at
FROM
  `${var.project_id}.${google_bigquery_dataset.staging_dataset.dataset_id}.${google_bigquery_table.ext_binance_raw.table_id}`
EOF
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.ext_binance_raw]
}

# Vista normalizada para CoinGecko (unificando las claves anidadas)
resource "google_bigquery_table" "stg_coingecko_prices" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "stg_coingecko_prices"
  description = "Vista procesada que desanida y estandariza los datos de CoinGecko"

  view {
    sql            = <<EOF
SELECT 'BTCUSDT' AS symbol, CAST(bitcoin.usd AS NUMERIC) AS price_usd, CAST(bitcoin.usd_24h_vol AS NUMERIC) AS volume_24h, CAST(bitcoin.usd_24h_change AS NUMERIC) AS change_24h_percent, 'coingecko' AS source, CURRENT_TIMESTAMP() AS processed_at FROM `${var.project_id}.${google_bigquery_dataset.staging_dataset.dataset_id}.${google_bigquery_table.ext_coingecko_raw.table_id}` WHERE bitcoin.usd IS NOT NULL
UNION ALL
SELECT 'ETHUSDT' AS symbol, CAST(ethereum.usd AS NUMERIC) AS price_usd, CAST(ethereum.usd_24h_vol AS NUMERIC) AS volume_24h, CAST(ethereum.usd_24h_change AS NUMERIC) AS change_24h_percent, 'coingecko' AS source, CURRENT_TIMESTAMP() AS processed_at FROM `${var.project_id}.${google_bigquery_dataset.staging_dataset.dataset_id}.${google_bigquery_table.ext_coingecko_raw.table_id}` WHERE ethereum.usd IS NOT NULL
UNION ALL
SELECT 'SOLUSDT' AS symbol, CAST(solana.usd AS NUMERIC) AS price_usd, CAST(solana.usd_24h_vol AS NUMERIC) AS volume_24h, CAST(solana.usd_24h_change AS NUMERIC) AS change_24h_percent, 'coingecko' AS source, CURRENT_TIMESTAMP() AS processed_at FROM `${var.project_id}.${google_bigquery_dataset.staging_dataset.dataset_id}.${google_bigquery_table.ext_coingecko_raw.table_id}` WHERE solana.usd IS NOT NULL
EOF
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.ext_coingecko_raw]
}