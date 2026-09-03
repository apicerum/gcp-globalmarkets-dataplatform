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
    autodetect            = false
    source_format         = "NEWLINE_DELIMITED_JSON"
    ignore_unknown_values = true
    source_uris           = ["gs://${google_storage_bucket.raw_bucket.name}/crypto/binance/*.json"]

    schema = jsonencode([
      { name = "symbol", type = "STRING", mode = "NULLABLE" },
      { name = "priceChange", type = "STRING", mode = "NULLABLE" },
      { name = "priceChangePercent", type = "STRING", mode = "NULLABLE" },
      { name = "lastPrice", type = "STRING", mode = "NULLABLE" },
      { name = "volume", type = "STRING", mode = "NULLABLE" }
    ])
  }
}

# Tabla externa para CoinGecko (Estructura plana del endpoint /coins/markets)
resource "google_bigquery_table" "ext_coingecko_raw" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "ext_coingecko_raw"
  description = "Tabla externa que mapea los JSONs crudos de CoinGecko desde GCS Raw"

  external_data_configuration {
    autodetect            = false
    source_format         = "NEWLINE_DELIMITED_JSON"
    ignore_unknown_values = true
    source_uris           = ["gs://${google_storage_bucket.raw_bucket.name}/crypto/coingecko/*/*.json"]

    schema = jsonencode([
      { name = "id", type = "STRING", mode = "NULLABLE" },
      { name = "symbol", type = "STRING", mode = "NULLABLE" },
      { name = "name", type = "STRING", mode = "NULLABLE" },
      { name = "current_price", type = "FLOAT", mode = "NULLABLE" },
      { name = "total_volume", type = "FLOAT", mode = "NULLABLE" },
      { name = "price_change_percentage_24h", type = "FLOAT", mode = "NULLABLE" },
      { name = "market_cap", type = "FLOAT", mode = "NULLABLE" },
      { name = "last_updated", type = "STRING", mode = "NULLABLE" }
    ])
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
    query          = <<EOF
SELECT
  symbol,
  CAST(lastPrice AS NUMERIC) AS price_usd,
  CAST(volume AS NUMERIC) AS volume_24h,
  CAST(priceChangePercent AS NUMERIC) AS change_24h_percent,
  'binance' AS source,
  CURRENT_TIMESTAMP() AS processed_at
FROM
  `${var.project_id}.${google_bigquery_dataset.staging.dataset_id}.${google_bigquery_table.ext_binance_raw.table_id}`
EOF
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.ext_binance_raw]
}

# Vista normalizada para CoinGecko
resource "google_bigquery_table" "stg_coingecko_prices" {
  dataset_id  = google_bigquery_dataset.staging.dataset_id
  table_id    = "stg_coingecko_prices"
  description = "Vista procesada que estandariza las métricas de CoinGecko"

  view {
    query          = <<EOF
SELECT
  -- Concatenamos USDT al símbolo en mayúsculas (ej: 'btc' -> 'BTCUSDT') para homologar con Binance
  CONCAT(UPPER(symbol), 'USDT') AS symbol,
  CAST(current_price AS NUMERIC) AS price_usd,
  CAST(total_volume AS NUMERIC) AS volume_24h,
  CAST(price_change_percentage_24h AS NUMERIC) AS change_24h_percent,
  'coingecko' AS source,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', last_updated) AS updated_at,
  CURRENT_TIMESTAMP() AS processed_at
FROM
  `${var.project_id}.${google_bigquery_dataset.staging.dataset_id}.${google_bigquery_table.ext_coingecko_raw.table_id}`
WHERE
  current_price IS NOT NULL
EOF
    use_legacy_sql = false
  }

  depends_on = [google_bigquery_table.ext_coingecko_raw]
}