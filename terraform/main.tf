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
# Habilitar la API de IAM 
resource "google_project_service" "iam_api" {
  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

# Service Account para Airflow con dependencia de la API
resource "google_service_account" "airflow_sa" {
  account_id   = "airflow-orchestrator"
  display_name = "Airflow Orchestrator Service Account"
  depends_on   = [google_project_service.iam_api]
}

# Permisos para GCS
resource "google_storage_bucket_iam_member" "gcs_admin" {
  bucket     = google_storage_bucket.raw_bucket.name
  role       = "roles/storage.objectAdmin"
  member     = "serviceAccount:${google_service_account.airflow_sa.email}"
  depends_on = [google_project_service.iam_api]
}

# Permisos para BigQuery
resource "google_project_iam_member" "bq_editor" {
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow_sa.email}"
  depends_on = [google_project_service.iam_api]
}

resource "google_project_iam_member" "bq_job_user" {
  project    = var.project_id
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${google_service_account.airflow_sa.email}"
  depends_on = [google_project_service.iam_api]
}