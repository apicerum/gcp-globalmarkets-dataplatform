output "raw_bucket_name" {
  value       = google_storage_bucket.raw_bucket.name
  description = "Nombre del bucket GCS Raw"
}

output "service_account_email" {
  value       = google_service_account.airflow_sa.email
  description = "Email de la Service Account creada para Airflow"
}