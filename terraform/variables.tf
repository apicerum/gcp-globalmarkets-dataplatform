variable "project_id" {
  type        = string
  description = "ID del proyecto en Google Cloud"
  default     = "gcp-globalmarkets-dataplatform"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Región/Ubicación de los recursos de BigQuery"
}