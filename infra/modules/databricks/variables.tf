variable "catalog_name" {
  description = "Must match spark_session.py's CATALOG_NAME for the two environments to represent the same logical lakehouse."
  type        = string
  default     = "chip_lakehouse"
}

variable "storage_root" {
  description = "Cloud storage path backing this catalog, e.g. \"s3://chip-lakehouse-prod/\" or \"abfss://...\". OSS UC's local setup uses plain local disk instead (see uc_delta.py) - Databricks UC requires a real cloud storage root."
  type        = string
}

variable "data_engineer_email" {
  description = "Must match the principal in iam/access.yaml for the two environments' grants to represent the same policy."
  type        = string
  default     = "data-engineer@chip-lakehouse.local"
}

variable "data_analyst_email" {
  description = "Must match the principal in iam/access.yaml for the two environments' grants to represent the same policy."
  type        = string
  default     = "data-analyst@chip-lakehouse.local"
}

variable "warehouse_size" {
  description = "Databricks SQL Warehouse cluster size (e.g. \"2X-Small\", \"Small\")."
  type        = string
  default     = "2X-Small"
}
