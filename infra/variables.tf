variable "environment" {
  description = "Which infra to provision: \"local\" (Docker-managed OSS Unity Catalog, mirrors docker/docker-compose.yml - actually apply-able against this machine's Docker daemon) or \"production\" (illustrative Databricks target - see infra/modules/databricks for why it isn't applied as part of this project's real setup)."
  type        = string
  default     = "local"

  validation {
    condition     = contains(["local", "production"], var.environment)
    error_message = "environment must be \"local\" or \"production\"."
  }
}

variable "catalog_name" {
  type    = string
  default = "chip_lakehouse"
}

# --- local_docker module inputs ------------------------------------------
# Left nullable with no default: computed in main.tf relative to this
# directory (docker/etc/conf/... is a sibling of infra/) unless overridden.

variable "server_properties_path" {
  type     = string
  default  = null
  nullable = true
}

variable "token_file_path" {
  type     = string
  default  = null
  nullable = true
}

# --- databricks module inputs (only meaningful when environment = "production") ---

variable "databricks_host" {
  description = "Workspace URL, e.g. https://<workspace>.cloud.databricks.com. Read from DATABRICKS_HOST env var if unset."
  type        = string
  default     = null
  nullable    = true
}

variable "databricks_token" {
  description = "Personal access token. Read from DATABRICKS_TOKEN env var if unset - never commit a real value here."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "databricks_storage_root" {
  description = "Cloud storage root for the Databricks-managed catalog, e.g. s3://chip-lakehouse-prod/. Required only when environment = \"production\"."
  type        = string
  default     = ""
}
