output "environment" {
  value = var.environment
}

output "local_server_url" {
  value = try(module.local_unity_catalog[0].server_url, null)
}

output "databricks_catalog_name" {
  value = try(module.databricks_lakehouse[0].catalog_name, null)
}
