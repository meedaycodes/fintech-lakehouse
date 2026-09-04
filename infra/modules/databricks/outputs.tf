output "catalog_name" {
  value = databricks_catalog.chip_lakehouse.name
}

output "warehouse_id" {
  value = databricks_sql_endpoint.warehouse.id
}
