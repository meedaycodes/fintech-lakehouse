# Illustrative production target: Databricks as the managed Unity Catalog
# + warehouse layer this project's local OSS setup stands in for. NOT
# applied as part of this project's actual working environment - it
# requires a real Databricks workspace (DATABRICKS_HOST/DATABRICKS_TOKEN)
# with an existing metastore assignment and cloud storage, none of which
# exist in this project. Kept here as the documented migration target.
#
# The catalog/schema/grant shape is a direct port of iam/access.yaml and
# manage_access.py's model onto Databricks' native Terraform provider.
# Note the privilege naming actually differs between the two systems -
# OSS Unity Catalog's REST API takes space-separated strings ("USE
# SCHEMA", "CREATE TABLE" - see Privileges.java upstream), while
# Databricks' own API/Terraform provider uses underscore-separated
# enum values ("USE_SCHEMA", "CREATE_TABLE"). Getting this wrong doesn't
# fail loudly - it just silently grants nothing, so this module hardcodes
# the correct Databricks spelling rather than reusing iam/access.yaml's
# strings directly.
#
# Databricks ships this grant reconciliation as a first-class provider
# resource - a hosted deployment doesn't need a custom REST-based script
# like manage_access.py at all; that script exists specifically to cover
# the gap in the self-hosted OSS server's tooling.

terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

resource "databricks_catalog" "chip_lakehouse" {
  name         = var.catalog_name
  comment      = "chip fintech lakehouse catalog"
  storage_root = var.storage_root
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.chip_lakehouse.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.chip_lakehouse.name
  name         = "silver"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.chip_lakehouse.name
  name         = "gold"
}

resource "databricks_schema" "ml" {
  catalog_name = databricks_catalog.chip_lakehouse.name
  name         = "ml"
}

# USE CATALOG for both roles - matches catalog_privileges in
# iam/access.yaml for both users.
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.chip_lakehouse.name

  grant {
    principal  = var.data_engineer_email
    privileges = ["USE_CATALOG"]
  }
  grant {
    principal  = var.data_analyst_email
    privileges = ["USE_CATALOG"]
  }
}

# Full CRUD for data-engineer only - matches iam/access.yaml: analysts
# get no bronze access at all, on purpose.
resource "databricks_grants" "bronze" {
  schema = "${databricks_catalog.chip_lakehouse.name}.${databricks_schema.bronze.name}"

  grant {
    principal  = var.data_engineer_email
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

# Full CRUD for data-engineer, read-only for data-analyst - matches
# iam/access.yaml exactly.
resource "databricks_grants" "silver" {
  schema = "${databricks_catalog.chip_lakehouse.name}.${databricks_schema.silver.name}"

  grant {
    principal  = var.data_engineer_email
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = var.data_analyst_email
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.chip_lakehouse.name}.${databricks_schema.gold.name}"

  grant {
    principal  = var.data_engineer_email
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
  grant {
    principal  = var.data_analyst_email
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}

# Full CRUD for data-engineer only - iam/access.yaml never grants
# data-analyst anything on ml either.
resource "databricks_grants" "ml" {
  schema = "${databricks_catalog.chip_lakehouse.name}.${databricks_schema.ml.name}"

  grant {
    principal  = var.data_engineer_email
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
  }
}

# The warehouse layer: a Databricks SQL Warehouse for analyst/BI query
# compute, separate from the cluster(s) pipeline jobs would run on.
resource "databricks_sql_endpoint" "warehouse" {
  name             = "chip-lakehouse-warehouse"
  cluster_size     = var.warehouse_size
  auto_stop_mins   = 30
  enable_serverless_compute = true
}
