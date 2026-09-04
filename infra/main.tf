terraform {
  required_version = ">= 1.5"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

locals {
  # docker/etc/conf/ is a sibling of this infra/ directory. Docker's
  # provider requires absolute bind-mount paths - path.root alone is
  # relative ("."), so it has to go through abspath().
  server_properties_path = coalesce(var.server_properties_path, abspath("${path.root}/../docker/etc/conf/server.properties"))
  token_file_path         = coalesce(var.token_file_path, abspath("${path.root}/../docker/etc/conf/token.txt"))
}

# --- Local Docker deployment ----------------------------------------------
# Mirrors docker/docker-compose.yml. This is the module that's actually
# apply-able against the Docker daemon already running on this machine.
# See modules/local_docker/main.tf for the full topology and the caveat
# about not running this alongside `docker compose up` at the same time.

provider "docker" {}

module "local_unity_catalog" {
  count  = var.environment == "local" ? 1 : 0
  source = "./modules/local_docker"

  server_properties_path = local.server_properties_path
  token_file_path         = local.token_file_path
}

# --- Databricks production target -----------------------------------------
# Illustrative: Databricks as the managed warehouse/Unity Catalog layer
# this local OSS setup stands in for. See modules/databricks/main.tf for
# why this isn't applied as part of this project's actual working setup.

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

module "databricks_lakehouse" {
  count  = var.environment == "production" ? 1 : 0
  source = "./modules/databricks"

  catalog_name = var.catalog_name
  storage_root = var.databricks_storage_root
}
