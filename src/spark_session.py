from pathlib import Path

import requests
from pyspark.sql import SparkSession

UC_URI = "http://localhost:8080"
CATALOG_NAME = "chip_lakehouse"
# The UC docker container (re)generates this admin PAT on every boot and
# mirrors it to the host - see docker/docker-compose.yml.
UC_TOKEN_FILE = Path(__file__).resolve().parent.parent / "docker" / "etc" / "conf" / "token.txt"


def load_uc_token(path: Path = UC_TOKEN_FILE) -> str:
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found - is the Unity Catalog docker container running? "
            "(cd docker && docker compose up -d)"
        )
    return path.read_text().strip()


def ensure_uc_catalog(name: str = CATALOG_NAME, uri: str = UC_URI, token: str | None = None) -> None:
    """Create the Unity Catalog catalog if it doesn't exist yet.

    Spark SQL has no `CREATE CATALOG` statement (that's a Databricks-only
    extension), so the catalog has to be created through UC's own REST API
    before Spark can reference it.
    """
    token = token if token is not None else load_uc_token()
    headers = {"Authorization": f"Bearer {token}"}
    get_resp = requests.get(f"{uri}/api/2.1/unity-catalog/catalogs/{name}", headers=headers)
    if get_resp.status_code == 200:
        return
    create_resp = requests.post(
        f"{uri}/api/2.1/unity-catalog/catalogs",
        headers=headers,
        json={"name": name, "comment": "chip fintech lakehouse catalog"},
    )
    create_resp.raise_for_status()


def get_spark(app_name = "chip-lakehouse"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,io.unitycatalog:unitycatalog-spark_2.12:0.2.1")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        # Required for any Delta write, even a plain path-based .save() that
        # never touches the UC catalog - Delta checks that the *session*
        # catalog is Delta-aware regardless of which catalog you're using.
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # UCSingleCatalog treats the Spark catalog config name as the UC
        # catalog name, so this must match CATALOG_NAME exactly.
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "io.unitycatalog.spark.UCSingleCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", UC_URI)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.token", load_uc_token())
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        .getOrCreate()
    )

if __name__ == "__main__":
    ensure_uc_catalog()
    spark = get_spark()
    for schema in ["bronze", "silver", "gold", "ml"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")