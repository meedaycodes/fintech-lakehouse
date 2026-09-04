"""Ingests the raw synthetic CSVs (data_gen/generate_data.py output) into
the bronze schema of the chip_lakehouse Unity Catalog, as Delta tables.

Bronze keeps data close to its source shape (schema-inferred, tagged with
an ingestion timestamp) - proper typing/cleaning is silver_transform.py's
job, not this one.

Run after data_gen/generate_data.py and after src/spark_session.py has
bootstrapped the catalog/schemas:
    python3 src/bronze_ingest.py

Implementation note - why this doesn't just call df.write.saveAsTable():
unitycatalog-spark 0.2.1's TableCatalog.createTable path is broken in this
setup - when Delta auto-wraps itself around UCSingleCatalog (the default),
its own schema-existence check resolves against Spark's built-in session
catalog instead of the UC-backed one ([SCHEMA_NOT_FOUND] even though the
schema exists); with that wrapping disabled, the raw UC connector's own
type-serialization has real gaps (rejects DATE outright, sends malformed
type_json for LONG). All of that is scoped to *writing new tables* through
the connector - reads through UCSingleCatalog work correctly regardless.
So tables here are written as plain Delta files on disk, then registered
as EXTERNAL tables directly through UC's REST API (the same reliable
pattern ensure_uc_catalog() in spark_session.py already uses), building
correct field-level type_json ourselves via Spark's own field.jsonValue().
Everything downstream (silver/gold, manage_access.py's grants) reads these
tables through the catalog exactly as if they'd been created any other
way - only the write registration route is different.
"""
from pathlib import Path

from pyspark.sql import functions as F

from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import write_delta_table

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# (csv filename, bronze table name)
SOURCES = [
    ("users.csv", "users"),
    ("accounts.csv", "accounts"),
    ("transactions.csv", "transactions"),
    ("savings_goals.csv", "savings_goals"),
]


def ingest_table(spark, token: str, csv_filename: str, table_name: str) -> None:
    csv_path = DATA_DIR / csv_filename
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not found - run data_gen/generate_data.py first")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(csv_path))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    row_count = df.count()

    location = write_delta_table(token, df, "bronze", table_name, mode="overwrite")
    print(f"ingested {row_count} rows -> {CATALOG_NAME}.bronze.{table_name} ({location})")


def ingest_incremental_transactions(spark, token) -> None:
    """Appends data/raw/transactions_incremental.csv into the existing
    bronze.transactions table, instead of reloading everything. The other
    three bronze tables are untouched.
    """
    table_name = "transactions"
    csv_path = DATA_DIR / "transactions_incremental.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} not found - run data_gen/generate_incremental_transactions.py first"
        )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(csv_path))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    row_count = df.count()

    location = write_delta_table(token, df, "bronze", table_name, mode="append")
    print(f"appended {row_count} new rows -> {CATALOG_NAME}.bronze.{table_name} ({location})")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Append data/raw/transactions_incremental.csv into bronze.transactions "
        "instead of a full reload of all four tables",
    )
    args = parser.parse_args()

    spark = get_spark()
    token = load_uc_token()

    if args.incremental:
        ingest_incremental_transactions(spark, token)
    else:
        for csv_filename, table_name in SOURCES:
            ingest_table(spark, token, csv_filename, table_name)
