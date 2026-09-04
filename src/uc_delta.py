"""Shared helper for writing Delta tables into Unity Catalog.

unitycatalog-spark 0.2.1's TableCatalog.createTable path is broken for
tables created through Spark's own DataFrameWriter/CTAS APIs - see
bronze_ingest.py's module docstring for the full investigation. Every
layer that creates a new table works around it the same way: write plain
Delta files to disk, then register the table directly through UC's REST
API. Reads through Spark's UCSingleCatalog are unaffected either way.
"""
import json
from pathlib import Path

import requests
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField

from spark_session import CATALOG_NAME, UC_URI

LAKEHOUSE_DIR = Path(__file__).resolve().parent.parent / "data" / "lakehouse"

# Maps Spark's DataType.simpleString() to UC's ColumnTypeName enum. Extend
# this if a future column infers/casts to a type not listed here - the
# alternative is a confusing 400 from the UC API, not a clean local error.
# Decimal is handled separately below since it carries precision/scale.
#
# Note simpleString() != typeName() for the integer family - e.g. LongType
# is "bigint" here, not "long" ("long" is typeName(), used in JSON/DDL
# elsewhere). Verified directly against pyspark.sql.types rather than
# assumed, after this exact mismatch broke gold_marts.py's F.count() output.
TYPE_NAME_MAP = {
    "string": "STRING",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "boolean": "BOOLEAN",
    "tinyint": "BYTE",
    "smallint": "SHORT",
    "int": "INT",
    "bigint": "LONG",
    "float": "FLOAT",
    "double": "DOUBLE",
}


def _uc_column(position: int, field: StructField) -> dict:
    simple = field.dataType.simpleString()
    column = {
        "name": field.name,
        "type_text": simple,
        "type_json": json.dumps(field.jsonValue()),
        "position": position,
        "nullable": field.nullable,
    }
    if simple.startswith("decimal"):
        column["type_name"] = "DECIMAL"
        column["type_precision"] = field.dataType.precision
        column["type_scale"] = field.dataType.scale
    elif simple in TYPE_NAME_MAP:
        column["type_name"] = TYPE_NAME_MAP[simple]
    else:
        raise ValueError(
            f"no UC type mapping for Spark type '{simple}' (column '{field.name}') - "
            "add it to TYPE_NAME_MAP"
        )
    return column


def uc_columns(fields: list[StructField]) -> list[dict]:
    return [_uc_column(position, field) for position, field in enumerate(fields)]


def table_exists(token: str, schema: str, table_name: str) -> bool:
    resp = requests.get(
        f"{UC_URI}/api/2.1/unity-catalog/tables/{CATALOG_NAME}.{schema}.{table_name}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return resp.status_code == 200


def register_uc_table(token: str, schema: str, table_name: str, location: str, fields: list[StructField]) -> None:
    """Registers the Delta files at `location` as a UC external table, if
    not already registered. Overwriting the files on a re-run doesn't need
    re-registration - only the first ingest for a given table does.
    """
    if table_exists(token, schema, table_name):
        return
    body = {
        "name": table_name,
        "catalog_name": CATALOG_NAME,
        "schema_name": schema,
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "columns": uc_columns(fields),
        "storage_location": location,
    }
    resp = requests.post(
        f"{UC_URI}/api/2.1/unity-catalog/tables",
        headers={"Authorization": f"Bearer {token}"},
        json=body,
    )
    resp.raise_for_status()


def write_delta_table(token: str, df: DataFrame, schema: str, table_name: str, mode: str = "overwrite") -> str:
    """Writes df as Delta files under data/lakehouse/<schema>/<table_name>
    and registers it in UC if not already registered. Returns the location.
    """
    location = f"file://{(LAKEHOUSE_DIR / schema / table_name).resolve()}"
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    writer.save(location)
    register_uc_table(token, schema, table_name, location, df.schema.fields)
    return location
