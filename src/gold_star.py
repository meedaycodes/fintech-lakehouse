"""Builds a conformed Kimball star schema in the gold schema, alongside
the wide marts in gold_marts.py.

Run after src/silver_transform.py:
    python3 src/gold_star.py

Tables (all in the gold schema, dim_/fct_ prefixed):
  - dim_date                       static, overwrite
  - dim_transaction_type           static, overwrite (from silver.transaction_types)
  - dim_customer                   SCD2, persisted, Delta MERGE
  - dim_account                    SCD2, persisted, Delta MERGE
  - fct_transaction                transaction grain, overwrite
  - fct_account_monthly_snapshot   periodic snapshot, overwrite

The two SCD2 dimensions are the only tables in this project that persist
across pipeline runs and carry a pipeline-generated integer surrogate
key (<entity>_key); the source UUID stays alongside as the business key.
Everything else here is a full overwrite, same as gold_marts.py.

See docs/superpowers/specs/2026-09-06-kimball-star-schema-design.md.
"""
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import (
    LAKEHOUSE_DIR,
    get_uc_table,
    register_uc_table,
    write_delta_table,
)


def silver_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.silver.{name}")


def gold_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.gold.{name}")


def build_dim_date(spark, start: date, end: date) -> DataFrame:
    """One row per calendar day in [start, end] inclusive. Callers should
    pass month-snapped bounds (trunc-to-month / last_day) so every
    month-end date_key the snapshot fact emits is covered.
    """
    # Spark's dayofweek() is 1=Sunday..7=Saturday. Remap to ISO 1=Monday..7=Sunday.
    iso_dow = ((F.dayofweek(F.col("date")) + 5) % 7) + 1

    days = spark.createDataFrame([(start, end)], ["start", "end"]).select(
        F.explode(
            F.sequence(F.col("start"), F.col("end"), F.expr("interval 1 day"))
        ).alias("date")
    )
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        iso_dow.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        (iso_dow >= 6).alias("is_weekend"),
        (F.col("date") == F.last_day("date")).alias("is_month_end"),
    )


def build_dim_transaction_type(silver_transaction_types: DataFrame) -> DataFrame:
    return silver_transaction_types.select(
        F.col("transaction_type_id").alias("transaction_type_key"),
        "type_name",
        "direction",
        "description",
    )


def silver_date_bounds(silver_users, silver_accounts, silver_transactions) -> tuple[date, date]:
    dates = (
        silver_transactions.select(F.to_date("transaction_ts").alias("d"))
        .union(silver_accounts.select(F.col("opened_date").alias("d")))
        .union(silver_users.select(F.col("signup_date").alias("d")))
    )
    row = dates.agg(
        F.trunc(F.min("d"), "month").alias("start"),
        F.last_day(F.max("d")).alias("end"),
    ).first()
    return row["start"], row["end"]
