"""Builds an insert-only Data Vault 2.0 raw layer in the vault schema,
as a parallel branch off bronze (not a stage between bronze and silver).

Run after src/bronze_ingest.py; no ordering constraint with
silver_transform.py / gold_marts.py / gold_star.py:
    python3 src/vault_load.py

Tables (all in the vault schema):
  hub_user / hub_account / hub_transaction / hub_savings_goal
      one row per distinct business key ever seen
  link_account_user / link_transaction_account / link_savings_goal_user
      one row per distinct relationship ever seen
  sat_user_details / sat_account_details / sat_transaction_details
  sat_savings_goal_details
      insert-only attribute history; a new row only when hash_diff changes
  ref_account_type / ref_transaction_type
      the same closed enumerations silver holds, seeded from
      silver_transform.py's constants

Why this layer exists: bronze is full-overwrite, so a corrected or
withdrawn source row destroys the prior value; silver's dedupe collapses
history to one row per key. The vault keeps every version, every
relationship, and the source of each, and is never updated in place.
"current" is derived at query time (current_sat). verify_e2e.py
reconstructs current-state entities from the vault and asserts they match
silver, proving the raw layer loses nothing.

See docs/superpowers/specs/2026-09-06-data-vault-raw-layer-design.md.
"""
from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_transform import (
    build_account_types,
    build_transaction_types,
    dedupe_latest,
)
from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import get_uc_table, write_delta_table


def bronze_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.bronze.{name}")


def vault_table(spark, token: str, name: str) -> "DataFrame | None":
    """The registered vault table, or None if it has never been loaded."""
    if get_uc_table(token, "vault", name) is None:
        return None
    return spark.table(f"{CATALOG_NAME}.vault.{name}")


def _hash(*cols):
    """sha2-256 hex of the given Columns, joined with '||', nulls -> '∅'.
    Matches gold_star.py's _row_hash convention.
    """
    parts = [F.coalesce(c.cast("string"), F.lit("∅")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def add_hash_key(df: DataFrame, bk_cols: list, hk_col: str) -> DataFrame:
    """Adds hk_col = _hash of the trimmed bk_cols, in the given order."""
    return df.withColumn(
        hk_col, _hash(*[F.trim(F.col(c).cast("string")) for c in bk_cols])
    )


def add_hash_diff(df: DataFrame, attr_cols: list, out_col: str = "hash_diff") -> DataFrame:
    """Adds out_col = _hash of attr_cols in sorted-by-name order (not trimmed)."""
    return df.withColumn(out_col, _hash(*[F.col(c) for c in sorted(attr_cols)]))
