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


def new_rows_by_key(
    incoming: DataFrame, existing: "DataFrame | None", key_col: str
) -> DataFrame:
    """Rows of incoming (one per key_col) whose key_col is not in existing."""
    incoming = incoming.dropDuplicates([key_col])
    if existing is None:
        return incoming
    return incoming.join(existing.select(key_col), key_col, "left_anti")


def _latest_by_load_date(df: DataFrame, key_col: str) -> DataFrame:
    w = Window.partitionBy(key_col).orderBy(
        F.col("load_date").desc(), F.col("hash_diff").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def changed_sat_rows(
    incoming: DataFrame, existing: "DataFrame | None", key_col: str
) -> DataFrame:
    """Rows of incoming (one per key_col) whose hash_diff differs from that
    key's latest hash_diff in existing, plus keys absent from existing.
    """
    incoming = incoming.dropDuplicates([key_col])
    if existing is None:
        return incoming
    latest = _latest_by_load_date(existing, key_col).select(
        key_col, F.col("hash_diff").alias("_cur_hd")
    )
    return (
        incoming.join(latest, key_col, "left")
        .where(F.col("_cur_hd").isNull() | (F.col("hash_diff") != F.col("_cur_hd")))
        .drop("_cur_hd")
    )


def current_sat(sat: DataFrame, key_col: str) -> DataFrame:
    """One row per key_col - the greatest load_date (ties broken by hash_diff)."""
    return _latest_by_load_date(sat, key_col)


# (hub table, bronze table, business key, hash-key column)
HUBS = [
    ("hub_user", "users", "user_id", "user_hk"),
    ("hub_account", "accounts", "account_id", "account_hk"),
    ("hub_transaction", "transactions", "transaction_id", "transaction_hk"),
    ("hub_savings_goal", "savings_goals", "goal_id", "savings_goal_hk"),
]

# (link table, bronze table, [(business key, its hub-key column), ...] in
# hash order, link hash-key column). The first pair's business key is the
# child entity's PK - used to dedupe the bronze rows.
LINKS = [
    ("link_account_user", "accounts",
     [("account_id", "account_hk"), ("user_id", "user_hk")], "account_user_hk"),
    ("link_transaction_account", "transactions",
     [("transaction_id", "transaction_hk"), ("account_id", "account_hk")],
     "transaction_account_hk"),
    ("link_savings_goal_user", "savings_goals",
     [("goal_id", "savings_goal_hk"), ("user_id", "user_hk")], "savings_goal_user_hk"),
]

# (sat table, bronze table, business key, hub-key column, [attribute cols],
# {attribute col: cast type}). Attributes not in the cast map land as
# bronze inferred them.
SATS = [
    ("sat_user_details", "users", "user_id", "user_hk",
     ["date_of_birth", "signup_date"],
     {"date_of_birth": "date", "signup_date": "date"}),
    ("sat_account_details", "accounts", "account_id", "account_hk",
     ["account_type", "account_number", "opened_date"],
     {"opened_date": "date"}),
    ("sat_transaction_details", "transactions", "transaction_id", "transaction_hk",
     ["transaction_type", "amount", "currency", "transaction_ts"],
     {"amount": "decimal(10,2)", "transaction_ts": "timestamp"}),
    ("sat_savings_goal_details", "savings_goals", "goal_id", "savings_goal_hk",
     ["goal_name", "target_amount", "current_amount", "created_date", "target_date"],
     {"target_amount": "decimal(10,2)", "current_amount": "decimal(10,2)",
      "created_date": "date", "target_date": "date"}),
]


def _write(spark, token, delta: DataFrame, name: str, first_load: bool) -> int:
    n = delta.count()
    if first_load:
        write_delta_table(token, delta, "vault", name, mode="overwrite")
    elif n > 0:
        write_delta_table(token, delta, "vault", name, mode="append")
    total = spark.table(f"{CATALOG_NAME}.vault.{name}").count()
    print(f"vault.{name}: +{n} rows ({total} total)")
    return n


if __name__ == "__main__":
    spark = get_spark()
    token = load_uc_token()
    LOAD_DATE = datetime.now()

    def _load_date_col():
        return F.lit(LOAD_DATE).cast("timestamp")

    # --- reference tables (closed lists, plain overwrite) ---
    write_delta_table(token, build_account_types(spark), "vault", "ref_account_type")
    write_delta_table(token, build_transaction_types(spark), "vault", "ref_transaction_type")
    print("vault.ref_account_type / vault.ref_transaction_type: seeded")

    # --- hubs ---
    for hub, btbl, bk, hk in HUBS:
        b = dedupe_latest(bronze_table(spark, btbl), [bk])
        incoming = add_hash_key(b, [bk], hk).select(
            hk, bk,
            _load_date_col().alias("load_date"),
            F.lit(f"bronze.{btbl}").alias("record_source"),
        )
        existing = vault_table(spark, token, hub)
        _write(spark, token, new_rows_by_key(incoming, existing, hk), hub, existing is None)

    # --- links ---
    for link, btbl, pairs, link_hk in LINKS:
        child_bk = pairs[0][0]
        b = dedupe_latest(bronze_table(spark, btbl), [child_bk])
        for bk, hk in pairs:
            b = add_hash_key(b, [bk], hk)
        b = add_hash_key(b, [pair[0] for pair in pairs], link_hk)
        incoming = b.select(
            link_hk, *[hk for _, hk in pairs],
            _load_date_col().alias("load_date"),
            F.lit(f"bronze.{btbl}").alias("record_source"),
        )
        existing = vault_table(spark, token, link)
        _write(spark, token, new_rows_by_key(incoming, existing, link_hk), link, existing is None)

    # --- satellites ---
    for sat, btbl, bk, hk, attrs, casts in SATS:
        b = dedupe_latest(bronze_table(spark, btbl), [bk])
        b = add_hash_key(b, [bk], hk)
        for c, t in casts.items():
            b = b.withColumn(c, F.col(c).cast(t))
        b = add_hash_diff(b, attrs)
        incoming = b.select(
            hk, _load_date_col().alias("load_date"), "hash_diff",
            F.lit(f"bronze.{btbl}").alias("record_source"), *attrs,
        )
        existing = vault_table(spark, token, sat)
        _write(spark, token, changed_sat_rows(incoming, existing, hk), sat, existing is None)
