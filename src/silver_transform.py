"""Transforms bronze entities into cleaned, deduplicated, type-conformed,
referentially-validated, PII-reduced silver tables.

Run after src/bronze_ingest.py:
    python3 src/silver_transform.py

Design:
  - Same four entities as bronze, one-to-one - no joins/aggregation into a
    wide or dimensional shape here. That's gold_marts.py's job; silver's
    contract is "clean, deduped, typed, referentially sound, PII-safe."
  - Dedup: keep the latest row per primary key by _ingested_at, via a
    window function rather than a bare dropDuplicates() - so a row that
    arrives twice (e.g. via both a full reload and an incremental append)
    doesn't silently produce two "true" versions.
  - Type conformity: money columns become DecimalType(10,2) (not double -
    floats carry rounding error that's unacceptable for financial
    figures), dates/timestamps are cast explicitly rather than trusted
    from bronze's loose CSV-inferred types, and category columns are
    validated against their known enum - anything else is quarantined,
    not silently kept or silently dropped.
  - Referential sanity: every FK is inner-joined (via left_semi/left_anti)
    against its already-cleaned silver parent, rather than trusted from
    bronze. Orphans go to data/quarantine/, not /dev/null.
  - PII: direct identifiers (full_name, email, address) are dropped from
    silver.users entirely - user_id (meaningless without the bronze
    lookup table) remains as the join key, which is what pseudonymizes
    the record. date_of_birth is generalized to a 10-year age band.
    account_number is masked to its last 4 digits. See docs/governance.md
    for the full PII inventory and tier definitions this follows.
"""
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import write_delta_table

QUARANTINE_DIR = Path(__file__).resolve().parent.parent / "data" / "quarantine"

ACCOUNT_TYPES = ["savings", "investment", "pension"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "roundup", "investment_contribution"]

# Inmon-style lookup tables (silver.account_types, silver.transaction_types).
# Hardcoded, not derived from data - these are small, closed, code-known
# enumerations. direction drives gold_marts.py's inflow/outflow
# classification; it must reproduce today's INFLOW_TRANSACTION_TYPES list
# exactly (deposit/roundup/investment_contribution = inflow).
ACCOUNT_TYPE_SEED = [
    (1, "savings", "cash", "Instant/easy-access cash savings account"),
    (2, "investment", "investment", "Stocks & shares investment account"),
    (3, "pension", "retirement", "Personal pension account"),
]

TRANSACTION_TYPE_SEED = [
    (1, "deposit", "inflow", "Manual deposit into the account"),
    (2, "withdrawal", "outflow", "Withdrawal out of the account"),
    (3, "roundup", "inflow", "Spare change swept in from a linked card purchase"),
    (4, "investment_contribution", "inflow", "Contribution into an investment sub-account"),
]


def build_account_types(spark) -> DataFrame:
    return spark.createDataFrame(
        ACCOUNT_TYPE_SEED, schema=["account_type_id", "type_name", "category", "description"]
    )


def build_transaction_types(spark) -> DataFrame:
    return spark.createDataFrame(
        TRANSACTION_TYPE_SEED, schema=["transaction_type_id", "type_name", "direction", "description"]
    )


def bronze_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.bronze.{name}")


def dedupe_latest(df: DataFrame, pk_cols: list) -> DataFrame:
    """Keeps one row per pk_cols - the most recently ingested one."""
    window = Window.partitionBy(*pk_cols).orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def quarantine(df: DataFrame, name: str) -> int:
    """Writes rows that failed validation to data/quarantine/<name> for
    inspection, instead of silently dropping them. Returns the row count.
    """
    count = df.count()
    if count > 0:
        path = str((QUARANTINE_DIR / name).resolve())
        df.write.mode("overwrite").option("header", "true").csv(path)
        print(f"  quarantined {count} row(s) -> {path}")
    return count


def transform_users(bronze_users: DataFrame) -> DataFrame:
    deduped = dedupe_latest(bronze_users, ["user_id"])

    age_years = F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
    band_start = (F.floor(age_years / 10) * 10).cast("int")

    return (
        deduped
        .withColumn("_band_start", band_start)
        .select(
            "user_id",
            # Direct identifiers (full_name, email, address) are dropped
            # here, not carried forward - see docs/governance.md. user_id
            # stays as the join key; it's what pseudonymizes the record.
            F.concat(F.col("_band_start"), F.lit("-"), F.col("_band_start") + 9).alias("age_band"),
            F.col("signup_date").cast("date"),
        )
    )


def transform_accounts(bronze_accounts: DataFrame, silver_users: DataFrame, account_types: DataFrame) -> DataFrame:
    deduped = dedupe_latest(bronze_accounts, ["account_id"])

    lookup = account_types.select("account_type_id", "type_name")
    bad_type = deduped.join(lookup, deduped.account_type == lookup.type_name, "left_anti")
    quarantine(bad_type, "accounts_bad_type")

    typed = (
        deduped.join(lookup, deduped.account_type == lookup.type_name, "inner")
        .select(
            "account_id",
            "user_id",
            "account_type_id",
            F.concat(F.lit("****"), F.substring(F.col("account_number"), -4, 4)).alias("account_number_masked"),
            F.col("opened_date").cast("date"),
        )
    )

    parent_keys = silver_users.select("user_id")
    quarantine(typed.join(parent_keys, "user_id", "left_anti"), "accounts_orphans")
    return typed.join(parent_keys, "user_id", "left_semi")


def transform_transactions(bronze_transactions: DataFrame, silver_accounts: DataFrame) -> DataFrame:
    deduped = dedupe_latest(bronze_transactions, ["transaction_id"])

    valid_type = deduped.filter(F.col("transaction_type").isin(TRANSACTION_TYPES))
    quarantine(deduped.filter(~F.col("transaction_type").isin(TRANSACTION_TYPES)), "transactions_bad_type")

    typed = valid_type.select(
        "transaction_id",
        "account_id",
        "transaction_type",
        F.col("amount").cast(DecimalType(10, 2)).alias("amount"),
        "currency",
        F.col("transaction_ts").cast("timestamp"),
    )

    parent_keys = silver_accounts.select("account_id")
    quarantine(typed.join(parent_keys, "account_id", "left_anti"), "transactions_orphans")
    return typed.join(parent_keys, "account_id", "left_semi")


def transform_savings_goals(bronze_goals: DataFrame, silver_users: DataFrame) -> DataFrame:
    deduped = dedupe_latest(bronze_goals, ["goal_id"])

    typed = deduped.select(
        "goal_id",
        "user_id",
        F.trim(F.col("goal_name")).alias("goal_name"),
        F.col("target_amount").cast(DecimalType(10, 2)).alias("target_amount"),
        F.col("current_amount").cast(DecimalType(10, 2)).alias("current_amount"),
        F.col("created_date").cast("date"),
        F.col("target_date").cast("date"),
    )

    parent_keys = silver_users.select("user_id")
    quarantine(typed.join(parent_keys, "user_id", "left_anti"), "savings_goals_orphans")
    return typed.join(parent_keys, "user_id", "left_semi")


if __name__ == "__main__":
    spark = get_spark()
    token = load_uc_token()

    account_types = build_account_types(spark)
    write_delta_table(token, account_types, "silver", "account_types")
    print(f"silver.account_types: {account_types.count()} rows")

    transaction_types = build_transaction_types(spark)
    write_delta_table(token, transaction_types, "silver", "transaction_types")
    print(f"silver.transaction_types: {transaction_types.count()} rows")

    silver_users = transform_users(bronze_table(spark, "users"))
    write_delta_table(token, silver_users, "silver", "users")
    print(f"silver.users: {silver_users.count()} rows")

    silver_accounts = transform_accounts(bronze_table(spark, "accounts"), silver_users, account_types)
    write_delta_table(token, silver_accounts, "silver", "accounts")
    print(f"silver.accounts: {silver_accounts.count()} rows")

    silver_transactions = transform_transactions(bronze_table(spark, "transactions"), silver_accounts)
    write_delta_table(token, silver_transactions, "silver", "transactions")
    print(f"silver.transactions: {silver_transactions.count()} rows")

    silver_goals = transform_savings_goals(bronze_table(spark, "savings_goals"), silver_users)
    write_delta_table(token, silver_goals, "silver", "savings_goals")
    print(f"silver.savings_goals: {silver_goals.count()} rows")
