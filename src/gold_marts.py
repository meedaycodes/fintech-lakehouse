"""Builds business-level marts from clean silver entities.

Run after src/silver_transform.py:
    python3 src/gold_marts.py

Two marts, deliberately layered so they can't silently disagree:

  - account_summary: one row per account. There's no stored "balance"
    column anywhere upstream - accounts/transactions are an append-only
    ledger, so this mart derives balance itself by summing signed
    transaction amounts. This is the foundational aggregate.

  - customer_360: one row per user. Built by reading account_summary back
    *from the catalog* (not by re-aggregating raw transactions a second
    time) and rolling it up to the user grain, alongside savings-goal
    progress. This is what "genuinely interoperable" means here: a user's
    total_balance in customer_360 is mathematically guaranteed to equal
    the sum of their own rows in account_summary, because it's computed
    from that mart, not recomputed independently against silver.
    Two dashboards built on these can never quietly drift apart.

Per docs/standard.md's layer contract, gold is where joins/aggregation
belong - silver stays one-to-one with source entities on purpose.
"""
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import write_delta_table

def zero_decimal():
    # Built lazily, not at module import time - F.lit() needs an active
    # SparkSession, which doesn't exist yet when this module is imported.
    return F.lit(Decimal("0.00")).cast(DecimalType(12, 2))


def silver_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.silver.{name}")


def gold_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.gold.{name}")


def build_account_summary(
    silver_accounts: DataFrame,
    silver_transactions: DataFrame,
    account_types: DataFrame,
    transaction_types: DataFrame,
) -> DataFrame:
    zero = zero_decimal()

    # direction now lives in silver.transaction_types, not a Python list -
    # this is the actual point of the Inmon refactor: the business rule
    # that used to be INFLOW_TRANSACTION_TYPES in code is now governed data.
    txn_with_direction = silver_transactions.join(
        transaction_types.select("transaction_type_id", "direction"), "transaction_type_id", "inner"
    )

    txn_agg = txn_with_direction.groupBy("account_id").agg(
        F.sum(F.when(F.col("direction") == "inflow", F.col("amount"))).alias("total_inflows"),
        F.sum(F.when(F.col("direction") == "outflow", F.col("amount"))).alias("total_outflows"),
        F.count(F.lit(1)).alias("transaction_count"),
        F.min("transaction_ts").alias("first_transaction_ts"),
        F.max("transaction_ts").alias("last_transaction_ts"),
    )

    # silver normalizes account_type for governance; gold resolves it back
    # to a readable label for consumption - that's gold's job per
    # docs/standard.md's layer contract.
    accounts_with_label = silver_accounts.join(
        account_types.select("account_type_id", F.col("type_name").alias("account_type")),
        "account_type_id",
        "inner",
    )

    # left join: an account with zero transactions still gets a row here,
    # with a zero balance rather than disappearing from the mart.
    return (
        accounts_with_label.join(txn_agg, "account_id", "left")
        .select(
            "account_id",
            "user_id",
            "account_type",
            "opened_date",
            F.coalesce(F.col("total_inflows"), zero).alias("total_inflows"),
            F.coalesce(F.col("total_outflows"), zero).alias("total_outflows"),
            (F.coalesce(F.col("total_inflows"), zero) - F.coalesce(F.col("total_outflows"), zero)).alias("balance"),
            F.coalesce(F.col("transaction_count"), F.lit(0)).alias("transaction_count"),
            "first_transaction_ts",
            "last_transaction_ts",
        )
    )


def build_customer_360(silver_users: DataFrame, account_summary: DataFrame, silver_goals: DataFrame) -> DataFrame:
    zero = zero_decimal()

    account_agg = account_summary.groupBy("user_id").agg(
        F.count(F.lit(1)).alias("num_accounts"),
        F.sum("balance").alias("total_balance"),
        F.sum("total_inflows").alias("total_inflows"),
        F.sum("total_outflows").alias("total_outflows"),
    )

    goal_agg = silver_goals.groupBy("user_id").agg(
        F.count(F.lit(1)).alias("num_savings_goals"),
        F.sum("target_amount").alias("total_target_amount"),
        F.sum("current_amount").alias("total_saved_toward_goals"),
    )

    return (
        silver_users
        .join(account_agg, "user_id", "left")
        .join(goal_agg, "user_id", "left")
        .select(
            "user_id",
            "age_band",
            "signup_date",
            F.coalesce(F.col("num_accounts"), F.lit(0)).alias("num_accounts"),
            F.coalesce(F.col("total_balance"), zero).alias("total_balance"),
            F.coalesce(F.col("total_inflows"), zero).alias("total_inflows"),
            F.coalesce(F.col("total_outflows"), zero).alias("total_outflows"),
            F.coalesce(F.col("num_savings_goals"), F.lit(0)).alias("num_savings_goals"),
            F.coalesce(F.col("total_target_amount"), zero).alias("total_target_amount"),
            F.coalesce(F.col("total_saved_toward_goals"), zero).alias("total_saved_toward_goals"),
            # NULL (not 0%) when there's no goal at all - "no goal" and
            # "goal with 0% progress" are different facts.
            F.when(
                F.coalesce(F.col("total_target_amount"), zero) > 0,
                F.round(F.col("total_saved_toward_goals") / F.col("total_target_amount") * 100, 1),
            ).alias("goal_progress_pct"),
        )
    )


if __name__ == "__main__":
    spark = get_spark()
    token = load_uc_token()

    account_summary = build_account_summary(
        silver_table(spark, "accounts"),
        silver_table(spark, "transactions"),
        silver_table(spark, "account_types"),
        silver_table(spark, "transaction_types"),
    )
    write_delta_table(token, account_summary, "gold", "account_summary")
    print(f"gold.account_summary: {account_summary.count()} rows")

    # Read back from the catalog, not the in-memory DataFrame above - this
    # mart genuinely depends on account_summary as a persisted table, the
    # same way a separate, later pipeline run would.
    customer_360 = build_customer_360(
        silver_table(spark, "users"),
        gold_table(spark, "account_summary"),
        silver_table(spark, "savings_goals"),
    )
    write_delta_table(token, customer_360, "gold", "customer_360")
    print(f"gold.customer_360: {customer_360.count()} rows")
