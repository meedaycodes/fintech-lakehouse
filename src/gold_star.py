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


def _scd2_open():
    # Lazy: F.lit needs an active SparkContext, so it can't be a module constant.
    return F.lit(date(9999, 12, 31)).cast("date")


def _row_hash(tracked_cols: list[str]):
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in sorted(tracked_cols)
    ]
    return F.sha2(F.concat_ws("||", *parts), 256)


def plan_scd2(
    incoming: DataFrame,
    current_target: "DataFrame | None",
    *,
    business_key: str,
    tracked_cols: list[str],
    effective_from_col: str,
    surrogate_key: str,
    run_date: date,
) -> "tuple[DataFrame, DataFrame]":
    """Decide the SCD2 delta. Pure - no I/O. `incoming` must be one row
    per business_key. See the plan's Task 3 interface block for the
    output schema.
    """
    carry = list(incoming.columns)  # every attribute stays; valid_from is derived, not moved
    hashed = incoming.withColumn("row_hash", _row_hash(tracked_cols))

    def _finalize(df, key_col, valid_from_col):
        return df.select(
            key_col.alias(surrogate_key),
            *carry,
            "row_hash",
            valid_from_col.alias("valid_from"),
            _scd2_open().alias("valid_to"),
            F.lit(True).alias("is_current"),
        )

    if current_target is None:
        w = Window.orderBy(business_key)
        inserts = _finalize(
            hashed, F.row_number().over(w), F.col(effective_from_col).cast("date")
        )
        expire = hashed.select(F.col(business_key)).where(F.lit(False))
        return inserts, expire

    max_key = current_target.agg(
        F.coalesce(F.max(surrogate_key), F.lit(0)).alias("m")
    ).first()["m"]
    cur_keys = (
        current_target.select(business_key).distinct().withColumn("_exists", F.lit(True))
    )
    cur_open_hash = (
        current_target.where(F.col("is_current"))
        .select(business_key, F.col("row_hash").alias("_cur_hash"))
    )
    tagged = hashed.join(cur_keys, business_key, "left").join(
        cur_open_hash, business_key, "left"
    )

    new_or_changed = tagged.where(
        F.col("_exists").isNull() | (F.col("row_hash") != F.col("_cur_hash"))
    ).withColumn(
        "_valid_from",
        F.when(
            F.col("_exists").isNull(), F.col(effective_from_col).cast("date")
        ).otherwise(F.lit(run_date).cast("date")),
    )
    w = Window.orderBy(business_key, "_valid_from")
    inserts = _finalize(
        new_or_changed, F.lit(max_key) + F.row_number().over(w), F.col("_valid_from")
    )
    expire = (
        tagged.where(
            F.col("_exists").isNotNull() & (F.col("row_hash") != F.col("_cur_hash"))
        )
        .select(business_key)
        .distinct()
    )
    return inserts, expire


def _delta_log_exists(location: str) -> bool:
    from pathlib import Path

    return Path(location.replace("file://", "")).joinpath("_delta_log").is_dir()


def merge_scd2(
    token,
    spark,
    incoming: DataFrame,
    schema: str,
    table: str,
    *,
    business_key: str,
    tracked_cols: list[str],
    effective_from_col: str,
    surrogate_key: str,
) -> None:
    from delta.tables import DeltaTable

    location = f"file://{(LAKEHOUSE_DIR / schema / table).resolve()}"
    first_load = (
        get_uc_table(token, schema, table) is None
        and not _delta_log_exists(location)
    )

    if first_load:
        inserts, _ = plan_scd2(
            incoming,
            None,
            business_key=business_key,
            tracked_cols=tracked_cols,
            effective_from_col=effective_from_col,
            surrogate_key=surrogate_key,
            run_date=date.today(),
        )
        write_delta_table(token, inserts, schema, table)
        return

    run_date = date.today()
    current = spark.read.format("delta").load(location)
    inserts, expire = plan_scd2(
        incoming,
        current,
        business_key=business_key,
        tracked_cols=tracked_cols,
        effective_from_col=effective_from_col,
        surrogate_key=surrogate_key,
        run_date=run_date,
    )

    if expire.count() > 0:
        (
            DeltaTable.forPath(spark, location)
            .alias("t")
            .merge(
                expire.alias("s"),
                f"t.{business_key} = s.{business_key} AND t.is_current = true",
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": f"DATE '{run_date.isoformat()}'",
                    "is_current": "false",
                }
            )
            .execute()
        )

    if inserts.count() > 0:
        inserts.write.format("delta").mode("append").save(location)

    fields = spark.read.format("delta").load(location).schema.fields
    register_uc_table(token, schema, table, location, fields)


def _date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_fct_transaction(
    silver_transactions: DataFrame,
    silver_transaction_types: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
) -> DataFrame:
    direction = silver_transaction_types.select("transaction_type_id", "direction")
    txn = (
        silver_transactions.join(direction, "transaction_type_id", "inner")
        .withColumn("_txn_date", F.to_date("transaction_ts"))
    )

    acct = dim_account.select(
        "account_key",
        F.col("account_id").alias("_ba"),
        F.col("user_id").alias("_owner"),
        F.col("valid_from").alias("_af"),
        F.col("valid_to").alias("_at"),
    )
    with_acct = txn.join(
        acct,
        (txn["account_id"] == acct["_ba"])
        & (txn["_txn_date"] >= acct["_af"])
        & (txn["_txn_date"] < acct["_at"]),
        "inner",
    )

    cust = dim_customer.select(
        "customer_key",
        F.col("user_id").alias("_bu"),
        F.col("valid_from").alias("_cf"),
        F.col("valid_to").alias("_ct"),
    )
    with_cust = with_acct.join(
        cust,
        (with_acct["_owner"] == cust["_bu"])
        & (with_acct["_txn_date"] >= cust["_cf"])
        & (with_acct["_txn_date"] < cust["_ct"]),
        "inner",
    )

    amount = F.col("amount").cast("decimal(12,2)")
    return with_cust.select(
        _date_key(F.col("_txn_date")).alias("date_key"),
        "customer_key",
        "account_key",
        F.col("transaction_type_id").alias("transaction_type_key"),
        "transaction_id",
        amount.alias("amount"),
        F.when(F.col("direction") == "inflow", amount)
        .otherwise(-amount)
        .alias("signed_amount"),
        "currency",
    )


def build_fct_account_monthly_snapshot(
    silver_transactions: DataFrame,
    silver_accounts: DataFrame,
    silver_transaction_types: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
) -> DataFrame:
    zero = F.lit(0).cast("decimal(12,2)")
    direction = silver_transaction_types.select("transaction_type_id", "direction")
    txn = (
        silver_transactions.join(direction, "transaction_type_id", "inner")
        .withColumn("_d", F.to_date("transaction_ts"))
        .withColumn("_m", F.trunc("_d", "month"))
        .withColumn(
            "_signed",
            F.when(F.col("direction") == "inflow", F.col("amount"))
            .otherwise(-F.col("amount"))
            .cast("decimal(12,2)"),
        )
    )

    max_month = txn.agg(F.trunc(F.max("_d"), "month").alias("mm")).first()["mm"]

    spine = (
        silver_accounts.select("account_id", "user_id", "opened_date")
        .withColumn("_start", F.trunc("opened_date", "month"))
        .withColumn(
            "_month",
            F.explode(
                F.sequence(F.col("_start"), F.lit(max_month), F.expr("interval 1 month"))
            ),
        )
        .withColumn("_month_end", F.last_day("_month"))
        .withColumn("date_key", _date_key(F.col("_month_end")))
        .select("account_id", "user_id", "_month", "_month_end", "date_key")
    )

    month_agg = txn.groupBy("account_id", "_m").agg(
        F.coalesce(F.sum(F.when(F.col("direction") == "inflow", F.col("amount"))), zero)
        .cast("decimal(12,2)")
        .alias("month_inflow"),
        F.coalesce(F.sum(F.when(F.col("direction") == "outflow", F.col("amount"))), zero)
        .cast("decimal(12,2)")
        .alias("month_outflow"),
        F.count(F.lit(1)).alias("transaction_count"),
    )

    # closing balance: sum of every signed txn for the account dated on or
    # before this spine row's month end.
    closing = (
        spine.join(
            txn.select(
                F.col("account_id").alias("_ta"), F.col("_d").alias("_td"), "_signed"
            ),
            (spine["account_id"] == F.col("_ta")) & (F.col("_td") <= spine["_month_end"]),
            "left",
        )
        .groupBy(
            spine["account_id"], spine["user_id"], spine["_month"],
            spine["_month_end"], spine["date_key"],
        )
        .agg(F.coalesce(F.sum("_signed"), zero).cast("decimal(12,2)").alias("closing_balance"))
    )

    combined = (
        closing.join(
            month_agg,
            (closing["account_id"] == month_agg["account_id"])
            & (closing["_month"] == month_agg["_m"]),
            "left",
        )
        .select(
            closing["account_id"].alias("account_id"),
            closing["user_id"].alias("user_id"),
            closing["_month_end"].alias("_month_end"),
            closing["date_key"].alias("date_key"),
            "closing_balance",
            F.coalesce(F.col("month_inflow"), zero).alias("month_inflow"),
            F.coalesce(F.col("month_outflow"), zero).alias("month_outflow"),
            F.coalesce(F.col("transaction_count"), F.lit(0)).alias("transaction_count"),
        )
        .withColumn(
            "month_net",
            (F.col("month_inflow") - F.col("month_outflow")).cast("decimal(12,2)"),
        )
    )

    acct = dim_account.select(
        "account_key",
        F.col("account_id").alias("_ba"),
        F.col("valid_from").alias("_af"),
        F.col("valid_to").alias("_at"),
    )
    cust = dim_customer.select(
        "customer_key",
        F.col("user_id").alias("_bu"),
        F.col("valid_from").alias("_cf"),
        F.col("valid_to").alias("_ct"),
    )
    return (
        combined.join(
            acct,
            (combined["account_id"] == acct["_ba"])
            & (combined["_month_end"] >= acct["_af"])
            & (combined["_month_end"] < acct["_at"]),
            "inner",
        )
        .join(
            cust,
            (combined["user_id"] == cust["_bu"])
            & (combined["_month_end"] >= cust["_cf"])
            & (combined["_month_end"] < cust["_ct"]),
            "inner",
        )
        .select(
            "date_key",
            "customer_key",
            "account_key",
            "month_inflow",
            "month_outflow",
            "month_net",
            "closing_balance",
            "transaction_count",
        )
    )
