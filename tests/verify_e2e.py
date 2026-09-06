"""End-to-end invariant checks against the live chip_lakehouse catalog.

Testing tier 4 (see docs/standard.md): cross-layer invariants no
catalog-free unit test can cover - row-count conservation, cross-mart
balance agreement, Kimball star reconciliation, SCD2 integrity.

Requires the Docker Unity Catalog stack up and a full pipeline run:
    cd docker && docker compose up -d
    python3 src/bronze_ingest.py
    python3 src/silver_transform.py
    python3 src/gold_marts.py
    python3 src/gold_star.py
    python3 tests/verify_e2e.py
"""
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from spark_session import CATALOG_NAME, get_spark  # noqa: E402


def _t(spark, layer, name):
    return spark.table(f"{CATALOG_NAME}.{layer}.{name}")


def check(name, ok, detail=""):
    return (name, bool(ok), detail)


def silver_checks(spark):
    users = _t(spark, "silver", "users")
    accounts = _t(spark, "silver", "accounts")
    txns = _t(spark, "silver", "transactions")
    goals = _t(spark, "silver", "savings_goals")
    atypes = _t(spark, "silver", "account_types")
    ttypes = _t(spark, "silver", "transaction_types")

    orphan_acc = accounts.join(users, "user_id", "left_anti").count()
    orphan_txn = txns.join(accounts, "account_id", "left_anti").count()
    orphan_goal = goals.join(users, "user_id", "left_anti").count()
    bad_at = accounts.join(atypes, "account_type_id", "left_anti").count()
    bad_tt = txns.join(ttypes, "transaction_type_id", "left_anti").count()

    return [
        check("silver.accounts has rows", accounts.count() > 0),
        check("silver.transactions has rows", txns.count() > 0),
        check("silver.accounts -> users: no orphans", orphan_acc == 0, f"{orphan_acc} orphans"),
        check("silver.transactions -> accounts: no orphans", orphan_txn == 0, f"{orphan_txn} orphans"),
        check("silver.savings_goals -> users: no orphans", orphan_goal == 0, f"{orphan_goal} orphans"),
        check("silver.accounts.account_type_id resolves", bad_at == 0, f"{bad_at} unresolved"),
        check("silver.transactions.transaction_type_id resolves", bad_tt == 0, f"{bad_tt} unresolved"),
    ]


def gold_mart_checks(spark):
    acc_sum = _t(spark, "gold", "account_summary")
    cust = _t(spark, "gold", "customer_360")

    recomputed = acc_sum.groupBy("user_id").agg(F.sum("balance").alias("_sum"))
    mismatch = (
        cust.join(recomputed, "user_id", "left")
        .withColumn("_diff", F.col("total_balance") - F.coalesce(F.col("_sum"), F.lit(0)))
        .where(F.col("_diff") != 0)
        .count()
    )
    return [
        check("gold.account_summary has rows", acc_sum.count() > 0),
        check("gold.customer_360 has rows", cust.count() > 0),
        check(
            "customer_360.total_balance == sum(account_summary.balance)",
            mismatch == 0,
            f"{mismatch} users differ",
        ),
    ]


def gold_star_checks(spark):
    fct = _t(spark, "gold", "fct_transaction")
    snap = _t(spark, "gold", "fct_account_monthly_snapshot")
    dim_c = _t(spark, "gold", "dim_customer")
    dim_a = _t(spark, "gold", "dim_account")
    dim_tt = _t(spark, "gold", "dim_transaction_type")
    dim_d = _t(spark, "gold", "dim_date")
    acc_sum = _t(spark, "gold", "account_summary")
    silver_txn = _t(spark, "silver", "transactions")

    out = []

    # 1. row-count conservation
    fct_n, txn_n = fct.count(), silver_txn.count()
    out.append(check("fct_transaction count == silver.transactions count", fct_n == txn_n, f"{fct_n} vs {txn_n}"))

    # current-version account_id <-> account_key map (account_summary is keyed by account_id)
    acc_map = dim_a.where(F.col("is_current")).select("account_key", "account_id")

    # 2. per-account sum(signed_amount) == account_summary.balance
    star_bal = (
        fct.join(acc_map, "account_key", "inner")
        .groupBy("account_id")
        .agg(F.sum("signed_amount").alias("_star_bal"))
    )
    m2 = (
        acc_sum.join(star_bal, "account_id", "left")
        .withColumn("_diff", F.col("balance") - F.coalesce(F.col("_star_bal"), F.lit(0)))
        .where(F.col("_diff") != 0)
        .count()
    )
    out.append(check("sum(fct_transaction.signed_amount) == account_summary.balance", m2 == 0, f"{m2} accounts differ"))

    # 3. latest-month closing_balance == account_summary.balance
    latest = snap.agg(F.max("date_key").alias("m")).first()["m"]
    snap_bal = (
        snap.where(F.col("date_key") == latest)
        .join(acc_map, "account_key", "inner")
        .select("account_id", "closing_balance")
    )
    m3 = (
        acc_sum.join(snap_bal, "account_id", "left")
        .withColumn("_diff", F.col("balance") - F.coalesce(F.col("closing_balance"), F.lit(0)))
        .where(F.col("_diff") != 0)
        .count()
    )
    out.append(check("latest monthly-snapshot closing_balance == account_summary.balance", m3 == 0, f"{m3} accounts differ"))

    # 4. dim key uniqueness, then fact FK integrity (no orphan, no fan-out)
    for dn, dd, k in [
        ("dim_customer", dim_c, "customer_key"),
        ("dim_account", dim_a, "account_key"),
        ("dim_transaction_type", dim_tt, "transaction_type_key"),
        ("dim_date", dim_d, "date_key"),
    ]:
        dup = dd.count() - dd.select(k).distinct().count()
        out.append(check(f"{dn}.{k} unique", dup == 0, f"{dup} dupes"))

    for fact_name, fact_df, key, dd in [
        ("fct_transaction", fct, "customer_key", dim_c),
        ("fct_transaction", fct, "account_key", dim_a),
        ("fct_transaction", fct, "transaction_type_key", dim_tt),
        ("fct_account_monthly_snapshot", snap, "customer_key", dim_c),
        ("fct_account_monthly_snapshot", snap, "account_key", dim_a),
    ]:
        dim_keys = dd.select(key).distinct()
        orphans = fact_df.join(dim_keys, key, "left_anti").count()
        joined = fact_df.join(dim_keys, key, "inner").count()
        n = fact_df.count()
        out.append(
            check(
                f"{fact_name}.{key} FK integrity",
                orphans == 0 and joined == n,
                f"{orphans} orphans, joined {joined} vs {n}",
            )
        )

    # 5. SCD2 integrity
    for dn, dd, bk in [("dim_customer", dim_c, "user_id"), ("dim_account", dim_a, "account_id")]:
        multi_current = (
            dd.where(F.col("is_current")).groupBy(bk).count().where(F.col("count") > 1).count()
        )
        out.append(check(f"{dn}: exactly one is_current per {bk}", multi_current == 0, f"{multi_current} with >1"))
        w = Window.partitionBy(bk).orderBy("valid_from")
        boundary_bad = (
            dd.withColumn("_next_from", F.lead("valid_from").over(w))
            .where(F.col("_next_from").isNotNull() & (F.col("valid_to") != F.col("_next_from")))
            .count()
        )
        out.append(check(f"{dn}: version ranges contiguous & non-overlapping", boundary_bad == 0, f"{boundary_bad} bad boundaries"))

    # 6. dim_date covers every fact date_key
    for fact_name, fact_df in [("fct_transaction", fct), ("fct_account_monthly_snapshot", snap)]:
        missing = (
            fact_df.select("date_key").distinct()
            .join(dim_d.select("date_key"), "date_key", "left_anti")
            .count()
        )
        out.append(check(f"dim_date covers {fact_name}.date_key", missing == 0, f"{missing} missing"))

    return out


def main():
    spark = get_spark()
    results = silver_checks(spark) + gold_mart_checks(spark) + gold_star_checks(spark)

    failed = 0
    for name, ok, detail in results:
        if ok:
            print(f"PASS  {name}")
        else:
            failed += 1
            print(f"FAIL  {name}  -- {detail}")
    print(f"\n{len(results) - failed}/{len(results)} checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
