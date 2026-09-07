"""End-to-end invariant checks against the live chip_lakehouse catalog.

Testing tier 4 (see docs/standard.md): cross-layer invariants no
catalog-free unit test can cover - row-count conservation, cross-mart
balance agreement, Kimball star reconciliation, SCD2 integrity, and Data Vault raw-layer reconciliation.

Requires the Docker Unity Catalog stack up and a full pipeline run:
    cd docker && docker compose up -d
    python3 src/bronze_ingest.py
    python3 src/silver_transform.py
    python3 src/gold_marts.py
    python3 src/gold_star.py
    python3 src/vault_load.py
    python3 tests/verify_e2e.py
"""
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from spark_session import CATALOG_NAME, get_spark  # noqa: E402
from vault_load import current_sat  # noqa: E402


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


def vault_checks(spark):
    out = []
    b_users = _t(spark, "bronze", "users")
    b_accounts = _t(spark, "bronze", "accounts")
    b_txns = _t(spark, "bronze", "transactions")
    b_goals = _t(spark, "bronze", "savings_goals")

    s_users = _t(spark, "silver", "users")
    s_accounts = _t(spark, "silver", "accounts")
    s_txns = _t(spark, "silver", "transactions")
    s_goals = _t(spark, "silver", "savings_goals")

    hub_user = _t(spark, "vault", "hub_user")
    hub_account = _t(spark, "vault", "hub_account")
    hub_transaction = _t(spark, "vault", "hub_transaction")
    hub_goal = _t(spark, "vault", "hub_savings_goal")

    ref_at = _t(spark, "vault", "ref_account_type")
    ref_tt = _t(spark, "vault", "ref_transaction_type")

    # 1. hub key uniqueness + 2. hub completeness vs distinct bronze keys
    for hub, hk, bk, bronze_df in [
        (hub_user, "user_hk", "user_id", b_users),
        (hub_account, "account_hk", "account_id", b_accounts),
        (hub_transaction, "transaction_hk", "transaction_id", b_txns),
        (hub_goal, "savings_goal_hk", "goal_id", b_goals),
    ]:
        dup = hub.count() - hub.select(hk).distinct().count()
        out.append(check(f"vault.{hk[:-3]} hub key unique", dup == 0, f"{dup} dupes"))
        want = bronze_df.select(bk).distinct().count()
        got = hub.count()
        out.append(check(f"vault hub_{bk[:-3]} count == distinct bronze.{bk}", got == want, f"{got} vs {want}"))

    # 3. link FK integrity (parent hk resolves to its hub) + link key uniqueness
    for link_name, parents in [
        ("link_account_user", [("account_hk", hub_account), ("user_hk", hub_user)]),
        ("link_transaction_account", [("transaction_hk", hub_transaction), ("account_hk", hub_account)]),
        ("link_savings_goal_user", [("savings_goal_hk", hub_goal), ("user_hk", hub_user)]),
    ]:
        link = _t(spark, "vault", link_name)
        link_hk = link_name.replace("link_", "") + "_hk"
        dup = link.count() - link.select(link_hk).distinct().count()
        out.append(check(f"vault.{link_name} link key unique", dup == 0, f"{dup} dupes"))
        for phk, hub in parents:
            orphan = link.join(hub.select(phk), phk, "left_anti").count()
            out.append(check(f"vault.{link_name}.{phk} resolves to hub", orphan == 0, f"{orphan} orphans"))

    # 4. satellite FK integrity + 5. no consecutive equal hash_diff per key
    for sat_name, hk, hub in [
        ("sat_user_details", "user_hk", hub_user),
        ("sat_account_details", "account_hk", hub_account),
        ("sat_transaction_details", "transaction_hk", hub_transaction),
        ("sat_savings_goal_details", "savings_goal_hk", hub_goal),
    ]:
        sat = _t(spark, "vault", sat_name)
        orphan = sat.join(hub.select(hk), hk, "left_anti").count()
        out.append(check(f"vault.{sat_name}.{hk} resolves to hub", orphan == 0, f"{orphan} orphans"))
        w = Window.partitionBy(hk).orderBy("load_date")
        repeats = (
            sat.withColumn("_prev", F.lag("hash_diff").over(w))
            .where(F.col("_prev") == F.col("hash_diff"))
            .count()
        )
        out.append(check(f"vault.{sat_name}: no consecutive equal hash_diff", repeats == 0, f"{repeats} repeats"))

    # 6. loss-free reconciliation: current-state reconstruction == silver.
    # Reconstructed columns are aliased with a _v suffix so the post-join
    # comparison is never ambiguous.
    def _recon_check(label, recon, silver_df, join_key, pairs):
        # pairs: list of (recon _v column, silver column)
        joined = recon.join(silver_df, join_key, "inner")
        cond = None
        for v_col, s_col in pairs:
            c = ~recon[v_col].eqNullSafe(silver_df[s_col])
            cond = c if cond is None else (cond | c)
        mism = joined.where(cond).count()
        rc, sc, jn = recon.count(), silver_df.count(), joined.count()
        out.append(check(
            label,
            rc == sc and jn == rc and mism == 0,
            f"{rc} vs {sc} rows, {jn} joined, {mism} mismatches",
        ))

    # users: age_band recomputed from date_of_birth with transform_users' formula
    su = current_sat(_t(spark, "vault", "sat_user_details"), "user_hk").join(hub_user, "user_hk", "inner")
    # NOTE: recomputed with today's date - assumes silver_transform.py and this
    # script run in the same session (silver freezes age_band; the vault keeps only date_of_birth).
    age_years = F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
    band_start = (F.floor(age_years / 10) * 10).cast("int")
    su = su.select(
        "user_id",
        F.concat(band_start, F.lit("-"), band_start + 9).alias("age_band_v"),
        F.col("signup_date").alias("signup_date_v"),
    )
    _recon_check("vault users reconstruct == silver.users", su, s_users, "user_id",
                 [("age_band_v", "age_band"), ("signup_date_v", "signup_date")])

    # accounts: masked number recomputed, account_type resolved via ref,
    # owning user_id recovered from link_account_user (hub_account carries
    # no user_id) so it can be reconciled against silver (FIX 4).
    sa = current_sat(_t(spark, "vault", "sat_account_details"), "account_hk").join(hub_account, "account_hk", "inner")
    acc_user = (
        _t(spark, "vault", "link_account_user")
        .join(hub_user.select("user_hk", "user_id"), "user_hk", "inner")
        .select("account_hk", "user_id")
    )
    sa = (
        sa.join(ref_at.select(F.col("type_name").alias("account_type"), "account_type_id"), "account_type", "inner")
        .join(acc_user, "account_hk", "inner")
    )
    # FIX 3: apply silver's orphan rule - drop accounts whose owning user is absent from silver.
    sa = sa.join(s_users.select("user_id"), "user_id", "left_semi")
    sa = sa.select(
        "account_id",
        F.col("user_id").alias("user_id_v"),
        F.col("account_type_id").alias("account_type_id_v"),
        F.concat(F.lit("****"), F.substring(F.col("account_number"), -4, 4)).alias("account_number_masked_v"),
        F.col("opened_date").alias("opened_date_v"),
    )
    _recon_check("vault accounts reconstruct == silver.accounts", sa, s_accounts, "account_id",
                 [("user_id_v", "user_id"),
                  ("account_type_id_v", "account_type_id"),
                  ("account_number_masked_v", "account_number_masked"),
                  ("opened_date_v", "opened_date")])

    # transactions: amount/currency/ts equal, transaction_type resolved via ref
    st = current_sat(_t(spark, "vault", "sat_transaction_details"), "transaction_hk").join(hub_transaction, "transaction_hk", "inner")
    txn_acc = (
        _t(spark, "vault", "link_transaction_account")
        .join(hub_account.select("account_hk", "account_id"), "account_hk", "inner")
        .select("transaction_hk", "account_id")
    )
    st = (
        st.join(ref_tt.select(F.col("type_name").alias("transaction_type"), "transaction_type_id"), "transaction_type", "inner")
        .join(txn_acc, "transaction_hk", "inner")
    )
    # FIX 3: apply silver's orphan rule - drop transactions whose account is absent from silver.
    st = st.join(s_accounts.select("account_id"), "account_id", "left_semi")
    st = st.select(
        "transaction_id",
        F.col("transaction_type_id").alias("transaction_type_id_v"),
        F.col("amount").cast("decimal(10,2)").alias("amount_v"),
        F.col("currency").alias("currency_v"),
        F.col("transaction_ts").alias("transaction_ts_v"),
    )
    _recon_check("vault transactions reconstruct == silver.transactions", st, s_txns, "transaction_id",
                 [("transaction_type_id_v", "transaction_type_id"), ("amount_v", "amount"),
                  ("currency_v", "currency"), ("transaction_ts_v", "transaction_ts")])

    # savings_goals: all typed columns equal
    sg = current_sat(_t(spark, "vault", "sat_savings_goal_details"), "savings_goal_hk").join(hub_goal, "savings_goal_hk", "inner")
    goal_user = (
        _t(spark, "vault", "link_savings_goal_user")
        .join(hub_user.select("user_hk", "user_id"), "user_hk", "inner")
        .select("savings_goal_hk", "user_id")
    )
    sg = sg.join(goal_user, "savings_goal_hk", "inner")
    # FIX 3: apply silver's orphan rule - drop goals whose user is absent from silver.
    sg = sg.join(s_users.select("user_id"), "user_id", "left_semi")
    sg = sg.select(
        "goal_id",
        F.col("goal_name").alias("goal_name_v"),
        F.col("target_amount").cast("decimal(10,2)").alias("target_amount_v"),
        F.col("current_amount").cast("decimal(10,2)").alias("current_amount_v"),
        F.col("created_date").alias("created_date_v"),
        F.col("target_date").alias("target_date_v"),
    )
    _recon_check("vault savings_goals reconstruct == silver.savings_goals", sg, s_goals, "goal_id",
                 [("goal_name_v", "goal_name"), ("target_amount_v", "target_amount"),
                  ("current_amount_v", "current_amount"), ("created_date_v", "created_date"),
                  ("target_date_v", "target_date")])

    return out


def main():
    spark = get_spark()
    results = (
        silver_checks(spark)
        + gold_mart_checks(spark)
        + gold_star_checks(spark)
        + vault_checks(spark)
    )

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
