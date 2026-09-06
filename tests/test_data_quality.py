from datetime import date
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

import silver_transform
from gold_marts import build_account_summary
from gold_star import (
    _delta_log_exists,
    build_dim_date,
    build_dim_transaction_type,
    build_fct_transaction,
    plan_scd2,
)
from silver_transform import (
    build_account_types,
    build_transaction_types,
    transform_accounts,
    transform_transactions,
)


def test_build_account_types_has_expected_rows(spark):
    df = build_account_types(spark)
    rows = {r["type_name"]: r["category"] for r in df.collect()}
    assert rows == {
        "savings": "cash",
        "investment": "investment",
        "pension": "retirement",
    }


def test_build_transaction_types_direction_matches_business_rule(spark):
    df = build_transaction_types(spark)
    rows = {r["type_name"]: r["direction"] for r in df.collect()}
    assert rows == {
        "deposit": "inflow",
        "withdrawal": "outflow",
        "roundup": "inflow",
        "investment_contribution": "inflow",
    }


# type_name is what both transforms join bronze's category column against.
# A duplicate would fan a single bronze row out into several silver rows -
# silently breaking silver's one-row-per-primary-key contract - so the seed
# lists have to keep it unique even though nothing at the storage layer
# enforces that for us.
def test_account_types_type_name_is_unique(spark):
    df = build_account_types(spark)
    assert df.count() == df.select("type_name").distinct().count()


def test_transaction_types_type_name_is_unique(spark):
    df = build_transaction_types(spark)
    assert df.count() == df.select("type_name").distinct().count()


def test_transform_accounts_assigns_fk_and_masks_account_number(spark):
    account_types = build_account_types(spark)
    silver_users = spark.createDataFrame([Row(user_id="u1")])
    bronze_accounts = (
        spark.createDataFrame([
            Row(
                account_id="a1", user_id="u1", account_type="savings",
                account_number="GB12ABCD10203040506070", opened_date="2021-01-01",
                _ingested_at="2026-01-01 00:00:00",
            ),
        ])
        .withColumn("opened_date", F.to_date("opened_date"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_accounts(bronze_accounts, silver_users, account_types).collect()

    assert len(result) == 1
    assert result[0]["account_type_id"] == 1  # savings
    assert result[0]["account_number_masked"] == "****6070"
    assert "account_type" not in result[0].asDict()


def test_transform_accounts_keeps_only_the_latest_duplicate(spark):
    # Regression guard for the Inmon refactor: dedupe_latest()'s output is now
    # consumed by the lookup join rather than a filter, so a dedup that picked
    # the wrong row would still produce one well-formed row per account and
    # look fine. Assert on a column that differs between the duplicates.
    account_types = build_account_types(spark)
    silver_users = spark.createDataFrame([Row(user_id="u1")])
    bronze_accounts = (
        spark.createDataFrame([
            Row(
                account_id="a1", user_id="u1", account_type="savings",
                account_number="GB12ABCD10203040501111", opened_date="2021-01-01",
                _ingested_at="2026-01-01 00:00:00",  # older
            ),
            Row(
                account_id="a1", user_id="u1", account_type="savings",
                account_number="GB12ABCD10203040502222", opened_date="2021-01-01",
                _ingested_at="2026-06-01 00:00:00",  # later - this one wins
            ),
        ])
        .withColumn("opened_date", F.to_date("opened_date"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_accounts(bronze_accounts, silver_users, account_types).collect()

    assert len(result) == 1
    assert result[0]["account_number_masked"] == "****2222"


def test_transform_accounts_quarantines_unrecognized_type(spark, monkeypatch, tmp_path):
    # QUARANTINE_DIR is the real, shared data/quarantine/ - redirect it so
    # this test can't overwrite a genuine pipeline run's quarantined rows.
    monkeypatch.setattr(silver_transform, "QUARANTINE_DIR", tmp_path)
    account_types = build_account_types(spark)
    silver_users = spark.createDataFrame([Row(user_id="u1")])
    bronze_accounts = (
        spark.createDataFrame([
            Row(
                account_id="a1", user_id="u1", account_type="BOGUS",
                account_number="GB12ABCD10203040506070", opened_date="2021-01-01",
                _ingested_at="2026-01-01 00:00:00",
            ),
        ])
        .withColumn("opened_date", F.to_date("opened_date"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_accounts(bronze_accounts, silver_users, account_types)

    assert result.count() == 0


def test_transform_transactions_assigns_fk(spark):
    transaction_types = build_transaction_types(spark)
    silver_accounts = spark.createDataFrame([Row(account_id="a1")])
    bronze_transactions = (
        spark.createDataFrame([
            Row(
                transaction_id="t1", account_id="a1", transaction_type="withdrawal",
                amount=100.50, currency="GBP", transaction_ts="2026-01-01 00:00:00",
                _ingested_at="2026-01-01 00:00:00",
            ),
        ])
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_transactions(bronze_transactions, silver_accounts, transaction_types).collect()

    assert len(result) == 1
    assert result[0]["transaction_type_id"] == 2  # withdrawal
    assert float(result[0]["amount"]) == 100.50


def test_transform_transactions_keeps_only_the_latest_duplicate(spark):
    # Same regression guard as the accounts case above - amount is the column
    # that differs, so picking the older duplicate would be visible here.
    transaction_types = build_transaction_types(spark)
    silver_accounts = spark.createDataFrame([Row(account_id="a1")])
    bronze_transactions = (
        spark.createDataFrame([
            Row(
                transaction_id="t1", account_id="a1", transaction_type="withdrawal",
                amount=100.50, currency="GBP", transaction_ts="2026-01-01 00:00:00",
                _ingested_at="2026-01-01 00:00:00",  # older
            ),
            Row(
                transaction_id="t1", account_id="a1", transaction_type="withdrawal",
                amount=250.75, currency="GBP", transaction_ts="2026-01-01 00:00:00",
                _ingested_at="2026-06-01 00:00:00",  # later - this one wins
            ),
        ])
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_transactions(bronze_transactions, silver_accounts, transaction_types).collect()

    assert len(result) == 1
    assert result[0]["amount"] == Decimal("250.75")


def test_transform_transactions_quarantines_unrecognized_type(spark, monkeypatch, tmp_path):
    # See the accounts equivalent: keep the shared data/quarantine/ untouched.
    monkeypatch.setattr(silver_transform, "QUARANTINE_DIR", tmp_path)
    transaction_types = build_transaction_types(spark)
    silver_accounts = spark.createDataFrame([Row(account_id="a1")])
    bronze_transactions = (
        spark.createDataFrame([
            Row(
                transaction_id="t1", account_id="a1", transaction_type="BOGUS",
                amount=100.50, currency="GBP", transaction_ts="2026-01-01 00:00:00",
                _ingested_at="2026-01-01 00:00:00",
            ),
        ])
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
        .withColumn("_ingested_at", F.to_timestamp("_ingested_at"))
    )

    result = transform_transactions(bronze_transactions, silver_accounts, transaction_types)

    assert result.count() == 0


def test_build_account_summary_classifies_by_direction_and_resolves_label(spark):
    account_types = build_account_types(spark)
    transaction_types = build_transaction_types(spark)

    silver_accounts = (
        spark.createDataFrame([Row(account_id="a1", user_id="u1", account_type_id=1, opened_date="2021-01-01")])
        .withColumn("opened_date", F.to_date("opened_date"))
    )

    silver_transactions = (
        spark.createDataFrame([
            Row(transaction_id="t1", account_id="a1", transaction_type_id=1,
                amount=Decimal("100.00"), currency="GBP", transaction_ts="2026-01-01 00:00:00"),  # deposit -> inflow
            Row(transaction_id="t2", account_id="a1", transaction_type_id=2,
                amount=Decimal("30.00"), currency="GBP", transaction_ts="2026-01-02 00:00:00"),  # withdrawal -> outflow
        ])
        .withColumn("transaction_ts", F.to_timestamp("transaction_ts"))
    )

    result = build_account_summary(silver_accounts, silver_transactions, account_types, transaction_types).collect()

    assert len(result) == 1
    row = result[0]
    assert row["account_type"] == "savings"
    assert row["total_inflows"] == Decimal("100.00")
    assert row["total_outflows"] == Decimal("30.00")
    assert row["balance"] == Decimal("70.00")


def test_build_dim_date_spans_and_flags(spark):
    dim = build_dim_date(spark, date(2024, 1, 1), date(2024, 3, 31))
    rows = {r["date_key"]: r for r in dim.collect()}

    assert dim.count() == 91  # Jan 31 + Feb 29 (2024 leap) + Mar 31
    assert 20240101 in rows and 20240331 in rows
    assert rows[20240131]["is_month_end"]
    assert not rows[20240115]["is_month_end"]
    # 2024-01-06 is a Saturday, 2024-01-08 is a Monday
    assert rows[20240106]["day_of_week"] == 6
    assert rows[20240106]["is_weekend"]
    assert rows[20240108]["day_of_week"] == 1
    assert not rows[20240108]["is_weekend"]


def test_delta_log_exists(tmp_path):
    loc = f"file://{tmp_path}"
    assert not _delta_log_exists(loc)
    (tmp_path / "_delta_log").mkdir()
    assert _delta_log_exists(loc)


def test_build_dim_transaction_type_passthrough(spark):
    stt = spark.createDataFrame(
        [(1, "deposit", "inflow", "d"), (2, "withdrawal", "outflow", "w")],
        ["transaction_type_id", "type_name", "direction", "description"],
    )
    out = {
        r["transaction_type_key"]: r["direction"]
        for r in build_dim_transaction_type(stt).collect()
    }
    assert out == {1: "inflow", 2: "outflow"}


_C_ARGS = dict(
    business_key="user_id",
    tracked_cols=["age_band", "signup_date"],
    effective_from_col="signup_date",
    surrogate_key="customer_key",
)


def _seed(spark, rows):
    incoming = spark.createDataFrame(rows, ["user_id", "age_band", "signup_date"])
    inserts, _ = plan_scd2(incoming, None, run_date=date(2026, 1, 1), **_C_ARGS)
    return inserts


def test_plan_scd2_initial_load_all_current(spark):
    incoming = spark.createDataFrame(
        [("u2", "30-39", date(2022, 1, 1)), ("u1", "20-29", date(2021, 6, 1))],
        ["user_id", "age_band", "signup_date"],
    )
    inserts, expire = plan_scd2(incoming, None, run_date=date(2026, 9, 6), **_C_ARGS)

    assert expire.count() == 0
    rows = {r["user_id"]: r for r in inserts.collect()}
    assert rows["u1"]["customer_key"] == 1  # keyed in business-key order
    assert rows["u2"]["customer_key"] == 2
    assert all(r["is_current"] for r in rows.values())
    assert all(r["valid_to"] == date(9999, 12, 31) for r in rows.values())
    assert rows["u1"]["valid_from"] == date(2021, 6, 1)
    assert rows["u1"]["signup_date"] == date(2021, 6, 1)  # attribute retained too
    assert rows["u1"]["row_hash"] != rows["u2"]["row_hash"]


def test_plan_scd2_unchanged_is_noop(spark):
    seed = _seed(spark, [("u1", "20-29", date(2021, 6, 1))])
    incoming = spark.createDataFrame(
        [("u1", "20-29", date(2021, 6, 1))], ["user_id", "age_band", "signup_date"]
    )
    inserts, expire = plan_scd2(incoming, seed, run_date=date(2026, 9, 6), **_C_ARGS)
    assert inserts.count() == 0
    assert expire.count() == 0


def test_plan_scd2_changed_attribute_versions(spark):
    seed = _seed(spark, [("u1", "20-29", date(2021, 6, 1))])
    v2 = spark.createDataFrame(
        [("u1", "30-39", date(2021, 6, 1))], ["user_id", "age_band", "signup_date"]
    )
    inserts, expire = plan_scd2(v2, seed, run_date=date(2026, 9, 6), **_C_ARGS)

    assert [r["user_id"] for r in expire.collect()] == ["u1"]
    ins = inserts.collect()
    assert len(ins) == 1
    assert ins[0]["customer_key"] == 2  # max existing key (1) + 1
    assert ins[0]["age_band"] == "30-39"
    assert ins[0]["valid_from"] == date(2026, 9, 6)  # run_date, not signup_date
    assert ins[0]["is_current"]


def test_plan_scd2_new_business_key_inserts(spark):
    seed = _seed(spark, [("u1", "20-29", date(2021, 6, 1))])
    v2 = spark.createDataFrame(
        [("u1", "20-29", date(2021, 6, 1)), ("u2", "40-49", date(2023, 3, 1))],
        ["user_id", "age_band", "signup_date"],
    )
    inserts, expire = plan_scd2(v2, seed, run_date=date(2026, 9, 6), **_C_ARGS)

    assert expire.count() == 0
    ins = inserts.collect()
    assert len(ins) == 1
    assert ins[0]["user_id"] == "u2"
    assert ins[0]["customer_key"] == 2
    assert ins[0]["valid_from"] == date(2023, 3, 1)  # effective_from, not run_date


def _dim_account_row(spark, rows):
    return spark.createDataFrame(
        rows, ["account_key", "account_id", "user_id", "valid_from", "valid_to"]
    )


def _dim_customer_row(spark, rows):
    return spark.createDataFrame(
        rows, ["customer_key", "user_id", "valid_from", "valid_to"]
    )


def _txns(spark, rows):
    cols = [
        "transaction_id", "account_id", "transaction_type_id",
        "amount", "currency", "transaction_ts",
    ]
    return spark.createDataFrame(rows, cols).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )


def test_build_fct_transaction_signed_amount_by_direction(spark):
    stt = spark.createDataFrame(
        [(1, "inflow"), (2, "outflow")], ["transaction_type_id", "direction"]
    )
    st = _txns(spark, [
        ("t1", "a1", 1, Decimal("100.00"), "GBP", "2026-01-01 10:00:00"),
        ("t2", "a1", 2, Decimal("40.00"), "GBP", "2026-01-02 10:00:00"),
    ])
    da = _dim_account_row(spark, [(10, "a1", "u1", date(2020, 1, 1), date(9999, 12, 31))])
    dc = _dim_customer_row(spark, [(20, "u1", date(2019, 1, 1), date(9999, 12, 31))])

    rows = {r["transaction_id"]: r for r in build_fct_transaction(st, stt, dc, da).collect()}
    assert rows["t1"]["signed_amount"] == Decimal("100.00")
    assert rows["t2"]["signed_amount"] == Decimal("-40.00")
    assert rows["t1"]["date_key"] == 20260101
    assert rows["t1"]["customer_key"] == 20 and rows["t1"]["account_key"] == 10
    assert rows["t1"]["transaction_type_key"] == 1


def test_build_fct_transaction_point_in_time_join(spark):
    stt = spark.createDataFrame([(1, "inflow")], ["transaction_type_id", "direction"])
    st = _txns(spark, [
        ("t_early", "a1", 1, Decimal("10.00"), "GBP", "2026-01-10 10:00:00"),
        ("t_late", "a1", 1, Decimal("10.00"), "GBP", "2026-06-10 10:00:00"),
    ])
    da = _dim_account_row(spark, [
        (10, "a1", "u1", date(2020, 1, 1), date(2026, 3, 1)),
        (11, "a1", "u1", date(2026, 3, 1), date(9999, 12, 31)),
    ])
    dc = _dim_customer_row(spark, [(20, "u1", date(2019, 1, 1), date(9999, 12, 31))])

    keyed = {r["transaction_id"]: r["account_key"] for r in build_fct_transaction(st, stt, dc, da).collect()}
    assert keyed["t_early"] == 10
    assert keyed["t_late"] == 11


def test_build_fct_transaction_row_count_preserved(spark):
    stt = spark.createDataFrame(
        [(1, "inflow"), (2, "outflow")], ["transaction_type_id", "direction"]
    )
    st = _txns(spark, [
        (f"t{i}", "a1", (i % 2) + 1, Decimal("5.00"), "GBP", "2026-02-01 10:00:00")
        for i in range(7)
    ])
    da = _dim_account_row(spark, [(10, "a1", "u1", date(2020, 1, 1), date(9999, 12, 31))])
    dc = _dim_customer_row(spark, [(20, "u1", date(2019, 1, 1), date(9999, 12, 31))])

    assert build_fct_transaction(st, stt, dc, da).count() == 7
