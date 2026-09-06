from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

import silver_transform
from gold_marts import build_account_summary
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
