from pyspark.sql import Row
from pyspark.sql import functions as F

from silver_transform import build_account_types, build_transaction_types, transform_accounts


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


def test_transform_accounts_quarantines_unrecognized_type(spark):
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
