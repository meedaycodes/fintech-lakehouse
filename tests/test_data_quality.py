from silver_transform import build_account_types, build_transaction_types


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
