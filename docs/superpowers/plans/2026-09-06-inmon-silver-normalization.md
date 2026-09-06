# Inmon-Style Silver Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Normalize `silver.accounts.account_type` and `silver.transactions.transaction_type` out into governed lookup tables (`silver.account_types`, `silver.transaction_types`), replacing the string columns with FK ids, and move the inflow/outflow business rule `gold_marts.py` depends on from a hardcoded Python list into a `direction` column on the new lookup table.

**Architecture:** Two new lookup-table builder functions in `silver_transform.py`, seeded from hardcoded literals (not derived from data). `transform_accounts`/`transform_transactions` gain a lookup-table parameter and replace their `isin(...)` enum check with a join (a row that matches no lookup row is quarantined exactly like an orphaned FK today). `gold_marts.py`'s `build_account_summary` joins the transaction-type lookup to classify inflow/outflow via `direction`, and joins the account-type lookup to resolve the FK back to a readable label for the mart.

**Tech Stack:** PySpark 3.5.3, Delta Lake, pytest (new: a local, catalog-free `SparkSession` fixture for fast unit tests - none of the functions under test touch Unity Catalog).

**Spec:** [docs/superpowers/specs/2026-09-06-inmon-silver-normalization-design.md](../specs/2026-09-06-inmon-silver-normalization-design.md)

## Global Constraints

- All new tables are written via `uc_delta.write_delta_table()` - never `df.write.saveAsTable()` (unitycatalog-spark 0.2.1's own table-creation path is broken; see `bronze_ingest.py`'s module docstring).
- Surrogate keys are the exact hardcoded integers below - not generated, not computed.
- `direction` must exactly reproduce today's classification: `deposit`/`roundup`/`investment_contribution` = `"inflow"`, `withdrawal` = `"outflow"`. This is a correctness requirement, not a style choice - `gold_marts.py`'s balance math depends on it.
- Column naming: `<entity>_type_id` for the surrogate key, `type_name` for the business-key label (matches the values already in `data_gen/generate_data.py`'s `ACCOUNT_TYPES`/`TRANSACTION_TYPES`).
- Reuse the existing `dedupe_latest()`/`quarantine()` helpers in `silver_transform.py` unchanged - do not reimplement dedup or quarantine logic.
- Seed data (exact, from the spec):
  ```python
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
  ```

---

## Task 1: Test infrastructure + lookup table builders

**Files:**
- Create: `tests/conftest.py`
- Modify: `src/silver_transform.py` (add seed lists + two builder functions, after the existing `TRANSACTION_TYPES = [...]` line; leave the old `ACCOUNT_TYPES`/`TRANSACTION_TYPES` lists in place for now - Task 2/3 remove them once nothing references them)
- Modify: `src/silver_transform.py`'s `__main__` block (write the two new tables, before the existing `silver_users = ...` line)
- Test: `tests/test_data_quality.py`

**Interfaces:**
- Produces: `build_account_types(spark) -> DataFrame` with columns `account_type_id` (bigint), `type_name` (string), `category` (string), `description` (string).
- Produces: `build_transaction_types(spark) -> DataFrame` with columns `transaction_type_id` (bigint), `type_name` (string), `direction` (string), `description` (string).
- Produces: `spark` pytest fixture (session-scoped, local, no Delta/UC packages loaded - these functions and everything else tested in this plan are pure DataFrame transforms with no catalog dependency).

- [ ] **Step 1: Create the pytest Spark fixture**

Create `tests/conftest.py`:

```python
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# tests/ is a sibling of src/, not inside it.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("chip-lakehouse-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
```

This is deliberately a plain SparkSession - no `spark.jars.packages`, no Delta, no Unity Catalog config. Every function this plan tests takes DataFrames in and returns a DataFrame out; none of them call `spark.table(...)` or touch the catalog. That makes these tests fast (no Ivy dependency resolution, no Docker/UC container required) and independent of whether the UC server is running.

- [ ] **Step 2: Write the failing test**

Create `tests/test_data_quality.py`:

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd fintech-lakehouse && finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_account_types'`

- [ ] **Step 4: Add the seed lists and builder functions**

In `src/silver_transform.py`, immediately after the existing line
`TRANSACTION_TYPES = ["deposit", "withdrawal", "roundup", "investment_contribution"]`, add:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: `2 passed`

- [ ] **Step 6: Wire the new tables into `__main__`**

In `src/silver_transform.py`, in the `if __name__ == "__main__":` block, immediately after the `token = load_uc_token()` line and before the existing `silver_users = transform_users(...)` line, add:

```python
    account_types = build_account_types(spark)
    write_delta_table(token, account_types, "silver", "account_types")
    print(f"silver.account_types: {account_types.count()} rows")

    transaction_types = build_transaction_types(spark)
    write_delta_table(token, transaction_types, "silver", "transaction_types")
    print(f"silver.transaction_types: {transaction_types.count()} rows")

```

- [ ] **Step 7: Run the full script against the real stack**

Requires Docker running: `cd docker && docker compose up -d` (skip if already up).

Run: `cd src && ../finenv/bin/python3 silver_transform.py`
Expected: prints `silver.account_types: 3 rows` and `silver.transaction_types: 4 rows`, followed by the existing four tables' unchanged output (nothing else in the script has changed yet).

- [ ] **Step 8: Commit**

```bash
git add tests/conftest.py tests/test_data_quality.py src/silver_transform.py
git commit -m "Add Inmon lookup table builders for account/transaction types

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `transform_accounts` joins against the lookup instead of `isin()`

**Files:**
- Modify: `src/silver_transform.py:92-108` (the `transform_accounts` function)
- Modify: `src/silver_transform.py`'s `__main__` block (the `silver_accounts = transform_accounts(...)` call)
- Test: `tests/test_data_quality.py`

**Interfaces:**
- Consumes: `build_account_types(spark) -> DataFrame` from Task 1.
- Produces: `transform_accounts(bronze_accounts: DataFrame, silver_users: DataFrame, account_types: DataFrame) -> DataFrame` - note the new third parameter; `silver.accounts` now has `account_type_id` (bigint) instead of `account_type` (string). All other columns (`account_id`, `user_id`, `account_number_masked`, `opened_date`) are unchanged.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_data_quality.py`:

```python
from pyspark.sql import Row
from pyspark.sql import functions as F

from silver_transform import build_account_types, transform_accounts


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
```

Note: this second test exercises `quarantine()`, which writes a CSV to the
real `data/quarantine/accounts_bad_type/` directory (gitignored). That's
accepted here, not a bug to fix - matching how this function has already
been manually verified once before in this project.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k transform_accounts`
Expected: FAIL - `transform_accounts()` takes 2 positional arguments but 3 were given

- [ ] **Step 3: Rewrite `transform_accounts`**

In `src/silver_transform.py`, replace the entire `transform_accounts` function (lines 92-108) with:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k transform_accounts`
Expected: `2 passed`

- [ ] **Step 5: Update the `__main__` call site**

In `src/silver_transform.py`'s `__main__` block, change:
```python
    silver_accounts = transform_accounts(bronze_table(spark, "accounts"), silver_users)
```
to:
```python
    silver_accounts = transform_accounts(bronze_table(spark, "accounts"), silver_users, account_types)
```

- [ ] **Step 6: Run the full script against the real stack**

Run: `cd src && ../finenv/bin/python3 silver_transform.py`
Expected: no errors. Verify the schema changed:
```bash
../finenv/bin/python3 -c "
from spark_session import get_spark, CATALOG_NAME
spark = get_spark()
spark.table(f'{CATALOG_NAME}.silver.accounts').printSchema()
"
```
Expected output includes `account_type_id: long` and does NOT include `account_type`.

Note: `gold_marts.py` is now broken until Task 4 - it still references
`silver.accounts.account_type`, which no longer exists. That's expected
mid-plan state, not a regression to fix here.

- [ ] **Step 7: Commit**

```bash
git add src/silver_transform.py tests/test_data_quality.py
git commit -m "Normalize silver.accounts.account_type into a lookup FK

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `transform_transactions` joins against the lookup instead of `isin()`

**Files:**
- Modify: `src/silver_transform.py:111-128` (the `transform_transactions` function)
- Modify: `src/silver_transform.py`'s `__main__` block (the `silver_transactions = transform_transactions(...)` call)
- Modify: `src/silver_transform.py` (remove the now-unused `ACCOUNT_TYPES`/`TRANSACTION_TYPES` lists - nothing references them after this task)
- Test: `tests/test_data_quality.py`

**Interfaces:**
- Consumes: `build_transaction_types(spark) -> DataFrame` from Task 1.
- Produces: `transform_transactions(bronze_transactions: DataFrame, silver_accounts: DataFrame, transaction_types: DataFrame) -> DataFrame` - new third parameter; `silver.transactions` now has `transaction_type_id` (bigint) instead of `transaction_type` (string). All other columns unchanged.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_data_quality.py`:

```python
from pyspark.sql import Row
from pyspark.sql import functions as F

from silver_transform import build_transaction_types, transform_transactions


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


def test_transform_transactions_quarantines_unrecognized_type(spark):
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k transform_transactions`
Expected: FAIL - `transform_transactions()` takes 2 positional arguments but 3 were given

- [ ] **Step 3: Rewrite `transform_transactions`**

In `src/silver_transform.py`, replace the entire `transform_transactions` function (lines 111-128) with:

```python
def transform_transactions(bronze_transactions: DataFrame, silver_accounts: DataFrame, transaction_types: DataFrame) -> DataFrame:
    deduped = dedupe_latest(bronze_transactions, ["transaction_id"])

    lookup = transaction_types.select("transaction_type_id", "type_name")
    bad_type = deduped.join(lookup, deduped.transaction_type == lookup.type_name, "left_anti")
    quarantine(bad_type, "transactions_bad_type")

    typed = (
        deduped.join(lookup, deduped.transaction_type == lookup.type_name, "inner")
        .select(
            "transaction_id",
            "account_id",
            "transaction_type_id",
            F.col("amount").cast(DecimalType(10, 2)).alias("amount"),
            "currency",
            F.col("transaction_ts").cast("timestamp"),
        )
    )

    parent_keys = silver_accounts.select("account_id")
    quarantine(typed.join(parent_keys, "account_id", "left_anti"), "transactions_orphans")
    return typed.join(parent_keys, "account_id", "left_semi")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k transform_transactions`
Expected: `2 passed`

- [ ] **Step 5: Remove the now-unused enum lists**

In `src/silver_transform.py`, delete these two lines (nothing references them anymore - `build_account_types`/`build_transaction_types` use `ACCOUNT_TYPE_SEED`/`TRANSACTION_TYPE_SEED` instead):
```python
ACCOUNT_TYPES = ["savings", "investment", "pension"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "roundup", "investment_contribution"]
```

- [ ] **Step 6: Update the `__main__` call site**

In `src/silver_transform.py`'s `__main__` block, change:
```python
    silver_transactions = transform_transactions(bronze_table(spark, "transactions"), silver_accounts)
```
to:
```python
    silver_transactions = transform_transactions(bronze_table(spark, "transactions"), silver_accounts, transaction_types)
```

- [ ] **Step 7: Run the full script against the real stack**

Run: `cd src && ../finenv/bin/python3 silver_transform.py`
Expected: no errors, all six silver tables print a row count.

Verify the schema:
```bash
../finenv/bin/python3 -c "
from spark_session import get_spark, CATALOG_NAME
spark = get_spark()
spark.table(f'{CATALOG_NAME}.silver.transactions').printSchema()
"
```
Expected output includes `transaction_type_id: long` and does NOT include `transaction_type`.

`gold_marts.py` is still broken (Task 4 fixes it) - this is expected.

- [ ] **Step 8: Commit**

```bash
git add src/silver_transform.py tests/test_data_quality.py
git commit -m "Normalize silver.transactions.transaction_type into a lookup FK

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: `gold_marts.py` classifies by `direction` and resolves the account-type label

**Files:**
- Modify: `src/gold_marts.py:34-80` (remove `INFLOW_TRANSACTION_TYPES`, rewrite `build_account_summary`)
- Modify: `src/gold_marts.py`'s `__main__` block (the `build_account_summary(...)` call)
- Test: `tests/test_data_quality.py`

**Interfaces:**
- Consumes: `build_account_types`/`build_transaction_types` from Task 1 (for tests); `silver.account_types`/`silver.transaction_types` tables from Task 1 (for `__main__`).
- Produces: `build_account_summary(silver_accounts: DataFrame, silver_transactions: DataFrame, account_types: DataFrame, transaction_types: DataFrame) -> DataFrame` - two new parameters. Output shape is unchanged (`account_id`, `user_id`, `account_type`, `opened_date`, `total_inflows`, `total_outflows`, `balance`, `transaction_count`, `first_transaction_ts`, `last_transaction_ts`) - `account_type` is still a readable string, resolved via join rather than carried through directly.
- `build_customer_360` is unchanged - it only consumes `account_summary`'s already-resolved output, never touches `transaction_type`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_data_quality.py`:

```python
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from gold_marts import build_account_summary
from silver_transform import build_account_types, build_transaction_types


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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k build_account_summary`
Expected: FAIL - `build_account_summary()` takes 2 positional arguments but 4 were given

- [ ] **Step 3: Remove `INFLOW_TRANSACTION_TYPES` and rewrite `build_account_summary`**

In `src/gold_marts.py`, delete this block:
```python
# Everything increases an account's balance except a withdrawal - a
# roundup sweeps spare change *into* the account, same direction as a
# deposit or an investment contribution.
INFLOW_TRANSACTION_TYPES = ["deposit", "roundup", "investment_contribution"]
```

Then replace the entire `build_account_summary` function with:

```python
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k build_account_summary`
Expected: `1 passed`

- [ ] **Step 5: Update the `__main__` call site**

In `src/gold_marts.py`'s `__main__` block, change:
```python
    account_summary = build_account_summary(
        silver_table(spark, "accounts"), silver_table(spark, "transactions")
    )
```
to:
```python
    account_summary = build_account_summary(
        silver_table(spark, "accounts"),
        silver_table(spark, "transactions"),
        silver_table(spark, "account_types"),
        silver_table(spark, "transaction_types"),
    )
```

- [ ] **Step 6: Run the full test suite**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: all tests pass (this task's plus Tasks 1-3's).

- [ ] **Step 7: Consolidate imports at the top of the test file**

Tasks 1-4 each appended their own import lines where their test functions
were added, so `tests/test_data_quality.py` now has `from pyspark.sql import Row`,
`from pyspark.sql import functions as F`, and the `silver_transform`/`gold_marts`
imports repeated in several places. Move all imports to a single block at
the top of the file (deduplicated), leaving every test function body
otherwise unchanged. The top of the file should read:

```python
from decimal import Decimal

from pyspark.sql import Row
from pyspark.sql import functions as F

from gold_marts import build_account_summary
from silver_transform import (
    build_account_types,
    build_transaction_types,
    transform_accounts,
    transform_transactions,
)
```

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: all tests still pass - this step only moves import statements, it changes no logic.

- [ ] **Step 8: Commit**

```bash
git add src/gold_marts.py tests/test_data_quality.py
git commit -m "Classify gold.account_summary inflow/outflow via silver.transaction_types

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: End-to-end verification against the real stack

**Files:** none modified - this task is verification only.

**Interfaces:** none produced - terminal task.

- [ ] **Step 1: Ensure the Docker Unity Catalog server is running**

```bash
docker info >/dev/null 2>&1 || open -a Docker
cd docker && docker compose up -d
```

- [ ] **Step 2: Capture the pre-refactor gold numbers were already verified**

This project's `gold.account_summary`/`gold.customer_360` were already
verified once, before this refactor, with these known-good facts (from
prior verification in this project): `account_summary` had 4,020 rows,
`customer_360` had 2,000 rows, and every user's `customer_360.total_balance`
exactly equalled the sum of their own `account_summary.balance` rows with
zero mismatches. This step re-proves those same facts hold after the
refactor - a regression check, not a new correctness bar.

- [ ] **Step 3: Re-run the full pipeline**

```bash
cd src
../finenv/bin/python3 silver_transform.py
../finenv/bin/python3 gold_marts.py
```
Expected: no errors, no unexpected `quarantined N row(s)` lines (the real
generated data has no bad type values by construction).

- [ ] **Step 4: Verify row counts and FK resolution**

```bash
../finenv/bin/python3 -c "
from spark_session import get_spark, CATALOG_NAME
spark = get_spark()

print('account_types:', spark.table(f'{CATALOG_NAME}.silver.account_types').count())
print('transaction_types:', spark.table(f'{CATALOG_NAME}.silver.transaction_types').count())
print('silver.accounts:', spark.table(f'{CATALOG_NAME}.silver.accounts').count())
print('silver.transactions:', spark.table(f'{CATALOG_NAME}.silver.transactions').count())
print('gold.account_summary:', spark.table(f'{CATALOG_NAME}.gold.account_summary').count())
print('gold.customer_360:', spark.table(f'{CATALOG_NAME}.gold.customer_360').count())

# Every account_type_id/transaction_type_id must resolve to a real lookup row.
accounts = spark.table(f'{CATALOG_NAME}.silver.accounts')
account_types = spark.table(f'{CATALOG_NAME}.silver.account_types')
orphan_account_types = accounts.join(account_types, 'account_type_id', 'left_anti').count()
print('accounts with unresolvable account_type_id (expect 0):', orphan_account_types)

transactions = spark.table(f'{CATALOG_NAME}.silver.transactions')
transaction_types = spark.table(f'{CATALOG_NAME}.silver.transaction_types')
orphan_txn_types = transactions.join(transaction_types, 'transaction_type_id', 'left_anti').count()
print('transactions with unresolvable transaction_type_id (expect 0):', orphan_txn_types)
"
```
Expected: `account_types: 3`, `transaction_types: 4`, `silver.accounts: 4020`,
`silver.transactions: 110604` (or `110813` if the incremental-transactions
demo from earlier in this project has been run since - either is correct,
just confirm it matches whatever bronze currently holds), `gold.account_summary: 4020`,
`gold.customer_360: 2000`, and both orphan counts are `0`.

- [ ] **Step 5: Verify the customer_360/account_summary interoperability claim still holds**

```bash
../finenv/bin/python3 -c "
from pyspark.sql import functions as F
from spark_session import get_spark, CATALOG_NAME
spark = get_spark()

acc = spark.table(f'{CATALOG_NAME}.gold.account_summary')
cust = spark.table(f'{CATALOG_NAME}.gold.customer_360')

recomputed = acc.groupBy('user_id').agg(F.sum('balance').alias('recomputed_total'))
mismatch = (
    cust.join(recomputed, 'user_id', 'left')
    .withColumn('diff', F.col('total_balance') - F.coalesce(F.col('recomputed_total'), F.lit(0)))
    .filter(F.col('diff') != 0)
)
print('mismatched users (expect 0):', mismatch.count())
"
```
Expected: `mismatched users (expect 0): 0` - proves the refactor didn't
break the cross-mart consistency guarantee `gold_marts.py`'s design relies on.

- [ ] **Step 6: Run the full pytest suite one final time**

```bash
finenv/bin/python3 -m pytest tests/test_data_quality.py -v
```
Expected: all tests pass.

- [ ] **Step 7: Commit any remaining changes**

If Steps 1-6 required no code changes (they shouldn't - this is a
verification-only task), there is nothing to commit. If any step surfaced
a real bug, fix it, add a regression test for it in `tests/test_data_quality.py`,
then commit:
```bash
git add -A
git commit -m "Fix: <describe the specific bug found during end-to-end verification>

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```
