# Kimball Star Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a conformed Kimball star (`dim_date`, `dim_transaction_type`, SCD2 `dim_customer` / `dim_account`, `fct_transaction`, `fct_account_monthly_snapshot`) to the `gold` schema alongside the existing wide marts, plus a cross-layer `tests/verify_e2e.py`.

**Architecture:** One new module `src/gold_star.py`, peer of `gold_marts.py`. Static dims and both facts are full-overwrite via the existing `uc_delta.write_delta_table`. The two SCD2 dims persist across runs: a pure `plan_scd2()` planner (unit-tested) decides inserts/expiries, and a thin `merge_scd2()` wrapper executes a Delta `MERGE` to close changed rows plus an append for new versions. Facts resolve dimension keys with point-in-time joins on the SCD2 `[valid_from, valid_to)` ranges.

**Tech Stack:** PySpark 3.5.3, Delta Lake 3.2.0 (`delta-spark`, `DeltaTable` API), Unity Catalog OSS via REST (`uc_delta.py`), pytest with the existing catalog-free `spark` fixture.

**Spec:** [docs/superpowers/specs/2026-09-06-kimball-star-schema-design.md](../specs/2026-09-06-kimball-star-schema-design.md)

## Global Constraints

- Every table is created via `uc_delta.write_delta_table()`, or (for the SCD2 dims) written as Delta files + registered via `uc_delta.register_uc_table()`. Never `df.write.saveAsTable()` / CTAS (unitycatalog-spark 0.2.1's table-creation path is broken - see `bronze_ingest.py`'s docstring).
- All tables land in the existing `gold` schema, prefixed `dim_` / `fct_`. No new schema, no `iam/access.yaml` change, no `spark_session.py` change.
- Do **not** modify `src/gold_marts.py`, `src/silver_transform.py`, `src/bronze_ingest.py`.
- Dimensional surrogate keys (`customer_key`, `account_key`) are pipeline-generated integers; the source UUID (`user_id`, `account_id`) is retained alongside as the business key. `dim_date.date_key` / `dim_transaction_type.transaction_type_key` reuse an existing integer (`yyyymmdd`; `transaction_type_id`).
- SCD2 columns, exact names/types: `valid_from` (date, inclusive), `valid_to` (date, exclusive; `9999-12-31` when open), `is_current` (boolean), `row_hash` (sha2-256 hex of the sorted tracked attributes joined with `||`, nulls rendered as `∅`).
- The test `spark` fixture in `tests/conftest.py` stays Delta-free. `merge_scd2()` and its `DeltaTable` call are **not** unit-tested; they are covered by Task 7 (real run) and Task 8 (`verify_e2e.py`).
- `gold_star.py` runs after `silver_transform.py`. It has no ordering constraint with `gold_marts.py`.
- Money columns in the star are `decimal(12,2)` (matching `gold_marts.py`), not silver's `decimal(10,2)`.

---

## Task 1: Module scaffold + `dim_date`

**Files:**
- Create: `src/gold_star.py`
- Modify: `tests/test_data_quality.py` (add import + one test)

**Interfaces:**
- Produces: `build_dim_date(spark, start: datetime.date, end: datetime.date) -> DataFrame` with columns `date_key` (int, `yyyymmdd`), `date` (date), `year` (int), `quarter` (int), `month` (int), `month_name` (string), `day_of_month` (int), `day_of_week` (int, 1=Mon..7=Sun), `day_name` (string), `week_of_year` (int), `is_weekend` (boolean), `is_month_end` (boolean). One row per calendar day in `[start, end]` inclusive.
- Produces: `silver_table(spark, name)` / `gold_table(spark, name)` helpers (same shape as `gold_marts.py`).
- Produces: `silver_date_bounds(silver_users, silver_accounts, silver_transactions) -> tuple[date, date]` returning `(trunc-to-month of min, last-day-of-month of max)` across all silver dates.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_data_quality.py` (imports go at the top with the others):

```python
from datetime import date

from gold_star import build_dim_date
```

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_build_dim_date_spans_and_flags -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gold_star'`.

- [ ] **Step 3: Create `src/gold_star.py` with the scaffold and `build_dim_date`**

```python
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


# Spark's dayofweek() is 1=Sunday..7=Saturday. Remap to ISO 1=Monday..7=Sunday.
_ISO_DOW = ((F.dayofweek(F.col("date")) + 5) % 7) + 1


def build_dim_date(spark, start: date, end: date) -> DataFrame:
    """One row per calendar day in [start, end] inclusive. Callers should
    pass month-snapped bounds (trunc-to-month / last_day) so every
    month-end date_key the snapshot fact emits is covered.
    """
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
        _ISO_DOW.alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        (_ISO_DOW >= 6).alias("is_weekend"),
        (F.col("date") == F.last_day("date")).alias("is_month_end"),
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_build_dim_date_spans_and_flags -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add gold_star module scaffold and build_dim_date

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `dim_transaction_type`

**Files:**
- Modify: `src/gold_star.py` (add `build_dim_transaction_type`)
- Modify: `tests/test_data_quality.py` (add import + one test)

**Interfaces:**
- Consumes: a `silver.transaction_types`-shaped DataFrame (`transaction_type_id` int, `type_name` string, `direction` string, `description` string).
- Produces: `build_dim_transaction_type(silver_transaction_types: DataFrame) -> DataFrame` with columns `transaction_type_key` (= `transaction_type_id`), `type_name`, `direction`, `description`.

- [ ] **Step 1: Write the failing test**

Add the import (`build_dim_transaction_type`) to the `gold_star` import line, then:

```python
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_build_dim_transaction_type_passthrough -v`
Expected: FAIL with `ImportError: cannot import name 'build_dim_transaction_type'`.

- [ ] **Step 3: Implement**

Add to `src/gold_star.py`:

```python
def build_dim_transaction_type(silver_transaction_types: DataFrame) -> DataFrame:
    return silver_transaction_types.select(
        F.col("transaction_type_id").alias("transaction_type_key"),
        "type_name",
        "direction",
        "description",
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_build_dim_transaction_type_passthrough -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add build_dim_transaction_type

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `plan_scd2` - the pure SCD2 planner

**Files:**
- Modify: `src/gold_star.py` (add `_row_hash`, `plan_scd2`)
- Modify: `tests/test_data_quality.py` (add import + four tests)

**Interfaces:**
- Produces: `plan_scd2(incoming, current_target, *, business_key, tracked_cols, effective_from_col, surrogate_key, run_date) -> tuple[DataFrame, DataFrame]`.
  - `incoming`: one row per `business_key`; carries `business_key`, every column in `tracked_cols`, `effective_from_col`, and any other attribute columns. `effective_from_col` **stays** in the output as its own column *and* seeds `valid_from`.
  - `current_target`: the existing dim (full target schema) or `None` on first load.
  - Returns `(rows_to_insert, business_keys_to_expire)`. `rows_to_insert` has schema `[surrogate_key, *incoming.columns, row_hash, valid_from, valid_to, is_current]` in that order. `business_keys_to_expire` has one column: `business_key`.

- [ ] **Step 1: Write the failing tests**

Add `plan_scd2` to the `gold_star` import line, then:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k plan_scd2`
Expected: FAIL with `ImportError: cannot import name 'plan_scd2'`.

- [ ] **Step 3: Implement**

Add to `src/gold_star.py`:

```python
_SCD2_OPEN = F.lit(date(9999, 12, 31)).cast("date")


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
            _SCD2_OPEN.alias("valid_to"),
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
```

Note: `_finalize` selects `*carry` before `_valid_from`/`_exists`/`_cur_hash` are on the frame in the initial-load branch, and in the subsequent-load branch `new_or_changed` still has all `carry` columns plus the extras - `*carry` picks only the originals, so `_valid_from` etc. never leak into the output.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k plan_scd2`
Expected: `4 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add plan_scd2 SCD2 delta planner

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: `merge_scd2` - the Delta MERGE wrapper

**Files:**
- Modify: `src/gold_star.py` (add `_delta_log_exists`, `merge_scd2`)
- Modify: `tests/test_data_quality.py` (add import + one test for `_delta_log_exists` only)

**Interfaces:**
- Consumes: `plan_scd2` (Task 3); `uc_delta.get_uc_table`, `uc_delta.register_uc_table`, `uc_delta.write_delta_table`, `uc_delta.LAKEHOUSE_DIR`.
- Produces: `_delta_log_exists(location: str) -> bool` (pure).
- Produces: `merge_scd2(token, spark, incoming, schema, table, *, business_key, tracked_cols, effective_from_col, surrogate_key) -> None`. First run creates the table via `write_delta_table`; later runs expire changed current rows with a Delta `MERGE` and append new versions, then re-register.

**No automated test for `merge_scd2` itself** - the `tests/conftest.py` `spark` fixture is deliberately Delta-free (see spec). It is exercised for real in Task 7 and asserted by Task 8's `verify_e2e.py` (SCD2 integrity checks) plus Task 7's explicit run-it-twice idempotency check.

- [ ] **Step 1: Write the failing test**

Add `_delta_log_exists` to the `gold_star` import line, then:

```python
def test_delta_log_exists(tmp_path):
    loc = f"file://{tmp_path}"
    assert not _delta_log_exists(loc)
    (tmp_path / "_delta_log").mkdir()
    assert _delta_log_exists(loc)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_delta_log_exists -v`
Expected: FAIL with `ImportError: cannot import name '_delta_log_exists'`.

- [ ] **Step 3: Implement**

Add to `src/gold_star.py`:

```python
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py::test_delta_log_exists -v`
Expected: PASS.

- [ ] **Step 5: Run the full unit suite (nothing regressed)**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add merge_scd2 Delta MERGE wrapper for SCD2 dimensions

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: `build_fct_transaction`

**Files:**
- Modify: `src/gold_star.py` (add `_date_key`, `build_fct_transaction`)
- Modify: `tests/test_data_quality.py` (add import + three tests)

**Interfaces:**
- Consumes: `silver.transactions` (`transaction_id`, `account_id`, `transaction_type_id`, `amount` decimal, `currency`, `transaction_ts` timestamp); a `silver.transaction_types`-shaped frame; `dim_customer` and `dim_account` (must contain at least `customer_key`/`account_key`, the business key, `user_id`, `valid_from`, `valid_to`).
- Produces: `build_fct_transaction(silver_transactions, silver_transaction_types, dim_customer, dim_account) -> DataFrame` with columns `date_key` (int), `customer_key` (int), `account_key` (int), `transaction_type_key` (int), `transaction_id` (string, degenerate), `amount` (decimal(12,2)), `signed_amount` (decimal(12,2)), `currency` (string). Row count equals `silver_transactions` row count.

- [ ] **Step 1: Write the failing tests**

Add `build_fct_transaction` to the `gold_star` import line and `from decimal import Decimal` if not already imported, then:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k build_fct_transaction`
Expected: FAIL with `ImportError: cannot import name 'build_fct_transaction'`.

- [ ] **Step 3: Implement**

Add to `src/gold_star.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k build_fct_transaction`
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add build_fct_transaction with point-in-time dimension joins

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 6: `build_fct_account_monthly_snapshot`

**Files:**
- Modify: `src/gold_star.py` (add `build_fct_account_monthly_snapshot`)
- Modify: `tests/test_data_quality.py` (add import + two tests, then consolidate imports)

**Interfaces:**
- Consumes: `silver.transactions`; `silver.accounts` (`account_id`, `user_id`, `opened_date`); a `silver.transaction_types`-shaped frame; `dim_customer`, `dim_account` (same key/range columns as Task 5).
- Produces: `build_fct_account_monthly_snapshot(silver_transactions, silver_accounts, silver_transaction_types, dim_customer, dim_account) -> DataFrame` with columns `date_key` (int, month-end `yyyymmdd`), `customer_key` (int), `account_key` (int), `month_inflow` (decimal(12,2)), `month_outflow` (decimal(12,2)), `month_net` (decimal(12,2)), `closing_balance` (decimal(12,2)), `transaction_count` (int). One row per account per month from `trunc(opened_date,'month')` to the global max transaction month, inclusive - **including zero-activity months**.

- [ ] **Step 1: Write the failing tests**

Add `build_fct_account_monthly_snapshot` to the `gold_star` import line, then:

```python
def test_build_fct_account_monthly_snapshot_running_balance_and_gap(spark):
    stt = spark.createDataFrame(
        [(1, "inflow"), (2, "outflow")], ["transaction_type_id", "direction"]
    )
    st = _txns(spark, [
        ("t1", "a1", 1, Decimal("100.00"), "GBP", "2026-01-15 10:00:00"),  # Jan
        ("t2", "a1", 2, Decimal("30.00"), "GBP", "2026-03-10 10:00:00"),   # Mar; Feb empty
    ])
    sa = spark.createDataFrame(
        [("a1", "u1", date(2026, 1, 1))], ["account_id", "user_id", "opened_date"]
    )
    da = _dim_account_row(spark, [(10, "a1", "u1", date(2020, 1, 1), date(9999, 12, 31))])
    dc = _dim_customer_row(spark, [(20, "u1", date(2019, 1, 1), date(9999, 12, 31))])

    rows = {
        r["date_key"]: r
        for r in build_fct_account_monthly_snapshot(st, sa, stt, dc, da).collect()
    }
    assert set(rows) == {20260131, 20260228, 20260331}
    assert rows[20260131]["closing_balance"] == Decimal("100.00")
    assert rows[20260228]["closing_balance"] == Decimal("100.00")  # gap month carries forward
    assert rows[20260228]["transaction_count"] == 0
    assert rows[20260331]["closing_balance"] == Decimal("70.00")
    assert rows[20260331]["month_outflow"] == Decimal("30.00")
    assert rows[20260331]["month_net"] == Decimal("-30.00")
    assert rows[20260131]["customer_key"] == 20 and rows[20260131]["account_key"] == 10


def test_build_fct_account_monthly_snapshot_month_end_date_key(spark):
    stt = spark.createDataFrame([(1, "inflow")], ["transaction_type_id", "direction"])
    st = _txns(spark, [("t1", "a1", 1, Decimal("50.00"), "GBP", "2026-04-20 10:00:00")])
    sa = spark.createDataFrame(
        [("a1", "u1", date(2026, 4, 1))], ["account_id", "user_id", "opened_date"]
    )
    da = _dim_account_row(spark, [(10, "a1", "u1", date(2020, 1, 1), date(9999, 12, 31))])
    dc = _dim_customer_row(spark, [(20, "u1", date(2019, 1, 1), date(9999, 12, 31))])

    out = build_fct_account_monthly_snapshot(st, sa, stt, dc, da).collect()
    assert len(out) == 1
    assert out[0]["date_key"] == 20260430  # April month-end
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k monthly_snapshot`
Expected: FAIL with `ImportError: cannot import name 'build_fct_account_monthly_snapshot'`.

- [ ] **Step 3: Implement**

Add to `src/gold_star.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k monthly_snapshot`
Expected: `2 passed`.

- [ ] **Step 5: Consolidate imports at the top of the test file**

Tasks 1-6 each appended their own imports where the tests were added. Move every import to one block at the top of `tests/test_data_quality.py`, deduplicated, bodies unchanged. The block should read:

```python
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
    build_fct_account_monthly_snapshot,
    build_fct_transaction,
    plan_scd2,
)
from silver_transform import (
    build_account_types,
    build_transaction_types,
    transform_accounts,
    transform_transactions,
)
```

- [ ] **Step 6: Run the full unit suite**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: all tests pass (SP1's plus Tasks 1-6's - 24 total).

- [ ] **Step 7: Commit**

```bash
git add src/gold_star.py tests/test_data_quality.py
git commit -m "Add build_fct_account_monthly_snapshot and consolidate test imports

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 7: `__main__` orchestration + real run

**Files:**
- Modify: `src/gold_star.py` (add the `if __name__ == "__main__"` block)

**Interfaces:**
- Consumes: every builder from Tasks 1-6, plus `merge_scd2`.
- Produces: six tables in `chip_lakehouse.gold` - `dim_date`, `dim_transaction_type`, `dim_customer`, `dim_account`, `fct_transaction`, `fct_account_monthly_snapshot`.

**Requires:** Docker Unity Catalog stack running, and `data/raw/*.csv` present. This task runs the real pipeline; there is no unit test.

- [ ] **Step 1: Add the orchestration block**

Append to `src/gold_star.py`:

```python
if __name__ == "__main__":
    spark = get_spark()
    token = load_uc_token()

    s_users = silver_table(spark, "users")
    s_accounts = silver_table(spark, "accounts")
    s_transactions = silver_table(spark, "transactions")
    s_account_types = silver_table(spark, "account_types")
    s_transaction_types = silver_table(spark, "transaction_types")

    start, end = silver_date_bounds(s_users, s_accounts, s_transactions)
    dim_date = build_dim_date(spark, start, end)
    write_delta_table(token, dim_date, "gold", "dim_date")
    print(f"gold.dim_date: {dim_date.count()} rows")

    dim_txn_type = build_dim_transaction_type(s_transaction_types)
    write_delta_table(token, dim_txn_type, "gold", "dim_transaction_type")
    print(f"gold.dim_transaction_type: {dim_txn_type.count()} rows")

    customer_input = s_users.select("user_id", "age_band", "signup_date")
    merge_scd2(
        token, spark, customer_input, "gold", "dim_customer",
        business_key="user_id",
        tracked_cols=["age_band", "signup_date"],
        effective_from_col="signup_date",
        surrogate_key="customer_key",
    )
    dim_customer = gold_table(spark, "dim_customer")
    print(
        f"gold.dim_customer: {dim_customer.count()} rows "
        f"({dim_customer.where(F.col('is_current')).count()} current)"
    )

    account_input = s_accounts.join(s_account_types, "account_type_id", "inner").select(
        "account_id",
        "user_id",
        "account_number_masked",
        "opened_date",
        F.col("type_name").alias("account_type"),
        F.col("category").alias("account_type_category"),
    )
    merge_scd2(
        token, spark, account_input, "gold", "dim_account",
        business_key="account_id",
        tracked_cols=[
            "account_number_masked", "opened_date",
            "account_type", "account_type_category",
        ],
        effective_from_col="opened_date",
        surrogate_key="account_key",
    )
    dim_account = gold_table(spark, "dim_account")
    print(
        f"gold.dim_account: {dim_account.count()} rows "
        f"({dim_account.where(F.col('is_current')).count()} current)"
    )

    fct_txn = build_fct_transaction(
        s_transactions, s_transaction_types, dim_customer, dim_account
    )
    write_delta_table(token, fct_txn, "gold", "fct_transaction")
    print(f"gold.fct_transaction: {fct_txn.count()} rows")

    fct_snap = build_fct_account_monthly_snapshot(
        s_transactions, s_accounts, s_transaction_types, dim_customer, dim_account
    )
    write_delta_table(token, fct_snap, "gold", "fct_account_monthly_snapshot")
    print(f"gold.fct_account_monthly_snapshot: {fct_snap.count()} rows")
```

- [ ] **Step 2: Bring the stack up and run the prerequisite layers**

```bash
docker info >/dev/null 2>&1 || open -a Docker   # wait until it responds
cd docker && docker compose up -d && cd ..
cd src
../finenv/bin/python3 spark_session.py       # ensure catalog/schemas exist
../finenv/bin/python3 bronze_ingest.py
../finenv/bin/python3 silver_transform.py
../finenv/bin/python3 gold_marts.py
cd ..
```
Expected: each prints row counts, no tracebacks. (Skip `bronze_ingest.py` / `silver_transform.py` / `gold_marts.py` only if you have just run them and silver/gold are current.)

- [ ] **Step 3: Run `gold_star.py`**

Run: `cd src && ../finenv/bin/python3 gold_star.py && cd ..`
Expected: six `gold.<table>: N rows` lines. Sanity: `dim_transaction_type` = 4; `dim_customer` = 2000 (2000 current); `dim_account` = 4020 (4020 current); `fct_transaction` equals the `silver.transactions` count printed in Step 2; `dim_date` and `fct_account_monthly_snapshot` in the low-thousands / ~120-150k respectively.

- [ ] **Step 4: Run it a second time - idempotency check**

Run: `cd src && ../finenv/bin/python3 gold_star.py && cd ..`
Expected: identical row counts to Step 3. In particular `dim_customer` stays `2000 rows (2000 current)` and `dim_account` stays `4020 rows (4020 current)` - the MERGE path produced zero new versions on unchanged silver.

- [ ] **Step 5: Commit**

```bash
git add src/gold_star.py
git commit -m "Wire gold_star.py __main__ orchestration for the star schema

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 8: `tests/verify_e2e.py` - cross-layer invariant checks

**Files:**
- Create: `tests/verify_e2e.py`

**Interfaces:**
- Consumes: the live `chip_lakehouse` catalog (`silver.*`, `gold.*`) after a full pipeline run.
- Produces: a standalone script (`python3 tests/verify_e2e.py`) that prints `PASS` / `FAIL <detail>` per check and exits non-zero if any check fails.

**Requires:** Docker stack up and Tasks 1-7 done, with `bronze_ingest.py` → `silver_transform.py` → `gold_marts.py` → `gold_star.py` all run against the current data.

- [ ] **Step 1: Create the script**

```python
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
```

- [ ] **Step 2: Run it**

Run: `finenv/bin/python3 tests/verify_e2e.py`
Expected: every line `PASS`, final line `N/N checks passed`, exit code 0.
If any check FAILs, the star build has a real bug - fix it in `src/gold_star.py`, re-run `src/gold_star.py`, and re-run this script before committing.

- [ ] **Step 3: Commit**

```bash
git add tests/verify_e2e.py
git commit -m "Add tests/verify_e2e.py cross-layer invariant checks (testing tier 4)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 9: Documentation

**Files:**
- Modify: `docs/standard.md`
- Modify: `README.md`

- [ ] **Step 1: Update `docs/standard.md` layer contract**

In the `## Layer contracts` section, change the `gold` table row's `Enforced by` cell from `[gold_marts.py](../src/gold_marts.py) (pending)` to `[gold_marts.py](../src/gold_marts.py), [gold_star.py](../src/gold_star.py)`, and add this paragraph immediately after the table:

```markdown
The `gold` schema holds two shapes side by side: pre-aggregated wide
marts (`account_summary`, `customer_360`, from `gold_marts.py`) and a
conformed Kimball star (`dim_*` / `fct_*`, from `gold_star.py`). Both are
built from silver; the star is reconciled against the marts by
`tests/verify_e2e.py` so the two can't silently diverge.
```

- [ ] **Step 2: Update `docs/standard.md` naming conventions**

Under `**Table names**` (the `## Naming conventions` section), add:

```markdown
- Dimensional tables in `gold` are prefixed `dim_` / `fct_`
  (`dim_customer`, `fct_transaction`). This is the one place a table name
  carries a shape prefix - the star schema is a deliberate second
  modelling style layered over the same silver, and the prefix is how a
  consumer tells a conformed dimension from a wide mart at a glance.
```

Under `**Columns**`, add:

```markdown
- Dimensional surrogate keys: `<entity>_key`, a pipeline-generated
  integer, used only in the `gold` star (`customer_key`, `account_key`).
  The source UUID is retained alongside as the business key (`user_id`,
  `account_id`). This is the only sanctioned non-UUID, pipeline-assigned
  key; it exists to support SCD2 versioning and is never exposed as a
  source identifier. (`dim_date.date_key` is a `yyyymmdd` smart integer;
  `dim_transaction_type.transaction_type_key` reuses
  `silver.transaction_types.transaction_type_id`.)
- SCD2 history columns, on dimensions that track change (`dim_customer`,
  `dim_account`): `valid_from` (date, inclusive), `valid_to` (date,
  exclusive; `9999-12-31` while open), `is_current` (boolean), `row_hash`
  (sha2-256 hex of the tracked attributes, for change detection).
```

- [ ] **Step 3: Update `docs/standard.md` testing strategy**

At the end of the `## Testing strategy` section, add:

```markdown
**4. End-to-end invariant checks against the live catalog** ([tests/verify_e2e.py](../tests/verify_e2e.py))
A standalone script (not a pytest case - it needs the Docker Unity
Catalog stack and a full pipeline run). Checks cross-layer invariants no
unit test can see: silver FK/enum resolution, row-count conservation
from `silver.transactions` into `gold.fct_transaction`, the
`customer_360` / `account_summary` balance agreement, Kimball star
reconciliation (`sum(fct_transaction.signed_amount)` and the latest
monthly-snapshot `closing_balance` both tie to `account_summary.balance`),
fact-to-dimension FK integrity, and SCD2 range integrity. Prints
`PASS` / `FAIL` per check and exits non-zero on any failure. Run it after
`gold_star.py`.
```

- [ ] **Step 4: Update `README.md`**

In the `## Architecture` section's `Schemas` bullet, after "`gold` (business-level marts)" add ", including a conformed Kimball star (`dim_*` / `fct_*`)".

Add a new section after `## Setup` (before `## Project layout`):

```markdown
## Running the pipeline

After setup, run the stages in order (each writes into the `chip_lakehouse` catalog):

```bash
python3 data_gen/generate_data.py     # synthetic CSVs (first run only)
python3 src/bronze_ingest.py          # raw -> bronze
python3 src/silver_transform.py       # bronze -> silver (cleaned, normalized)
python3 src/gold_marts.py             # silver -> gold wide marts
python3 src/gold_star.py              # silver -> gold Kimball star (dim_*/fct_*)
python3 tests/verify_e2e.py           # cross-layer invariant checks
```

`gold_marts.py` and `gold_star.py` are independent and can run in either
order. `gold_star.py`'s `dim_customer` / `dim_account` persist across
runs (SCD2); every other gold table is a full overwrite.
```

- [ ] **Step 5: Commit**

```bash
git add docs/standard.md README.md
git commit -m "Document the gold Kimball star schema and verify_e2e (testing tier 4)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Placement (module, `gold` schema, prefixes, no IAM/session change) → Tasks 1, 9, Global Constraints. ✓
- `dim_date` (schema, `yyyymmdd` key, month-snapped range, attributes) → Task 1. ✓
- `dim_transaction_type` (reuse `transaction_type_id`) → Task 2. ✓
- `dim_customer` / `dim_account` schemas (surrogate + BK + SCD2 cols, `user_id` attribute on `dim_account`, `account_type`/`category` denormalized) → Task 7 inputs + Task 3 output schema. ✓
- `plan_scd2` (row_hash formula, first-load, new/changed/unchanged, key = max+row_number, valid_from rules) → Task 3. ✓
- `merge_scd2` (first-load create, Delta MERGE expire with `run_date` literal, append, re-register) → Task 4. ✓
- `fct_transaction` (grain, FKs, degenerate `transaction_id`, `signed_amount`, PIT joins, count preserved) → Task 5. ✓
- `fct_account_monthly_snapshot` (month spine incl. zero-activity, semi-additive `closing_balance`, month-end `date_key`, PIT joins) → Task 6. ✓
- Orchestration (read silver from catalog, dims then facts, print counts) → Task 7. ✓
- Reconciliation checks 1-6 → Task 8. ✓
- Unit tests (11 listed) → Tasks 1-6 (dim_date 1, dim_txn_type 1, plan_scd2 4, `_delta_log_exists` 1, fct_transaction 3, monthly_snapshot 2 = 12; the extra is `_delta_log_exists`, an addition beyond the spec's list, kept because it's a cheap guard on real logic). ✓
- `verify_e2e.py` as testing tier 4 → Task 8 + Task 9 Step 3. ✓
- Doc updates (standard.md layer/naming/testing, README) → Task 9. ✓
- Rollout (additive, no migration, run after silver) → Task 7 Step 2, Task 9 Step 4. ✓
- Out of scope (no goal dim/fact, no `gold_marts.py` change, no currency junk dim) → respected; no task touches them. ✓

**Placeholder scan:** No `TBD`/`TODO`/"handle edge cases"/"similar to Task N". Every code step has complete code. ✓

**Type consistency:**
- `plan_scd2` / `merge_scd2` keyword params (`business_key`, `tracked_cols`, `effective_from_col`, `surrogate_key`, `run_date`) identical in Tasks 3, 4, 7. ✓
- `build_fct_transaction(silver_transactions, silver_transaction_types, dim_customer, dim_account)` - defined Task 5, called Task 7 & Task 8 helpers with the same arg order. ✓
- `build_fct_account_monthly_snapshot(silver_transactions, silver_accounts, silver_transaction_types, dim_customer, dim_account)` - defined Task 6, called Task 7 with the same order. ✓
- `_date_key` helper defined once (Task 5), reused in Task 6. ✓
- Output column names (`date_key`, `customer_key`, `account_key`, `transaction_type_key`, `signed_amount`, `closing_balance`, `month_net`, …) consistent between builders (Tasks 5-6), orchestration (Task 7), and checks (Task 8). ✓
- `dim_date` column set consistent between Task 1 impl and Task 8's `date_key` uniqueness / coverage checks. ✓

No issues found.
