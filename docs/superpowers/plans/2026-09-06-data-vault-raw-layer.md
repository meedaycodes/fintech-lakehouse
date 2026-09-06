# Data Vault Raw Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an insert-only Data Vault 2.0 raw layer (`vault` schema: 4 hubs, 3 links, 4 satellites, 2 reference tables) as a parallel branch off `bronze`, plus `vault_checks()` in `tests/verify_e2e.py` that reconciles it back to `silver`.

**Architecture:** One new module `src/vault_load.py`, peer of `gold_star.py`. It reads `bronze.*`, dedupes each entity by `_ingested_at` (reusing `silver_transform.dedupe_latest`), computes SHA-256 hash keys / hash diffs, and appends only the delta to each vault table. Pure functions (`new_rows_by_key`, `changed_sat_rows`, `current_sat`, the hashing helpers) are unit-tested with synthetic rows; a thin `__main__` does the catalog I/O. Vault tables persist across runs — first run seeds, later runs append. `silver` is not touched or re-pointed; the two branches are reconciled by `verify_e2e.py`.

**Tech Stack:** PySpark 3.5.3, Delta Lake 3.2.0, Unity Catalog OSS via REST (`uc_delta.py`), pytest with the existing catalog-free `spark` fixture.

**Spec:** [docs/superpowers/specs/2026-09-06-data-vault-raw-layer-design.md](../specs/2026-09-06-data-vault-raw-layer-design.md)

## Global Constraints

- New schema `chip_lakehouse.vault`. Add `"vault"` to the `CREATE SCHEMA IF NOT EXISTS` loop in `src/spark_session.py`. Add it to `iam/access.yaml` as **`data-engineer`-only** (`USE SCHEMA, CREATE TABLE, SELECT, MODIFY`); **no `data-analyst` entry** — the vault is raw, exactly as `bronze` is treated.
- Every table is created via `uc_delta.write_delta_table()` (which registers through `register_uc_table()`), never `df.write.saveAsTable()` / CTAS — the `unitycatalog-spark` 0.2.1 table-creation path is broken (see `src/bronze_ingest.py`'s docstring).
- Vault tables **persist across runs**. First load of a table: `write_delta_table(..., mode="overwrite")`. Every later load: `write_delta_table(..., mode="append")` and only when the computed delta is non-empty. A re-run on unchanged `bronze` appends nothing.
- Do **not** modify `src/bronze_ingest.py`, `src/silver_transform.py` (except importing from it), `src/gold_marts.py`, `src/gold_star.py`, or the two wide marts.
- Hash helper, matching `gold_star.py`'s `_row_hash`: `F.sha2(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("∅")) for c in cols]), 256)`. Output is 64-char lowercase hex, stored as `string`.
  - **Hash keys** (`hub.*_hk`, `link.*_hk`): `trim` each business-key column first, hash in a **fixed column order** (hubs: the one business key; links: parents in the order the Links table lists).
  - **Hash diffs** (`sat.hash_diff`): the tracked attribute columns cast to string, in **`sorted()` by-name order**, nulls `∅`, **not trimmed**.
- Satellite attribute typing: `amount` / `target_amount` / `current_amount` → `decimal(10,2)`; `date_of_birth` / `signup_date` / `opened_date` / `created_date` / `target_date` → `date`; `transaction_ts` → `timestamp`. Every other attribute lands as `bronze` inferred it.
- `record_source` is the string `"bronze.<table>"` (the child entity's bronze table for links).
- `load_date` is pinned once per run: `LOAD_DATE = datetime.now()` at the top of `__main__`, written as `F.lit(LOAD_DATE).cast("timestamp")`.
- Before hashing, dedupe each raw `bronze` table to one row per business key by `_ingested_at` descending — reuse `from silver_transform import dedupe_latest`.
- PII: `sat_user_details` carries only `date_of_birth` and `signup_date`. `full_name`, `email`, `address` are never selected into any vault table.
- The `tests/conftest.py` `spark` fixture is Delta-free. `__main__` and `verify_e2e.py` are **not** unit-tested — they are covered by Task 4 (real run) and Task 5 (reconciliation).
- `vault_load.py` runs after `bronze_ingest.py`. It has no ordering constraint with `silver_transform.py` / `gold_marts.py` / `gold_star.py`.

---

## Task 1: Config, module scaffold, hashing helpers

**Files:**
- Modify: `src/spark_session.py` (add `"vault"` to the schema loop)
- Modify: `iam/access.yaml` (add `vault` under `data-engineer`)
- Create: `src/vault_load.py`
- Modify: `tests/test_data_quality.py` (add imports + three tests)

**Interfaces:**
- Produces: `bronze_table(spark, name) -> DataFrame`, `vault_table(spark, token, name) -> DataFrame | None` (None when the table is not registered in UC).
- Produces: `_hash(*cols)` — takes `Column` objects, returns a `Column` (the sha2 hex).
- Produces: `add_hash_key(df, bk_cols: list[str], hk_col: str) -> DataFrame` — adds `hk_col`, trimming each `bk_cols` entry, hashing in the given order.
- Produces: `add_hash_diff(df, attr_cols: list[str], out_col: str = "hash_diff") -> DataFrame` — adds `out_col`, hashing `sorted(attr_cols)`.

- [ ] **Step 1: Add `vault` to the schema bootstrap**

In `src/spark_session.py`, the last line of the `__main__` block:

```python
    for schema in ["bronze", "silver", "gold", "ml", "vault"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")
```

- [ ] **Step 2: Add the `vault` grant to `iam/access.yaml`**

Under the `data-engineer@chip-lakehouse.local` entry's `schema_privileges:`, add a `vault` line so the block reads:

```yaml
    schema_privileges:
      bronze: [USE SCHEMA, CREATE TABLE, SELECT, MODIFY]
      silver: [USE SCHEMA, CREATE TABLE, SELECT, MODIFY]
      gold: [USE SCHEMA, CREATE TABLE, SELECT, MODIFY]
      ml: [USE SCHEMA, CREATE TABLE, SELECT, MODIFY]
      vault: [USE SCHEMA, CREATE TABLE, SELECT, MODIFY]
```

Leave the `data-analyst` block unchanged — no `vault` entry, same as `bronze`.

- [ ] **Step 3: Write the failing tests**

Add to the import block at the top of `tests/test_data_quality.py`:

```python
from pyspark.sql import functions as F  # already imported - leave as-is

from vault_load import add_hash_diff, add_hash_key, _hash
```

Add these tests at the end of the file:

```python
def test_hash_is_deterministic_and_order_sensitive(spark):
    df = spark.createDataFrame([("a", "b")], ["x", "y"])
    got = df.select(
        _hash(F.col("x"), F.col("y")).alias("xy"),
        _hash(F.col("y"), F.col("x")).alias("yx"),
        _hash(F.col("x"), F.col("y")).alias("xy2"),
    ).first()
    assert got["xy"] == got["xy2"]          # deterministic
    assert got["xy"] != got["yx"]           # order matters
    assert len(got["xy"]) == 64             # sha-256 hex


def test_add_hash_key_trims_business_key(spark):
    df = spark.createDataFrame([("  u1  ", "u1")], ["padded", "clean"])
    out = add_hash_key(df, ["clean"], "clean_hk")
    out = add_hash_key(out.withColumnRenamed("padded", "bk"), ["bk"], "padded_hk")
    row = out.first()
    assert row["clean_hk"] == row["padded_hk"]   # trim makes them equal


def test_add_hash_diff_is_column_order_independent(spark):
    df = spark.createDataFrame([("x", "y")], ["b", "a"])
    row = add_hash_diff(df, ["a", "b"]).first()
    row2 = add_hash_diff(df, ["b", "a"]).first()
    assert row["hash_diff"] == row2["hash_diff"]  # sorted() by name, so arg order is irrelevant
    assert len(row["hash_diff"]) == 64
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k "hash"`
Expected: FAIL with `ModuleNotFoundError: No module named 'vault_load'`.

- [ ] **Step 5: Create `src/vault_load.py`**

```python
"""Builds an insert-only Data Vault 2.0 raw layer in the vault schema,
as a parallel branch off bronze (not a stage between bronze and silver).

Run after src/bronze_ingest.py; no ordering constraint with
silver_transform.py / gold_marts.py / gold_star.py:
    python3 src/vault_load.py

Tables (all in the vault schema):
  hub_user / hub_account / hub_transaction / hub_savings_goal
      one row per distinct business key ever seen
  link_account_user / link_transaction_account / link_savings_goal_user
      one row per distinct relationship ever seen
  sat_user_details / sat_account_details / sat_transaction_details
  sat_savings_goal_details
      insert-only attribute history; a new row only when hash_diff changes
  ref_account_type / ref_transaction_type
      the same closed enumerations silver holds, seeded from
      silver_transform.py's constants

Why this layer exists: bronze is full-overwrite, so a corrected or
withdrawn source row destroys the prior value; silver's dedupe collapses
history to one row per key. The vault keeps every version, every
relationship, and the source of each, and is never updated in place.
"current" is derived at query time (current_sat). verify_e2e.py
reconstructs current-state entities from the vault and asserts they match
silver, proving the raw layer loses nothing.

See docs/superpowers/specs/2026-09-06-data-vault-raw-layer-design.md.
"""
from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from silver_transform import (
    build_account_types,
    build_transaction_types,
    dedupe_latest,
)
from spark_session import CATALOG_NAME, get_spark, load_uc_token
from uc_delta import get_uc_table, write_delta_table


def bronze_table(spark, name: str) -> DataFrame:
    return spark.table(f"{CATALOG_NAME}.bronze.{name}")


def vault_table(spark, token: str, name: str) -> "DataFrame | None":
    """The registered vault table, or None if it has never been loaded."""
    if get_uc_table(token, "vault", name) is None:
        return None
    return spark.table(f"{CATALOG_NAME}.vault.{name}")


def _hash(*cols):
    """sha2-256 hex of the given Columns, joined with '||', nulls -> '∅'.
    Matches gold_star.py's _row_hash convention.
    """
    parts = [F.coalesce(c.cast("string"), F.lit("∅")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def add_hash_key(df: DataFrame, bk_cols: list, hk_col: str) -> DataFrame:
    """Adds hk_col = _hash of the trimmed bk_cols, in the given order."""
    return df.withColumn(
        hk_col, _hash(*[F.trim(F.col(c).cast("string")) for c in bk_cols])
    )


def add_hash_diff(df: DataFrame, attr_cols: list, out_col: str = "hash_diff") -> DataFrame:
    """Adds out_col = _hash of attr_cols in sorted-by-name order (not trimmed)."""
    return df.withColumn(out_col, _hash(*[F.col(c) for c in sorted(attr_cols)]))
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k "hash"`
Expected: `3 passed`.

- [ ] **Step 7: Commit**

```bash
git add src/spark_session.py iam/access.yaml src/vault_load.py tests/test_data_quality.py
git commit -m "Add vault schema, IAM grant, and vault_load hashing helpers

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 2: `new_rows_by_key` — the hub/link insert delta

**Files:**
- Modify: `src/vault_load.py` (add `new_rows_by_key`)
- Modify: `tests/test_data_quality.py` (add import + three tests)

**Interfaces:**
- Produces: `new_rows_by_key(incoming: DataFrame, existing: "DataFrame | None", key_col: str) -> DataFrame`. Returns the rows of `incoming` (deduped to one per `key_col`) whose `key_col` is absent from `existing`. When `existing is None`, returns `incoming` deduped. Column set is unchanged from `incoming`. Used for both hubs and links.

- [ ] **Step 1: Write the failing tests**

Add `new_rows_by_key` to the `vault_load` import line, then:

```python
def test_new_rows_by_key_first_load_returns_all_once(spark):
    incoming = spark.createDataFrame(
        [("hk1", "u1"), ("hk2", "u2"), ("hk1", "u1")], ["user_hk", "user_id"]
    )
    out = new_rows_by_key(incoming, None, "user_hk")
    assert sorted(r["user_hk"] for r in out.collect()) == ["hk1", "hk2"]


def test_new_rows_by_key_returns_only_unseen(spark):
    incoming = spark.createDataFrame(
        [("hk1", "u1"), ("hk2", "u2"), ("hk3", "u3")], ["user_hk", "user_id"]
    )
    existing = spark.createDataFrame([("hk1",), ("hk2",)], ["user_hk"])
    out = new_rows_by_key(incoming, existing, "user_hk")
    assert [r["user_hk"] for r in out.collect()] == ["hk3"]


def test_new_rows_by_key_all_seen_is_empty(spark):
    incoming = spark.createDataFrame([("hk1", "u1")], ["user_hk", "user_id"])
    existing = spark.createDataFrame([("hk1",)], ["user_hk"])
    assert new_rows_by_key(incoming, existing, "user_hk").count() == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k new_rows_by_key`
Expected: FAIL with `ImportError: cannot import name 'new_rows_by_key'`.

- [ ] **Step 3: Implement**

Add to `src/vault_load.py`:

```python
def new_rows_by_key(
    incoming: DataFrame, existing: "DataFrame | None", key_col: str
) -> DataFrame:
    """Rows of incoming (one per key_col) whose key_col is not in existing."""
    incoming = incoming.dropDuplicates([key_col])
    if existing is None:
        return incoming
    return incoming.join(existing.select(key_col), key_col, "left_anti")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k new_rows_by_key`
Expected: `3 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/vault_load.py tests/test_data_quality.py
git commit -m "Add new_rows_by_key for hub/link insert deltas

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 3: `changed_sat_rows` + `current_sat`

**Files:**
- Modify: `src/vault_load.py` (add `changed_sat_rows`, `current_sat`)
- Modify: `tests/test_data_quality.py` (add import + five tests)

**Interfaces:**
- Produces: `changed_sat_rows(incoming: DataFrame, existing: "DataFrame | None", key_col: str) -> DataFrame`. `incoming` has columns `key_col`, `load_date`, `hash_diff`, `record_source`, and the tracked attributes. Returns the rows of `incoming` (one per `key_col`) whose `hash_diff` differs from that `key_col`'s latest `hash_diff` in `existing` (latest by `load_date`), plus every `key_col` absent from `existing`. `existing is None` → `incoming` deduped. Output columns == `incoming` columns.
- Produces: `current_sat(sat: DataFrame, key_col: str) -> DataFrame`. Returns one row per `key_col` — the greatest `load_date` (ties broken by `hash_diff`). Output columns == `sat` columns.

- [ ] **Step 1: Write the failing tests**

Add `changed_sat_rows, current_sat` to the `vault_load` import line. Add `from datetime import datetime` to the test import block if not already present (it is not — `date` is). Then:

```python
def _sat_in(spark, rows):
    # rows: list of (hk, load_date: datetime, hash_diff, attr)
    return spark.createDataFrame(
        rows, ["user_hk", "load_date", "hash_diff", "attr"]
    )


def test_changed_sat_rows_first_load_returns_all(spark):
    incoming = _sat_in(spark, [
        ("hk1", datetime(2026, 1, 1), "d1", "a"),
        ("hk2", datetime(2026, 1, 1), "d2", "b"),
    ])
    out = changed_sat_rows(incoming, None, "user_hk")
    assert sorted(r["user_hk"] for r in out.collect()) == ["hk1", "hk2"]


def test_changed_sat_rows_unchanged_is_noop(spark):
    existing = _sat_in(spark, [("hk1", datetime(2026, 1, 1), "d1", "a")])
    incoming = _sat_in(spark, [("hk1", datetime(2026, 2, 1), "d1", "a")])
    assert changed_sat_rows(incoming, existing, "user_hk").count() == 0


def test_changed_sat_rows_changed_attribute_returns_row(spark):
    existing = _sat_in(spark, [("hk1", datetime(2026, 1, 1), "d1", "a")])
    incoming = _sat_in(spark, [("hk1", datetime(2026, 2, 1), "d2", "b")])
    out = changed_sat_rows(incoming, existing, "user_hk").collect()
    assert len(out) == 1 and out[0]["hash_diff"] == "d2"


def test_changed_sat_rows_compares_against_latest_existing_version(spark):
    # hk1 history: d1 then d2. Incoming d1 again = a real change from the
    # current (d2) state, so it must come through.
    existing = _sat_in(spark, [
        ("hk1", datetime(2026, 1, 1), "d1", "a"),
        ("hk1", datetime(2026, 2, 1), "d2", "b"),
    ])
    incoming = _sat_in(spark, [("hk1", datetime(2026, 3, 1), "d1", "a")])
    out = changed_sat_rows(incoming, existing, "user_hk").collect()
    assert len(out) == 1 and out[0]["hash_diff"] == "d1"


def test_current_sat_picks_greatest_load_date(spark):
    sat = _sat_in(spark, [
        ("hk1", datetime(2026, 1, 1), "d1", "a"),
        ("hk1", datetime(2026, 2, 1), "d2", "b"),
        ("hk2", datetime(2026, 1, 1), "d9", "z"),
    ])
    rows = {r["user_hk"]: r["hash_diff"] for r in current_sat(sat, "user_hk").collect()}
    assert rows == {"hk1": "d2", "hk2": "d9"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k "changed_sat_rows or current_sat"`
Expected: FAIL with `ImportError: cannot import name 'changed_sat_rows'`.

- [ ] **Step 3: Implement**

Add to `src/vault_load.py`:

```python
def _latest_by_load_date(df: DataFrame, key_col: str) -> DataFrame:
    w = Window.partitionBy(key_col).orderBy(
        F.col("load_date").desc(), F.col("hash_diff").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def changed_sat_rows(
    incoming: DataFrame, existing: "DataFrame | None", key_col: str
) -> DataFrame:
    """Rows of incoming (one per key_col) whose hash_diff differs from that
    key's latest hash_diff in existing, plus keys absent from existing.
    """
    incoming = incoming.dropDuplicates([key_col])
    if existing is None:
        return incoming
    latest = _latest_by_load_date(existing, key_col).select(
        key_col, F.col("hash_diff").alias("_cur_hd")
    )
    return (
        incoming.join(latest, key_col, "left")
        .where(F.col("_cur_hd").isNull() | (F.col("hash_diff") != F.col("_cur_hd")))
        .drop("_cur_hd")
    )


def current_sat(sat: DataFrame, key_col: str) -> DataFrame:
    """One row per key_col - the greatest load_date (ties broken by hash_diff)."""
    return _latest_by_load_date(sat, key_col)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v -k "changed_sat_rows or current_sat"`
Expected: `5 passed`.

- [ ] **Step 5: Run the full unit suite (nothing regressed)**

Run: `finenv/bin/python3 -m pytest tests/test_data_quality.py -v`
Expected: all tests pass (Kimball's 27 + Tasks 1-3's 11 = 38).

- [ ] **Step 6: Commit**

```bash
git add src/vault_load.py tests/test_data_quality.py
git commit -m "Add changed_sat_rows and current_sat for satellite history

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 4: `__main__` orchestration + real run

**Files:**
- Modify: `src/vault_load.py` (add config lists + the `if __name__ == "__main__"` block)

**Interfaces:**
- Consumes: `bronze_table`, `vault_table`, `add_hash_key`, `add_hash_diff`, `new_rows_by_key`, `changed_sat_rows`, `dedupe_latest`, `build_account_types`, `build_transaction_types`, `write_delta_table`.
- Produces: 13 tables in `chip_lakehouse.vault` — 4 `hub_*`, 3 `link_*`, 4 `sat_*`, 2 `ref_*`.

**Requires:** Docker Unity Catalog stack running and `bronze.*` loaded. No unit test — Task 5 asserts the result.

- [ ] **Step 1: Add the config lists**

Add to `src/vault_load.py`, after the helper functions:

```python
# (hub table, bronze table, business key, hash-key column)
HUBS = [
    ("hub_user", "users", "user_id", "user_hk"),
    ("hub_account", "accounts", "account_id", "account_hk"),
    ("hub_transaction", "transactions", "transaction_id", "transaction_hk"),
    ("hub_savings_goal", "savings_goals", "goal_id", "savings_goal_hk"),
]

# (link table, bronze table, [(business key, its hub-key column), ...] in
# hash order, link hash-key column). The first pair's business key is the
# child entity's PK - used to dedupe the bronze rows.
LINKS = [
    ("link_account_user", "accounts",
     [("account_id", "account_hk"), ("user_id", "user_hk")], "account_user_hk"),
    ("link_transaction_account", "transactions",
     [("transaction_id", "transaction_hk"), ("account_id", "account_hk")],
     "transaction_account_hk"),
    ("link_savings_goal_user", "savings_goals",
     [("goal_id", "savings_goal_hk"), ("user_id", "user_hk")], "savings_goal_user_hk"),
]

# (sat table, bronze table, business key, hub-key column, [attribute cols],
# {attribute col: cast type}). Attributes not in the cast map land as
# bronze inferred them.
SATS = [
    ("sat_user_details", "users", "user_id", "user_hk",
     ["date_of_birth", "signup_date"],
     {"date_of_birth": "date", "signup_date": "date"}),
    ("sat_account_details", "accounts", "account_id", "account_hk",
     ["account_type", "account_number", "opened_date"],
     {"opened_date": "date"}),
    ("sat_transaction_details", "transactions", "transaction_id", "transaction_hk",
     ["transaction_type", "amount", "currency", "transaction_ts"],
     {"amount": "decimal(10,2)", "transaction_ts": "timestamp"}),
    ("sat_savings_goal_details", "savings_goals", "goal_id", "savings_goal_hk",
     ["goal_name", "target_amount", "current_amount", "created_date", "target_date"],
     {"target_amount": "decimal(10,2)", "current_amount": "decimal(10,2)",
      "created_date": "date", "target_date": "date"}),
]
```

- [ ] **Step 2: Add the orchestration block**

Append to `src/vault_load.py`:

```python
def _write(spark, token, delta: DataFrame, name: str, first_load: bool) -> int:
    n = delta.count()
    if first_load:
        write_delta_table(token, delta, "vault", name, mode="overwrite")
    elif n > 0:
        write_delta_table(token, delta, "vault", name, mode="append")
    total = spark.table(f"{CATALOG_NAME}.vault.{name}").count()
    print(f"vault.{name}: +{n} rows ({total} total)")
    return n


if __name__ == "__main__":
    spark = get_spark()
    token = load_uc_token()
    LOAD_DATE = datetime.now()

    def _load_date_col():
        return F.lit(LOAD_DATE).cast("timestamp")

    # --- reference tables (closed lists, plain overwrite) ---
    write_delta_table(token, build_account_types(spark), "vault", "ref_account_type")
    write_delta_table(token, build_transaction_types(spark), "vault", "ref_transaction_type")
    print("vault.ref_account_type / vault.ref_transaction_type: seeded")

    # --- hubs ---
    for hub, btbl, bk, hk in HUBS:
        b = dedupe_latest(bronze_table(spark, btbl), [bk])
        incoming = add_hash_key(b, [bk], hk).select(
            hk, bk,
            _load_date_col().alias("load_date"),
            F.lit(f"bronze.{btbl}").alias("record_source"),
        )
        existing = vault_table(spark, token, hub)
        _write(spark, token, new_rows_by_key(incoming, existing, hk), hub, existing is None)

    # --- links ---
    for link, btbl, pairs, link_hk in LINKS:
        child_bk = pairs[0][0]
        b = dedupe_latest(bronze_table(spark, btbl), [child_bk])
        for bk, hk in pairs:
            b = add_hash_key(b, [bk], hk)
        b = add_hash_key(b, [pair[0] for pair in pairs], link_hk)
        incoming = b.select(
            link_hk, *[hk for _, hk in pairs],
            _load_date_col().alias("load_date"),
            F.lit(f"bronze.{btbl}").alias("record_source"),
        )
        existing = vault_table(spark, token, link)
        _write(spark, token, new_rows_by_key(incoming, existing, link_hk), link, existing is None)

    # --- satellites ---
    for sat, btbl, bk, hk, attrs, casts in SATS:
        b = dedupe_latest(bronze_table(spark, btbl), [bk])
        b = add_hash_key(b, [bk], hk)
        for c, t in casts.items():
            b = b.withColumn(c, F.col(c).cast(t))
        b = add_hash_diff(b, attrs)
        incoming = b.select(
            hk, _load_date_col().alias("load_date"), "hash_diff",
            F.lit(f"bronze.{btbl}").alias("record_source"), *attrs,
        )
        existing = vault_table(spark, token, sat)
        _write(spark, token, changed_sat_rows(incoming, existing, hk), sat, existing is None)
```

Note: `_write` and every builder take `spark` explicitly, matching how `gold_star.py`'s `__main__` passes its session into `gold_table(spark, ...)`.

- [ ] **Step 3: Bring the stack up and load the prerequisite layer**

```bash
docker info >/dev/null 2>&1 || open -a Docker   # wait until it responds
cd docker && docker compose up -d && cd ..
cd src
../finenv/bin/python3 spark_session.py       # creates the vault schema
../finenv/bin/python3 bronze_ingest.py
cd ..
```
Expected: `bronze_ingest.py` prints four `ingested N rows` lines, no tracebacks. (Skip `bronze_ingest.py` only if bronze is already current.)

- [ ] **Step 4: Run `vault_load.py`**

Run: `cd src && ../finenv/bin/python3 vault_load.py && cd ..`
Expected: a `seeded` line, then 11 `vault.<table>: +N rows (N total)` lines. Sanity against a standard 2000-user generate: `hub_user` 2000, `hub_account` ~4020, `hub_transaction` = the `bronze.transactions` row count, `hub_savings_goal` ~1200; each `link_*` equals its child hub's count; each `sat_*` current count equals its hub's count (every key gets exactly one version on a first load).

- [ ] **Step 5: Run it a second time — idempotency check**

Run: `cd src && ../finenv/bin/python3 vault_load.py && cd ..`
Expected: every hub/link/sat line reads `+0 rows`, and the `(N total)` figures are identical to Step 4.

- [ ] **Step 6: Commit**

```bash
git add src/vault_load.py
git commit -m "Wire vault_load.py __main__ orchestration for hubs, links, satellites

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 5: `tests/verify_e2e.py` — `vault_checks()`

**Files:**
- Modify: `tests/verify_e2e.py` (add `vault_checks`, call it from `main()`)

**Interfaces:**
- Consumes: the live `chip_lakehouse` catalog (`bronze.*`, `silver.*`, `vault.*`) after `bronze_ingest.py` → `silver_transform.py` → `vault_load.py`.
- Consumes: `current_sat` from `vault_load`.
- Produces: `vault_checks(spark) -> list[tuple[str, bool, str]]`, appended to `main()`'s result list.

**Requires:** Docker stack up, and `bronze_ingest.py` → `silver_transform.py` → `vault_load.py` all run against the current data.

- [ ] **Step 1: Add the import**

At the top of `tests/verify_e2e.py`, after the existing `from spark_session import ...` line:

```python
from vault_load import current_sat  # noqa: E402
```

- [ ] **Step 2: Add `vault_checks`**

Add this function above `def main():`:

```python
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
        cond = None
        for v_col, s_col in pairs:
            c = recon[v_col] != silver_df[s_col]
            cond = c if cond is None else (cond | c)
        mism = recon.join(silver_df, join_key, "inner").where(cond).count()
        rc, sc = recon.count(), silver_df.count()
        out.append(check(label, rc == sc and mism == 0, f"{rc} vs {sc} rows, {mism} value mismatches"))

    # users: age_band recomputed from date_of_birth with transform_users' formula
    su = current_sat(_t(spark, "vault", "sat_user_details"), "user_hk").join(hub_user, "user_hk", "inner")
    age_years = F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
    band_start = (F.floor(age_years / 10) * 10).cast("int")
    su = su.select(
        "user_id",
        F.concat(band_start, F.lit("-"), band_start + 9).alias("age_band_v"),
        F.col("signup_date").alias("signup_date_v"),
    )
    _recon_check("vault users reconstruct == silver.users", su, s_users, "user_id",
                 [("age_band_v", "age_band"), ("signup_date_v", "signup_date")])

    # accounts: masked number recomputed, account_type resolved via ref
    sa = current_sat(_t(spark, "vault", "sat_account_details"), "account_hk").join(hub_account, "account_hk", "inner")
    sa = (
        sa.join(ref_at.select(F.col("type_name").alias("account_type"), "account_type_id"), "account_type", "inner")
        .select(
            "account_id",
            F.col("account_type_id").alias("account_type_id_v"),
            F.concat(F.lit("****"), F.substring(F.col("account_number"), -4, 4)).alias("account_number_masked_v"),
            F.col("opened_date").alias("opened_date_v"),
        )
    )
    _recon_check("vault accounts reconstruct == silver.accounts", sa, s_accounts, "account_id",
                 [("account_type_id_v", "account_type_id"),
                  ("account_number_masked_v", "account_number_masked"),
                  ("opened_date_v", "opened_date")])

    # transactions: amount/currency/ts equal, transaction_type resolved via ref
    st = current_sat(_t(spark, "vault", "sat_transaction_details"), "transaction_hk").join(hub_transaction, "transaction_hk", "inner")
    st = (
        st.join(ref_tt.select(F.col("type_name").alias("transaction_type"), "transaction_type_id"), "transaction_type", "inner")
        .select(
            "transaction_id",
            F.col("transaction_type_id").alias("transaction_type_id_v"),
            F.col("amount").cast("decimal(10,2)").alias("amount_v"),
            F.col("currency").alias("currency_v"),
            F.col("transaction_ts").alias("transaction_ts_v"),
        )
    )
    _recon_check("vault transactions reconstruct == silver.transactions", st, s_txns, "transaction_id",
                 [("transaction_type_id_v", "transaction_type_id"), ("amount_v", "amount"),
                  ("currency_v", "currency"), ("transaction_ts_v", "transaction_ts")])

    # savings_goals: all typed columns equal
    sg = current_sat(_t(spark, "vault", "sat_savings_goal_details"), "savings_goal_hk").join(hub_goal, "savings_goal_hk", "inner")
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
```

- [ ] **Step 3: Call it from `main()`**

Change the `results =` line in `main()` to:

```python
    results = (
        silver_checks(spark)
        + gold_mart_checks(spark)
        + gold_star_checks(spark)
        + vault_checks(spark)
    )
```

- [ ] **Step 4: Run it**

Run: `finenv/bin/python3 tests/verify_e2e.py`
Expected: every line `PASS`, final line `N/N checks passed`, exit code 0.
If any vault check FAILs, fix `src/vault_load.py`, re-run `src/vault_load.py`, and re-run this script before committing.

- [ ] **Step 5: Commit**

```bash
git add tests/verify_e2e.py
git commit -m "Reconcile the vault raw layer to silver in verify_e2e.py

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 6: Documentation

**Files:**
- Modify: `docs/standard.md`
- Modify: `README.md`

- [ ] **Step 1: `docs/standard.md` — layer contract row**

In the `## Layer contracts` table, add a row after the `gold` row:

```markdown
| `vault` | Data Vault 2.0 raw layer: insert-only hubs / links / satellites, hash-keyed, source-tagged, never updated in place. A parallel branch off `bronze`, not a stage before `silver`. Keeps the full history `bronze` overwrites and `silver` dedupes away. | [vault_load.py](../src/vault_load.py) |
```

- [ ] **Step 2: `docs/standard.md` — contract + benefits prose**

Immediately after the layer-contracts table (after the "Bronze is intentionally dumb…" paragraph), add:

```markdown
The `vault` schema is a second, independent branch off `bronze`, built by
`vault_load.py` and reconciled to `silver` by `tests/verify_e2e.py`. It
exists for what neither `bronze` nor `silver` keeps:

- **Non-destructive history.** `bronze` is full-overwrite; `silver`
  dedupes to one row per key. Vault satellites are insert-only, keyed
  `(hub_hk, load_date)`, and a new version lands only when the tracked
  attributes' `hash_diff` changes — so every past state is still there.
- **Provenance.** `record_source` and `load_date` on every hub, link and
  satellite row record which load each came from.
- **Resilience to source change.** A new source attribute is a new
  satellite; a new relationship is a new link. Hubs and existing
  satellites never change and need no reload.
- **Deterministic keys.** A hub key is `sha2` of the business key —
  identical every run, no lookup — and links are built by hashing their
  parents' keys, not by joining.

"Current" is not stored; it is derived at query time (`current_sat`:
newest `load_date` per hub key). The vault never re-points `silver`; the
two coexist.
```

- [ ] **Step 3: `docs/standard.md` — naming conventions**

Under `**Catalog/schema/table**`, after the `dim_` / `fct_` bullet added by sub-project 2, add:

```markdown
- Data Vault tables in `vault` are prefixed `hub_` (business-key list),
  `link_` (relationship), `sat_` (attribute history) or `ref_`
  (seeded reference list). Like `dim_` / `fct_`, this is a deliberate
  second modelling style over the same source, and the prefix names the
  construct.
```

Under `**Columns**`, after the SCD2 history bullet, add:

```markdown
- Data Vault hash columns, in the `vault` schema: `<entity>_hk` (a
  `sha2`-256 hex of the trimmed business key — the hub/link surrogate),
  `hash_diff` (a `sha2`-256 hex of a satellite's tracked attributes, for
  change detection), `load_date` (timestamp the row was inserted),
  `record_source` (the `bronze.<table>` a row came from). Hash keys are
  never exposed as source identifiers; the source UUID stays alongside in
  the hub.
```

- [ ] **Step 4: `docs/standard.md` — testing strategy**

At the end of the `## Testing strategy` section (after the tier-4 paragraph from sub-project 2), add:

```markdown
The tier-4 script also reconciles the `vault` raw layer to `silver`:
it reconstructs each current-state entity from `hub ⨝ current_sat(sat)`,
recomputes the columns `silver` derives (`age_band`,
`account_number_masked`), and asserts row counts and every other value
match `silver` — so the Data Vault is a provable loss-free rebuild point,
not just an additional copy.
```

- [ ] **Step 5: `README.md` — schemas bullet**

In `## Architecture`, change the `Schemas` bullet's tail so it reads:

```markdown
- **Schemas**: `bronze` (raw ingest) → `silver` (cleaned/conformed) → `gold`
  (business-level marts), including a conformed Kimball star (`dim_*` / `fct_*`)
  → `ml` (features/model inputs). `vault` holds an insert-only Data Vault 2.0
  raw layer (`hub_*` / `link_*` / `sat_*`) built as a parallel branch off
  `bronze`.
```

- [ ] **Step 6: `README.md` — running the pipeline**

In the `## Running the pipeline` section, add `vault_load.py` to the code block and a sentence after it:

```bash
python3 data_gen/generate_data.py     # synthetic CSVs (first run only)
python3 src/bronze_ingest.py          # raw -> bronze
python3 src/silver_transform.py       # bronze -> silver (cleaned, normalized)
python3 src/gold_marts.py             # silver -> gold wide marts
python3 src/gold_star.py              # silver -> gold Kimball star (dim_*/fct_*)
python3 src/vault_load.py             # bronze -> vault Data Vault (hub_*/link_*/sat_*)
python3 tests/verify_e2e.py           # cross-layer invariant checks
```

```markdown
`vault_load.py` is a parallel branch off `bronze` — it can run any time
after `bronze_ingest.py`, independent of the silver/gold stages. Its
tables persist across runs (insert-only); a re-run on unchanged `bronze`
appends nothing.
```

- [ ] **Step 7: Commit**

```bash
git add docs/standard.md README.md
git commit -m "Document the vault Data Vault raw layer

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Task 7: `explore.ipynb` — Raw Data Vault section

**Files:**
- Modify: `notebooks/explore.ipynb`

**Requires:** Docker stack up and `vault_load.py` run against current data.

- [ ] **Step 1: Add the section**

Insert a new markdown cell + code cells immediately before the `## Scratch` markdown cell. Use a small script so the notebook JSON stays valid:

```python
python3 - <<'EOF'
import json
path = "notebooks/explore.ipynb"
nb = json.load(open(path))

def md(*s): return {"cell_type": "markdown", "metadata": {}, "source": list(s)}
def code(s): return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": [s]}

# mention vault_load.py alongside the other pipeline modules in cell 0
nb["cells"][0]["source"] = [
    x.replace("`gold_marts.py`, `gold_star.py`", "`gold_marts.py`, `gold_star.py`, `vault_load.py`")
    for x in nb["cells"][0]["source"]
]

new = [
    md("## Raw Data Vault (`vault`)\n", "\n",
       "Insert-only Data Vault 2.0 raw layer built by `src/vault_load.py` as a parallel branch off `bronze`. "
       "Hubs are the business-key lists, links the relationships, satellites the attribute history "
       "(`hash_diff` changes -> a new `(hub_hk, load_date)` row). `current_sat` takes the newest row per key. "
       "`tests/verify_e2e.py` reconstructs current state from the vault and reconciles it to `silver`."),
    code('for t in ["hub_user", "hub_account", "hub_transaction", "hub_savings_goal",\n'
         '          "link_account_user", "link_transaction_account", "link_savings_goal_user",\n'
         '          "sat_user_details", "sat_account_details", "sat_transaction_details",\n'
         '          "sat_savings_goal_details", "ref_account_type", "ref_transaction_type"]:\n'
         '    print(f"vault.{t}: {spark.table(f\'{CATALOG_NAME}.vault.{t}\').count():>8,} rows")'),
    code('spark.table(f"{CATALOG_NAME}.vault.hub_account").limit(5).toPandas()'),
    code('spark.table(f"{CATALOG_NAME}.vault.link_transaction_account").limit(5).toPandas()'),
    code('spark.table(f"{CATALOG_NAME}.vault.sat_account_details").orderBy("account_hk", "load_date").limit(10).toPandas()'),
    code('# current-state reconstruction of one satellite\n'
         'import sys; sys.path.insert(0, str(Path.cwd().parent / "src"))\n'
         'from vault_load import current_sat\n'
         'current_sat(spark.table(f"{CATALOG_NAME}.vault.sat_transaction_details"), "transaction_hk").limit(10).toPandas()'),
]
# find the "## Scratch" cell index
i = next(k for k, c in enumerate(nb["cells"]) if "".join(c["source"]).startswith("## Scratch"))
nb["cells"][i:i] = new
json.dump(nb, open(path, "w"), indent=1)
open(path, "a").write("\n")
print(f"done: {len(nb['cells'])} cells")
EOF
```

- [ ] **Step 2: Validate and execute the notebook**

```bash
finenv/bin/python3 -c "import nbformat; nbformat.validate(nbformat.read('notebooks/explore.ipynb', as_version=4)); print('valid')"
cd notebooks && ../finenv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=300 explore.ipynb && cd ..
```
Expected: `valid`, then `nbconvert` writes the file with no error cell. Confirm with:

```bash
finenv/bin/python3 -c "
import json; nb=json.load(open('notebooks/explore.ipynb'))
errs=[o for c in nb['cells'] if c['cell_type']=='code' for o in c.get('outputs',[]) if o.get('output_type')=='error']
print('error outputs:', len(errs)); assert not errs
"
```

- [ ] **Step 3: Commit**

```bash
git add notebooks/explore.ipynb
git commit -m "Show the vault raw layer in explore.ipynb

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- `vault` schema + spark_session loop + data-engineer-only IAM → Task 1. ✓
- New module `src/vault_load.py`, peer of `gold_star.py`, runs after bronze, no ordering constraint → Tasks 1-4, plan header. ✓
- `uc_delta.write_delta_table` for all creation; persist across runs (overwrite first, append later) → `_write` helper, Task 4; Global Constraints. ✓
- Don't touch bronze/silver/gold modules or wide marts → Global Constraints; only `silver_transform` *imports* (`dedupe_latest`, `build_*_types`). ✓
- Hubs (4): schema `_hk` / business key / `load_date` / `record_source` → Task 4 `HUBS` + loop. ✓
- Links (3): `_hk` = hash of parent business keys in order, parent `_hk`s, no `link_transaction_user` → Task 4 `LINKS` + loop. ✓
- Satellites (4): `(_hk, load_date)` PK, `hash_diff`, `record_source`, attributes; money→decimal(10,2), dates/ts cast, else as-inferred; new row only on `hash_diff` change → Task 4 `SATS` + `changed_sat_rows` (Task 3). ✓
- PII: `sat_user_details` only `date_of_birth` + `signup_date` → `SATS` attr list, Task 4; Global Constraints. ✓
- Reference tables from `silver_transform` seed constants, no type hubs → Task 4 uses `build_account_types` / `build_transaction_types`. ✓
- Hashing: `_hash` matches `_row_hash`; keys trimmed + ordered; diffs sorted + untrimmed; 64-hex string → Task 1 (`_hash`, `add_hash_key`, `add_hash_diff`) + tests. ✓
- Dedupe bronze by `_ingested_at` via `dedupe_latest` → Task 4 every loop. ✓
- Load algorithm pure fns: `add_hub_key`/`add_link_key`(=`add_hash_key`), `add_hash_diff`, `new_hub_rows`/`new_link_rows`(=`new_rows_by_key`), `changed_sat_rows`, `current_sat` → Tasks 1-3. ✓
- `__main__`: pin `load_date`, seed refs, hubs→links→sats, print `+N (M total)` → Task 4. ✓
- Idempotency: 2nd run all `+0` → Task 4 Step 5. ✓
- Reconciliation checks 1-6 → Task 5 `vault_checks` (hub uniqueness, hub completeness, link FK + uniqueness, sat FK, no consecutive `hash_diff`, loss-free per-entity). ✓
- Unit tests: hash determinism/order/trim (Task 1), `new_rows_by_key` first/overlap/empty (Task 2), `changed_sat_rows` first/noop/changed/latest-version + `current_sat` (Task 3) — 11 total, covering the spec's 5 test bullets. ✓
- Docs: standard.md layer/benefits/naming/testing + README schemas/run-order → Task 6. ✓
- explore.ipynb "Raw Data Vault" section → Task 7. ✓
- Rollout: additive, no migration, run order diagram → plan header + Task 6 Step 6. ✓
- Out of scope (re-point silver, business vault, effectivity/record-tracking/multi-source sats, per-load-type `record_source`, type hubs) → nothing in any task touches them. ✓

**Placeholder scan:** No `TBD` / `TODO` / "similar to Task N" / "handle edge cases". Every code step is complete. ✓

**Type consistency:**
- `_hash(*cols)` takes `Column`s in Task 1; every caller (`add_hash_key`, `add_hash_diff`, tests) passes `F.col(...)` / `F.trim(...)`. ✓
- `add_hash_key(df, bk_cols: list, hk_col: str)` / `add_hash_diff(df, attr_cols: list, out_col="hash_diff")` — signatures identical in Task 1 definition, Task 4 calls, and tests. ✓
- `new_rows_by_key(incoming, existing, key_col)` — Task 2 definition; Task 4 calls it for hubs and links with the `_hk` column name; return columns == incoming columns (so `_write` sees the right schema). ✓
- `changed_sat_rows(incoming, existing, key_col)` / `current_sat(sat, key_col)` — Task 3 definition; Task 4 and Task 5 both call with the `_hk` name; `current_sat` used in `verify_e2e.py` and Task 7 notebook. ✓
- `_latest_by_load_date` is private to `vault_load.py`, used by both `changed_sat_rows` and `current_sat` — defined once in Task 3. ✓
- Hub/link/sat table names and `_hk` column names are identical between `HUBS`/`LINKS`/`SATS` (Task 4), `vault_checks` (Task 5), the docs (Task 6) and the notebook (Task 7): `hub_user`/`user_hk`, `hub_account`/`account_hk`, `hub_transaction`/`transaction_hk`, `hub_savings_goal`/`savings_goal_hk`; `link_account_user`/`account_user_hk`, `link_transaction_account`/`transaction_account_hk`, `link_savings_goal_user`/`savings_goal_user_hk`. ✓
- `ref_account_type` columns (`account_type_id`, `type_name`, `category`, `description`) come from `build_account_types` and are what `vault_checks` joins on (`type_name` → `account_type`). ✓

No issues found.
