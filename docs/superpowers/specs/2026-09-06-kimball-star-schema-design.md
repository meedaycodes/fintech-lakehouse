# Kimball conformed star schema in gold

Status: approved, not yet implemented
Sub-project 2 of 3 (Inmon → Kimball → Data Vault). Sub-project 1
(`2026-09-06-inmon-silver-normalization-design.md`) is merged. This spec
covers the dimensional star only; sub-project 3 (Data Vault as a raw
layer between bronze and silver, which everything downstream re-points
to) is separate.

## Motivation

`gold` today holds two hand-rolled wide marts (`account_summary`,
`customer_360`). They answer their two specific questions well, but there
is no conformed dimensional model underneath them: no reusable
`dim_customer` / `dim_account` / `dim_date`, no fact table at
transaction grain, nothing an analyst can slice a new question against
without writing a fresh bespoke aggregate.

This sub-project adds a Kimball star **alongside** those marts, in the
same `gold` schema, built from the same silver tables. The star is
reconciled back to the existing marts (Section: Reconciliation) so the
two shapes cannot silently disagree - the same discipline
`customer_360` already applies to `account_summary`.

## Placement

- New module `src/gold_star.py`, peer of `src/gold_marts.py`. One file
  per pipeline stage, named after what it produces, per `docs/standard.md`.
- All tables land in the existing `gold` schema with `dim_` / `fct_`
  name prefixes. No new schema: `docs/standard.md` says schema names
  match the layer, and its `gold` contract already says "dimensional".
- No `iam/access.yaml` change - both roles already cover `gold`
  (`data-engineer`: full; `data-analyst`: `USE SCHEMA`, `SELECT`).
- No `spark_session.py` change - `gold` is already bootstrapped.
- `gold_marts.py`, `silver_transform.py`, `bronze_ingest.py` are **not
  touched**.

## Tables

| Table | Kind | Load | Grain |
|---|---|---|---|
| `gold.dim_date` | dimension | overwrite | one row per calendar day |
| `gold.dim_transaction_type` | dimension | overwrite | one row per transaction type |
| `gold.dim_customer` | dimension, **SCD2** | MERGE (persisted) | one row per customer version |
| `gold.dim_account` | dimension, **SCD2** | MERGE (persisted) | one row per account version |
| `gold.fct_transaction` | fact, transaction grain | overwrite | one row per `silver.transactions` row |
| `gold.fct_account_monthly_snapshot` | fact, periodic snapshot | overwrite | one row per account per month it exists |

**Out of scope for this sub-project:** any savings-goal dimension or
fact (goals stay served by `gold.customer_360`); any accumulating-
snapshot fact; any change to the two existing marts.

## Dimension schemas

### `gold.dim_date` (static, overwrite)

Smart integer key in `yyyymmdd` form - idiomatic for a date dimension
and readable in a fact row without a join.

| column | type | notes |
|---|---|---|
| `date_key` | int | PK, `year*10000 + month*100 + day` |
| `date` | date | |
| `year` | int | |
| `quarter` | int | 1-4 |
| `month` | int | 1-12 |
| `month_name` | string | `January` … |
| `day_of_month` | int | 1-31 |
| `day_of_week` | int | 1 = Monday … 7 = Sunday |
| `day_name` | string | `Monday` … |
| `week_of_year` | int | ISO week |
| `is_weekend` | boolean | `day_of_week in (6, 7)` |
| `is_month_end` | boolean | `date == last_day(date)` |

Range: `trunc(min, 'month')` to `last_day(max)` (inclusive), where `min`
/ `max` are taken across every date in silver -
`silver.transactions.transaction_ts::date`, `silver.accounts.opened_date`,
`silver.users.signup_date`. Snapping the ends to the enclosing month
guarantees `dim_date` also covers every **month-end** `date_key` the
monthly snapshot fact emits (its spine can run a few days past the last
transaction date). Built with `F.sequence(start, end, interval 1 day)`
then `explode`; all attributes derived with Spark date functions. No
Delta/catalog dependency.

### `gold.dim_transaction_type` (static, overwrite)

Effectively a rename-passthrough of `silver.transaction_types` into the
star's naming.

| column | type | notes |
|---|---|---|
| `transaction_type_key` | int | PK, **reuses** `silver.transaction_types.transaction_type_id` (not re-generated) |
| `type_name` | string | |
| `direction` | string | `inflow` \| `outflow` |
| `description` | string | |

### `gold.dim_customer` (SCD2, persisted, MERGE)

| column | type | notes |
|---|---|---|
| `customer_key` | int | PK, **pipeline-generated surrogate** |
| `user_id` | string | business key, the silver/source UUID |
| `age_band` | string | tracked attribute |
| `signup_date` | date | tracked attribute |
| `row_hash` | string | sha2-256 hex of tracked attributes |
| `valid_from` | date | inclusive |
| `valid_to` | date | exclusive; `9999-12-31` for the open row |
| `is_current` | boolean | |

- Tracked attributes (feed `row_hash`): `age_band`, `signup_date`.
- `valid_from` for a brand-new business key = its `signup_date`.
- Source: `silver.users`.

### `gold.dim_account` (SCD2, persisted, MERGE)

| column | type | notes |
|---|---|---|
| `account_key` | int | PK, **pipeline-generated surrogate** |
| `account_id` | string | business key, the silver/source UUID |
| `user_id` | string | account owner - a business **attribute**, not a dim→dim FK; lets the fact loader resolve `customer_key` without re-reading silver |
| `account_number_masked` | string | tracked attribute |
| `opened_date` | date | tracked attribute |
| `account_type` | string | tracked attribute; denormalized from `silver.account_types.type_name` (star, not snowflake) |
| `account_type_category` | string | tracked attribute; denormalized from `silver.account_types.category` |
| `row_hash` | string | sha2-256 hex of tracked attributes |
| `valid_from` | date | inclusive |
| `valid_to` | date | exclusive; `9999-12-31` for the open row |
| `is_current` | boolean | |

- Tracked attributes: `account_number_masked`, `opened_date`,
  `account_type`, `account_type_category`. **`user_id` is not tracked** -
  an account never changes owner in this model, and treating it as a
  tracked attribute would be noise.
- `valid_from` for a brand-new business key = its `opened_date`.
- Source: `silver.accounts` inner-joined to `silver.account_types` on
  `account_type_id`. The join is safe by construction -
  `transform_accounts` only emits rows whose `account_type_id` it
  resolved from that same lookup.

## SCD2 load mechanism

Split into a **pure planner** (unit-tested, catalog-free) and a thin
**I/O wrapper** (exercised only end-to-end), matching how sub-project 1
keeps `transform_*` functions pure.

### `plan_scd2(...)` - pure, in `gold_star.py`

```python
def plan_scd2(
    incoming: DataFrame,          # one row per business key: bk + tracked + attrs + effective_from col
    current_target: DataFrame | None,  # the existing dim, or None on first load
    *,
    business_key: str,
    tracked_cols: list[str],
    effective_from_col: str,
    surrogate_key: str,
    run_date: datetime.date,
) -> tuple[DataFrame, DataFrame]:  # (rows_to_insert, business_keys_to_expire)
```

`row_hash` = `sha2(concat_ws("||", *[coalesce(cast(c as string), "∅") for c in sorted(tracked_cols)]), 256)`.

- **First load** (`current_target is None`): every incoming row becomes an
  insert. `surrogate_key` = `row_number() over (order by business_key)`,
  `valid_from` = `effective_from_col::date`, `valid_to` = `9999-12-31`,
  `is_current` = `true`. `business_keys_to_expire` is empty.
- **Subsequent load**:
  - `max_key` = `max(surrogate_key)` over `current_target` (0 if empty).
  - **New** business keys (`incoming` left-anti `current_target` on
    `business_key`): insert. `valid_from` = `effective_from_col::date`.
  - **Changed**: `incoming` joined to `current_target` where
    `current_target.is_current` and `row_hash` differs. Emits **both** an
    entry in `business_keys_to_expire` **and** an insert with
    `valid_from` = `run_date`.
  - **Unchanged** (`is_current` row, equal `row_hash`): nothing.
  - New surrogate keys for all inserts: `max_key + row_number() over
    (order by business_key, valid_from)`.
  - Every insert gets `valid_to` = `9999-12-31`, `is_current` = `true`.
  - No delete path - silver never hard-deletes.

### `merge_scd2(...)` - I/O wrapper, in `gold_star.py`

```python
def merge_scd2(token, spark, incoming, schema, table, *,
               business_key, tracked_cols, effective_from_col, surrogate_key) -> None
```

1. `location` = the standard `data/lakehouse/<schema>/<table>` path.
2. If `uc_delta.get_uc_table(...)` is `None` **and** no Delta log exists
   at `location` → first load: `inserts, _ = plan_scd2(incoming, None, ...)`,
   then `uc_delta.write_delta_table(token, inserts, schema, table)` (its
   normal overwrite + register path).
3. Otherwise: read the current dim via
   `spark.read.format("delta").load(location)`; call `plan_scd2` with it
   and `run_date = date.today()`.
   - Expire: `DeltaTable.forPath(spark, location).alias("t").merge(
     expire.alias("s"), "t.<bk> = s.<bk> AND t.is_current = true")
     .whenMatchedUpdate(set={"valid_to": "DATE '<run_date>'",
     "is_current": "false"}).execute()` - the same `run_date` literal
     passed to `plan_scd2`, so the closed row's `valid_to` and the new
     row's `valid_from` match exactly.
   - Insert: `inserts.write.format("delta").mode("append").save(location)`.
   - `uc_delta.register_uc_table(...)` - a no-op unless the registration
     drifted (columns are unchanged here, so normally nothing happens).

Idempotent: a second run on unchanged silver produces an empty `inserts`
and empty `expire` - the dim is byte-for-byte unchanged.

`DeltaTable` comes from `delta-spark`, already on the classpath via
`spark_session.get_spark`. The `merge_scd2` wrapper and its `DeltaTable`
call are **not** unit-tested (the test `spark` fixture is deliberately
Delta-free); they are covered by `tests/verify_e2e.py`.

## Fact schemas

### `gold.fct_transaction` (transaction grain, overwrite)

```python
def build_fct_transaction(
    silver_transactions, silver_transaction_types, dim_customer, dim_account
) -> DataFrame
```

| column | type | notes |
|---|---|---|
| `date_key` | int | `yyyymmdd` of `transaction_ts::date`, computed directly (no join) |
| `customer_key` | int | FK → `dim_customer` |
| `account_key` | int | FK → `dim_account` |
| `transaction_type_key` | int | FK → `dim_transaction_type`; = `transaction_type_id` |
| `transaction_id` | string | degenerate dimension (no dim table) |
| `amount` | decimal(12,2) | unsigned |
| `signed_amount` | decimal(12,2) | `amount` if `direction = 'inflow'` else `-amount` |
| `currency` | string | low-cardinality attribute (always `GBP` today) |

- `direction` from an inner join to `silver_transaction_types` on
  `transaction_type_id` (safe by construction, as in `gold_marts.py`).
- **Point-in-time dimension resolution:** `account_key` from
  `dim_account` where `account_id` matches **and**
  `transaction_ts::date` ∈ `[valid_from, valid_to)`. Then `customer_key`
  from `dim_customer` where `user_id` = the matched
  `dim_account.user_id` **and** the same date is in that customer
  version's `[valid_from, valid_to)`. With current data every event
  resolves to the single open version, but the load is Type-2-correct.
- Row count is exactly `silver.transactions` row count - the PIT joins
  are one-to-one (guaranteed by the non-overlapping-range SCD2
  invariant), verified in `verify_e2e.py`.

### `gold.fct_account_monthly_snapshot` (periodic snapshot, overwrite)

```python
def build_fct_account_monthly_snapshot(
    silver_transactions, silver_accounts, silver_transaction_types, dim_customer, dim_account
) -> DataFrame
```

| column | type | notes |
|---|---|---|
| `date_key` | int | `yyyymmdd` of the **month-end** date |
| `customer_key` | int | FK → `dim_customer` (as of month end) |
| `account_key` | int | FK → `dim_account` (as of month end) |
| `month_inflow` | decimal(12,2) | Σ `amount` where `direction='inflow'` and txn in that month |
| `month_outflow` | decimal(12,2) | Σ `amount` where `direction='outflow'` and txn in that month |
| `month_net` | decimal(12,2) | `month_inflow - month_outflow` (additive) |
| `closing_balance` | decimal(12,2) | Σ `signed_amount` for all txns through month end - **semi-additive** (additive across accounts/customers, not across months) |
| `transaction_count` | int | txns in that month |

- Month spine: for each account, one row per month from
  `trunc(opened_date, 'month')` through the global maximum transaction
  month (`max(transaction_ts::date)` over all of silver). Built with a
  monthly `F.sequence` then `explode`.
- **Zero-activity months still get a row** - that is the point of a
  periodic snapshot. `month_*` measures are `0.00`, `closing_balance`
  carries the prior month's value forward.
- `customer_key` / `account_key` via the same PIT join as
  `fct_transaction`, as of the month-end date.

## Orchestration (`gold_star.py` `__main__`)

Reads every silver table back **from the catalog** (not in-memory
hand-offs - same discipline `gold_marts.py` follows for
`account_summary`):

1. `dim_date`, `dim_transaction_type` → `write_delta_table` (overwrite).
2. `merge_scd2(...)` for `dim_customer`, then `dim_account`.
3. Read `dim_customer`, `dim_account` back from the catalog.
4. `build_fct_transaction(...)` → `write_delta_table` (overwrite).
5. `build_fct_account_monthly_snapshot(...)` → `write_delta_table` (overwrite).
6. Print a row count per table.

Run order in the pipeline: after `silver_transform.py`. Independent of
`gold_marts.py` (no ordering constraint between the two gold modules).

## Reconciliation

`tests/verify_e2e.py` (new; see Testing) enforces these against the live
catalog - every one must be **exact-zero** mismatch:

1. `fct_transaction` row count == `silver.transactions` row count.
2. Per account: `SUM(fct_transaction.signed_amount)` ==
   `gold.account_summary.balance`.
3. Per account: the latest month's
   `fct_account_monthly_snapshot.closing_balance` ==
   `gold.account_summary.balance`.
4. Every `fct_*` FK resolves to exactly one dim row: for each
   (fact, dim) pair, `left_anti` count is 0 **and** the inner-join row
   count equals the fact row count (no fan-out).
5. SCD2 integrity for `dim_customer` and `dim_account`: exactly one
   `is_current = true` per business key; per business key the
   `[valid_from, valid_to)` intervals are contiguous and non-overlapping.
6. `dim_date` covers every `date_key` referenced by either fact
   (`left_anti` count 0).

## Testing

### Unit tests (append to `tests/test_data_quality.py`)

Same synthetic-`Row` pattern and catalog-free `spark` fixture as
sub-project 1. Consolidate imports at the top of the file.

1. `test_build_dim_date_spans_and_flags` - two far-apart input dates;
   first and last present, row count == day span, `is_month_end` true on
   a known month-end, `is_weekend` correct for a known Saturday.
2. `test_build_dim_transaction_type_passthrough` - `transaction_type_key`
   == `transaction_type_id`; `direction` preserved.
3. `test_plan_scd2_initial_load_all_current` - `current_target=None`, N
   incoming → N inserts, all `is_current`, `valid_to == 9999-12-31`,
   keys `1..N`, `valid_from == effective_from`, empty expire.
4. `test_plan_scd2_unchanged_is_noop` - target row with matching
   `row_hash` → empty inserts, empty expire.
5. `test_plan_scd2_changed_attribute_versions` - target row, incoming
   with a changed tracked column → expire contains that business key;
   inserts has one `is_current` row, key == `max_key + 1`,
   `valid_from == run_date`.
6. `test_plan_scd2_new_business_key_inserts` - incoming business key
   absent from target → one insert, empty expire, key == `max_key + 1`,
   `valid_from == effective_from`.
7. `test_build_fct_transaction_signed_amount_by_direction` - inflow row
   positive, outflow row negative.
8. `test_build_fct_transaction_point_in_time_join` - `dim_account` with
   two non-overlapping versions of one `account_id`; two transactions
   either side of the split resolve to different `account_key`.
9. `test_build_fct_transaction_row_count_preserved` - no drop, no
   fan-out.
10. `test_build_fct_account_monthly_snapshot_running_balance_and_gap` -
    account with transactions in month 1 and month 3; month 2 row exists
    with `closing_balance` == month 1's, month 3's includes all three
    months.
11. `test_build_fct_account_monthly_snapshot_month_end_date_key` -
    `date_key` is the month-end `yyyymmdd`.

### `tests/verify_e2e.py` (new) - testing tier 4

A standalone script (`if __name__ == "__main__"`, not a pytest case -
it needs the live Docker/Unity Catalog stack). Runs the whole
pipeline's cross-layer invariants:

- **silver**: row counts; orphan-FK `left_anti` checks
  (accounts→users, transactions→accounts, goals→users) all 0; every
  `account_type_id` / `transaction_type_id` resolves.
- **gold marts**: `account_summary` / `customer_360` row counts; per-user
  `customer_360.total_balance` == `Σ account_summary.balance`, 0
  mismatches (the existing guarantee).
- **gold star**: all six Reconciliation checks above.

Each check is a function returning `(name, ok, detail)`; the script
prints `PASS` / `FAIL <detail>` per line and `sys.exit(1)` if any fail.
Sub-project 3 will add a "data vault" section to this same script.

## Documentation updates

- **`docs/standard.md`**
  - Layer contracts: note that `gold` holds two shapes side by side -
    the pre-aggregated wide marts (`account_summary`, `customer_360`)
    and a conformed Kimball star (`dim_*` / `fct_*`), both built from
    silver, the star reconciled against the marts.
  - Naming, tables: `dim_` / `fct_` prefixes for the dimensional tables
    in `gold`.
  - Naming, columns: dimensional surrogate key `<entity>_key` - a
    pipeline-generated integer, `gold` star only, with the source UUID
    retained alongside as the business key; the one sanctioned
    non-UUID, pipeline-assigned key, existing to support SCD2 and never
    exposed as a source identifier. SCD2 columns: `valid_from` (date,
    inclusive), `valid_to` (date, exclusive, `9999-12-31` when open),
    `is_current` (boolean), `row_hash` (sha2-256 hex of tracked
    attributes).
  - Testing strategy: add tier 4 - end-to-end invariant checks against
    the live catalog (`tests/verify_e2e.py`).
- **`README.md`**: add `python3 src/gold_star.py` to the pipeline
  sequence and `python3 tests/verify_e2e.py` as the verification step.

## Rollout

`gold_star.py` is new and additive. No migration:

- `dim_date`, `dim_transaction_type`, `fct_transaction`,
  `fct_account_monthly_snapshot` use `write_delta_table`'s overwrite +
  `overwriteSchema` path, so re-runs just replace them.
- `dim_customer` / `dim_account` persist across runs. First run seeds
  them; later runs MERGE. A re-run on unchanged silver leaves them
  identical.

Run `python3 src/gold_star.py` after `python3 src/silver_transform.py`.
`gold_marts.py` is unaffected and can run before or after.

## Explicitly out of scope

- Any savings-goal dimension or fact.
- Any accumulating-snapshot fact (e.g. goal lifecycle).
- Any change to `gold_marts.py`, `silver_transform.py`, `bronze_ingest.py`.
- Sub-project 3 (Data Vault raw layer between bronze and silver, and
  re-pointing silver at it).
- A junk dimension for `currency` (single value today - kept as a
  degenerate attribute on `fct_transaction`).
