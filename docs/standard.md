# Engineering Standards

Conventions and strategy for this lakehouse. PII/access-control policy lives in
[governance.md](governance.md) - this doc covers naming, layer contracts,
partitioning, and testing.

## Layer contracts

Four schemas under the `chip_lakehouse` catalog, each with a distinct contract.
A table only moves to the next layer once it satisfies the current one - the
point is that each layer's guarantees are absolute, not "usually true."

| Layer | Contract | Enforced by |
|---|---|---|
| `bronze` | Raw, as close to source shape as possible. Schema-inferred, not hand-typed. Tagged with `_ingested_at`. No cleaning, no dedup, no PII handling. | [bronze_ingest.py](../src/bronze_ingest.py) |
| `silver` | Deduplicated (one row per primary key), explicitly typed, every FK referentially valid, direct identifiers removed. Still one-to-one with bronze entities - no joins, no aggregation. | [silver_transform.py](../src/silver_transform.py) |
| `gold` | Business-level marts: joined, aggregated, dimensional. Consumers (dashboards, analysts) query gold, not silver, for anything beyond raw entity lookups. | [gold_marts.py](../src/gold_marts.py) (pending) |
| `ml` | Model-ready feature tables, built from silver/gold. | [ml_train_register.py](../src/ml_train_register.py) (pending) |

Bronze is intentionally dumb (garbage in, garbage stored) so that silver has a
single, auditable place where every cleaning decision is made once. Don't
backfill cleaning logic into bronze, and don't skip silver by joining bronze
tables directly in gold.

## Naming conventions

**Catalog/schema/table**
- One catalog: `chip_lakehouse`. Schema names are single lowercase words matching the layer (`bronze`, `silver`, `gold`, `ml`).
- Table names are plural, snake_case, and identical across layers for the same entity (`bronze.accounts` → `silver.accounts`) - a table renamed between layers is a sign the transform is doing more than cleaning (see Layer contracts above).

**Columns**
- snake_case throughout, no abbreviations that aren't already domain-standard (`iban` would be fine; `acct_no` would not).
- Primary keys: `<entity_singular>_id` (`user_id`, `account_id`, `transaction_id`, `goal_id`) - always a UUID string, generated at source, never a database-assigned surrogate.
- Pipeline metadata columns are prefixed with `_` and never carry business meaning (`_ingested_at`). Purely transient columns used mid-transform and dropped before write are also `_`-prefixed (e.g. `_rn` in `dedupe_latest`) so they're unmistakable if one ever leaks into an output by accident.
- A column that has been transformed away from its bronze meaning gets a new name that says so, rather than silently changing what the same name means between layers: `account_number` (full IBAN, bronze) becomes `account_number_masked` (last 4 digits, silver), `date_of_birth` (exact date, bronze) becomes `age_band` (10-year bucket, silver). If you can `SELECT column_name FROM bronze... UNION ALL SELECT column_name FROM silver...` and get two different *kinds* of data back, the column needs two different names.

**IAM principals** ([iam/access.yaml](../iam/access.yaml))
- `<role>@chip-lakehouse.local` - e.g. `data-engineer@chip-lakehouse.local`, `data-analyst@chip-lakehouse.local`. Role-shaped, not person-shaped; a real deployment would map SSO identities to these roles rather than creating one UC user per role.

**Modules** (`src/`)
- One file per pipeline stage, named after the layer it produces: `bronze_ingest.py`, `silver_transform.py`, `gold_marts.py`, `ml_train_register.py`.
- Cross-cutting infrastructure that more than one stage needs lives in its own module rather than being duplicated or imported sideways from another stage's file: `spark_session.py` (session + catalog/token bootstrap), `uc_delta.py` (table write/registration), `manage_access.py` (IAM reconciliation).

## Table creation: always through `uc_delta.write_delta_table`

Every table in this project is created via [uc_delta.py](../src/uc_delta.py)'s
`write_delta_table()` - writing Delta files directly to disk, then registering
the table with UC's REST API - never via `df.write.saveAsTable(...)` or
`CREATE TABLE ... AS SELECT`. This isn't a style preference: `unitycatalog-spark`
0.2.1's own `TableCatalog.createTable` path is broken for genuinely new tables
(see `bronze_ingest.py`'s module docstring for the full investigation) - reads
work fine, only the connector's write-side table creation doesn't. Any new
pipeline stage (`gold_marts.py`, `ml_train_register.py`) must use the same
helper; falling back to `saveAsTable` will fail in a way that looks like a
config problem rather than a known library bug.

## Partitioning

**Current state: nothing is partitioned.** At ~110K rows, `transactions` - the
largest table by a wide margin - is comfortably within single-file/small-file
territory; adding a partition column now would create more small files than it
would prune, which is a net loss, not an optimization.

**Planned strategy, once it's warranted:** partition `transactions` by a
`transaction_date` column derived from `transaction_ts` (date truncation), since
that's both the natural query boundary (most analytical queries filter by a
date range) and the natural *load* boundary - it lines up with how
[bronze_ingest.py --incremental](../src/bronze_ingest.py) already appends new
transactions in date-bounded batches. `users`, `accounts`, and `savings_goals`
stay unpartitioned regardless of growth - they're dimension-like and small
relative to `transactions`; partitioning them would only add overhead.

The trigger for actually adding the partition isn't a specific row count so
much as evidence: when a `transactions` query that filters by date starts
scanning meaningfully more data than the date range needs, that's when to
partition, not before.

## Testing strategy

Three layers of verification, each catching a different class of problem:

**1. Source data integrity** ([data_gen/check_referential_integrity.py](../data_gen/check_referential_integrity.py))
Runs against the raw generated CSVs, before anything touches Spark or UC:
primary-key uniqueness and FK referential integrity via pandas. This is a gate
on the *synthetic data generator* itself, not the pipeline - it exists because
`generate_data.py` is depended on by everything downstream, so a bug there
would otherwise surface confusingly far from its actual cause.

**2. Runtime quarantine, built into the pipeline itself** ([silver_transform.py](../src/silver_transform.py))
Not a separate test suite - a property of the transform functions themselves.
Every FK is validated via `left_semi`/`left_anti` joins against the (already
clean) silver parent, and every category column against its known enum. Rows
that fail are written to `data/quarantine/<table>_<reason>/` rather than
silently dropped or allowed through - a bad row becomes a visible, inspectable
artifact instead of a mystery discovered three layers later. This is the
primary defense against real-world messy sources (the current CSV generator is
clean by construction, but a real upstream system won't be).

**3. Unit tests on transform functions** ([tests/test_data_quality.py](../tests/test_data_quality.py))
Each `transform_*` function in `silver_transform.py` takes and returns plain
DataFrames, so it's testable in isolation with a handful of synthetic rows -
no need to run the full pipeline against real bronze data to test one edge
case. The pattern (demonstrated ad hoc while building `silver_transform.py`,
to be formalized here as actual pytest cases):

```python
def test_transform_accounts_quarantines_bad_type_and_orphans(spark):
    silver_users = spark.createDataFrame([Row(user_id="u1")])
    bronze_accounts = spark.createDataFrame([
        Row(account_id="a1", user_id="u1", account_type="savings", ...),        # valid
        Row(account_id="a2", user_id="u1", account_type="BOGUS", ...),          # bad enum
        Row(account_id="a3", user_id="ORPHAN", account_type="savings", ...),    # bad FK
    ])
    result = transform_accounts(bronze_accounts, silver_users)
    assert result.count() == 1
    assert result.collect()[0]["account_id"] == "a1"
```

Every transform function should have at least: one happy-path case, one
duplicate-PK case (proving `dedupe_latest` keeps the latest, not an arbitrary
row), one bad-enum case, and one orphaned-FK case. A test that only exercises
the happy path doesn't prove the quarantine logic works - it proves it didn't
crash on clean data, which the generator already guarantees on its own.
