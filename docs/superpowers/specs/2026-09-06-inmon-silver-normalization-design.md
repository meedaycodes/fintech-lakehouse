# Inmon-style normalization of silver account/transaction types

Status: approved, not yet implemented
Sub-project 1 of 3 (Inmon → Kimball → Data Vault). See conversation history
for the full decomposition rationale; this spec covers Inmon only.

## Motivation

`silver.accounts.account_type` and `silver.transactions.transaction_type`
are free strings today, validated against hardcoded Python lists
(`ACCOUNT_TYPES`, `TRANSACTION_TYPES` in `silver_transform.py`). The
inflow/outflow business rule that `gold_marts.py` depends on
(`INFLOW_TRANSACTION_TYPES`) is *also* a hardcoded Python list - a
business rule expressed in code, not data, with no single governed
definition of what these values mean or which ones exist.

This is the Inmon move: pull both enumerations into normalized lookup
tables at the silver layer, so each value and its attributes exist in
exactly one governed place, and both the *validity* of a type value and
the *business rules about it* (like transaction direction) become data
any consumer can join against, not logic duplicated per-consumer.

## Schema

### `silver.account_types` (new)

| column | type | notes |
|---|---|---|
| `account_type_id` | int | PK, hardcoded surrogate key (see Seed data) |
| `type_name` | string | business key, matches today's `account_type` values |
| `category` | string | `"cash"` \| `"investment"` \| `"retirement"` |
| `description` | string | |

### `silver.transaction_types` (new)

| column | type | notes |
|---|---|---|
| `transaction_type_id` | int | PK, hardcoded surrogate key |
| `type_name` | string | business key, matches today's `transaction_type` values |
| `direction` | string | `"inflow"` \| `"outflow"` - drives gold's balance calc |
| `description` | string | |

### Seed data (hardcoded literals, not derived from source data)

These are the same closed sets `data_gen/generate_data.py` already
generates from (`ACCOUNT_TYPES`, `TRANSACTION_TYPES`), given explicit
surrogate IDs and the governed attributes from this design:

```python
ACCOUNT_TYPE_SEED = [
    (1, "savings",    "cash",       "Instant/easy-access cash savings account"),
    (2, "investment",  "investment", "Stocks & shares investment account"),
    (3, "pension",     "retirement", "Personal pension account"),
]

TRANSACTION_TYPE_SEED = [
    (1, "deposit",                 "inflow",  "Manual deposit into the account"),
    (2, "withdrawal",              "outflow", "Withdrawal out of the account"),
    (3, "roundup",                 "inflow",  "Spare change swept in from a linked card purchase"),
    (4, "investment_contribution", "inflow",  "Contribution into an investment sub-account"),
]
```

`direction` values above exactly reproduce `gold_marts.py`'s current
`INFLOW_TRANSACTION_TYPES = ["deposit", "roundup", "investment_contribution"]`
- this is a refactor of where the rule lives, not a change to the rule.

### `silver.accounts` (changed)

`account_type` (string) → **removed**, replaced by `account_type_id` (int,
FK to `silver.account_types`). All other columns unchanged.

### `silver.transactions` (changed)

`transaction_type` (string) → **removed**, replaced by
`transaction_type_id` (int, FK to `silver.transaction_types`). All other
columns unchanged.

## Pipeline changes

### `silver_transform.py`

Two new builder functions, following the file's existing shape (plain
functions returning a `DataFrame`, no side effects):

```python
def build_account_types(spark) -> DataFrame:
    return spark.createDataFrame(
        ACCOUNT_TYPE_SEED, schema=["account_type_id", "type_name", "category", "description"]
    )

def build_transaction_types(spark) -> DataFrame:
    return spark.createDataFrame(
        TRANSACTION_TYPE_SEED, schema=["transaction_type_id", "type_name", "direction", "description"]
    )
```

`transform_accounts` and `transform_transactions` gain a new parameter for
their respective lookup table (same pattern as the existing
`silver_users`/`silver_accounts` parent-table parameters), and their
enum-validation step changes from `isin(ACCOUNT_TYPES)` to a join:

```python
def transform_accounts(bronze_accounts, silver_users, account_types) -> DataFrame:
    deduped = dedupe_latest(bronze_accounts, ["account_id"])

    typed_with_fk = deduped.join(
        account_types.select("account_type_id", "type_name"),
        deduped.account_type == account_types.type_name,
        "inner",
    )
    bad_type = deduped.join(
        account_types.select("type_name"), deduped.account_type == account_types.type_name, "left_anti"
    )
    quarantine(bad_type, "accounts_bad_type")

    typed = typed_with_fk.select(
        "account_id", "user_id", "account_type_id",
        F.concat(F.lit("****"), F.substring(F.col("account_number"), -4, 4)).alias("account_number_masked"),
        F.col("opened_date").cast("date"),
    )
    # referential join against silver_users unchanged from today
    ...
```

`transform_transactions` follows the identical shape against
`transaction_types`/`transaction_type_id`.

`__main__` ordering: `account_types`/`transaction_types` are built and
written *before* `transform_accounts`/`transform_transactions` run, since
those now require the lookup DataFrames as join inputs - the same
dependency order the file already has for `silver_users` before
`transform_accounts`.

### `gold_marts.py`

`INFLOW_TRANSACTION_TYPES` (the hardcoded list) is deleted.
`build_account_summary` takes a `transaction_types` DataFrame parameter,
joins `silver.transactions` to it, and classifies inflow/outflow via
`F.col("direction") == "inflow"` instead of `.isin(...)`.

`build_account_summary` also takes an `account_types` DataFrame parameter
and joins to resolve `account_type_id` back to its `type_name` for the
`account_type` column in `gold.account_summary` - gold stays
human-readable even though silver is normalized. This is the one place
gold intentionally re-introduces a string label after silver removed it:
silver normalizes for governance, gold denormalizes for consumption,
matching gold's contract in `docs/standard.md`.

`__main__` reads `silver.account_types`/`silver.transaction_types` back
from the catalog (same "read the persisted table, not the in-memory
DataFrame" discipline `customer_360` already follows for
`account_summary`) and passes them into `build_account_summary`.

## Rollout

No explicit migration step: `write_delta_table`'s `overwrite` mode already
passes `overwriteSchema=true` (see `uc_delta.py`), so re-running
`silver_transform.py` naturally replaces `silver.accounts`/
`silver.transactions`' old string columns with the new FK columns and
creates the two new lookup tables. `gold_marts.py` must be re-run after,
same as any other silver change.

## Testing

Per `docs/standard.md`'s pattern (synthetic-Row unit tests on transform
functions), new cases:

1. `transform_accounts`/`transform_transactions` with an unrecognized
   `account_type`/`transaction_type` value quarantines that row via the
   join-based check (proves the join replaces `isin()` without losing
   the validation).
2. `build_account_summary`'s inflow/outflow totals, computed via the
   `direction`-column join, exactly match what the old hardcoded
   `INFLOW_TRANSACTION_TYPES` list would have produced on the same input
   - a regression check across the refactor, not just a new-feature test.
3. `check_referential_integrity.py`-style verification (informal, run
   manually against real data post-implementation, matching how prior
   layers in this project were verified): row counts unchanged after the
   refactor, and `account_type_id`/`transaction_type_id` in
   accounts/transactions all resolve to a real lookup row (no orphans).

## Explicitly out of scope

- Kimball dimensional model in gold (sub-project 2) - though it will
  reuse `silver.account_types`/`silver.transaction_types` as its
  dimension tables rather than rebuilding them.
- Data Vault (sub-project 3).
- Normalizing anything beyond `account_type`/`transaction_type` (e.g.
  `currency` is not touched here).
