# Data Vault raw layer (`vault` schema)

Status: approved, not yet implemented
Sub-project 3 of 3 (Inmon → Kimball → Data Vault). Sub-projects 1
(`2026-09-06-inmon-silver-normalization-design.md`) and 2
(`2026-09-06-kimball-star-schema-design.md`) are merged. This spec covers
the Data Vault raw layer only.

## Motivation

The project now has three modelling styles downstream of `bronze`:
Inmon-normalized `silver`, a Kimball star in `gold`, and two hand-rolled
wide marts. What it does not have is a raw store that keeps the full,
unedited history of what the source systems said and when.

`bronze` cannot be that store: it is full-overwrite (or append for
incremental transactions), so a corrected or withdrawn source row
destroys the previous value with no trace, and `_ingested_at` is a bare
timestamp with no labelled provenance. `silver` cannot be it either: its
dedupe-latest step deliberately collapses history to one row per key.

A Data Vault raw layer fills that gap: an insert-only, hash-keyed,
source-tagged model that never updates a row in place, sits alongside
`silver` as a second branch off `bronze`, and can be reconciled back to
`silver` to prove it loses nothing.

### What this layer provides

| Benefit | Why it matters here | Design choice that delivers it |
|---|---|---|
| **Non-destructive history** | `bronze` overwrite destroys prior attribute values; `silver` dedupe discards them. | Insert-only satellites keyed `(hub_hk, load_date)`; a new version lands only when `hash_diff` changes. |
| **Provenance** | Transactions already load from both a full reload and an incremental append; `_ingested_at` does not say which, and `silver` drops it. | `record_source` + `load_date` on every hub, link and satellite row. |
| **Resilience to source change** | Sub-project 2 recorded that `silver.accounts` gaining a column caused a UC registration drift. | A new source attribute is a new satellite; a new relationship is a new link. Hubs and existing satellites never change and need no reload. |
| **Restartable, order-tolerant loads** | `silver` must build parents before children so FK validation works. | Every load is an idempotent left-anti-join insert keyed by a deterministic hash; the only ordering rule is hubs before the links and satellites that reference them. |
| **Deterministic integration keys** | The Kimball star had to mint integer surrogates with `row_number()` and carry the UUID alongside. | The hash key is `sha2` of the business key — identical every run, computable without a lookup; links are built by hashing their parents' business keys, not by joining. |
| **Model-agnostic raw store** | Inmon `silver`, the Kimball star and the wide marts have each diverged from `bronze` in their own way. | `verify_e2e.py` reconstructs current-state entities from the vault and asserts they match `silver`, so the vault is a provable loss-free rebuild point. |

## Placement

- New schema `chip_lakehouse.vault`. Added to the `CREATE SCHEMA IF NOT
  EXISTS` loop in `spark_session.py` (`["bronze", "silver", "gold", "ml",
  "vault"]`) and to `iam/access.yaml` with the same grants the other
  read schemas already carry (`data-engineer`: full; `data-analyst`:
  `USE SCHEMA` + `SELECT`).
- New module `src/vault_load.py`, peer of `bronze_ingest.py` /
  `silver_transform.py` — one file per pipeline stage, named after what
  it produces, per `docs/standard.md`.
- Runs after `bronze_ingest.py`. It reads only `bronze.*` and has **no
  ordering constraint** with `silver_transform.py`, `gold_marts.py` or
  `gold_star.py` — it is a parallel branch off `bronze`, not a stage
  between `bronze` and `silver`.
- `bronze_ingest.py`, `silver_transform.py`, `gold_marts.py`,
  `gold_star.py` are **not touched**. `silver` is not re-pointed at the
  vault; the two coexist and are reconciled.
- Every table is created via `uc_delta.write_delta_table()` /
  `register_uc_table()`, never `df.write.saveAsTable()` / CTAS (the
  connector's table-creation path is broken — see `bronze_ingest.py`).
- Vault tables **persist across runs** — like the Kimball SCD2 dims, and
  unlike everything else in the project. First run seeds each table;
  later runs append insert-only deltas. A re-run on unchanged `bronze`
  appends nothing.

## Tables

All in the `vault` schema. Prefixes: `hub_`, `link_`, `sat_`, `ref_`.

### Hubs — one row per distinct business key ever seen

| table | business key (from `bronze`) |
|---|---|
| `hub_user` | `user_id` |
| `hub_account` | `account_id` |
| `hub_transaction` | `transaction_id` |
| `hub_savings_goal` | `goal_id` |

| column | type | notes |
|---|---|---|
| `<entity>_hk` | string | PK. `sha2` hex of the business key (see Hashing). E.g. `user_hk`. |
| `<business_key>` | string | the source UUID, retained as the natural key. E.g. `user_id`. |
| `load_date` | timestamp | when this key was first seen by a vault load. |
| `record_source` | string | `"bronze.<table>"`. |

### Links — one row per distinct relationship ever seen

| table | connects | parent business keys hashed into `_hk` |
|---|---|---|
| `link_account_user` | account → owning user | `account_id`, `user_id` |
| `link_transaction_account` | transaction → account | `transaction_id`, `account_id` |
| `link_savings_goal_user` | goal → user | `goal_id`, `user_id` |

| column | type | notes |
|---|---|---|
| `<link>_hk` | string | PK. `sha2` hex of the concatenated parent business keys, in the column order listed above. |
| `<parent>_hk` | string | one per parent hub, e.g. `account_hk`, `user_hk`. |
| `load_date` | timestamp | when this relationship was first seen. |
| `record_source` | string | `"bronze.<table>"` (the child entity's table). |

There is no `link_transaction_user` — it is derivable by traversing
`link_transaction_account` → `link_account_user`.

### Satellites — insert-only attribute history, one per hub

| satellite | parent hub | tracked attributes (raw `bronze`, pre-`silver`) |
|---|---|---|
| `sat_user_details` | `hub_user` | `date_of_birth`, `signup_date` |
| `sat_account_details` | `hub_account` | `account_type`, `account_number`, `opened_date` |
| `sat_transaction_details` | `hub_transaction` | `transaction_type`, `amount`, `currency`, `transaction_ts` |
| `sat_savings_goal_details` | `hub_savings_goal` | `goal_name`, `target_amount`, `current_amount`, `created_date`, `target_date` |

| column | type | notes |
|---|---|---|
| `<entity>_hk` | string | FK to the parent hub. Part of the PK. |
| `load_date` | timestamp | part of the PK. `(<entity>_hk, load_date)` is unique. |
| `hash_diff` | string | `sha2` hex of the tracked attributes (see Hashing). |
| `record_source` | string | `"bronze.<table>"`. |
| *(tracked attributes)* | as inferred by `bronze` | carried through as-is, except the type casts noted below. |

Attribute typing in satellites is light — the vault stores what the
source said. `amount` / `target_amount` / `current_amount` are cast to
`decimal(10,2)` (money is never stored as a float, per
`docs/standard.md`); dates and timestamps are cast explicitly from
`bronze`'s CSV-inferred types. Everything else lands as `bronze` inferred
it. Full type conformance, PII reduction, FK validation and quarantine
remain `silver`'s job, not the vault's.

A new satellite row is written for a hub key **only when its computed
`hash_diff` differs from that key's most recent existing `hash_diff`**.
"Current" is derived at query time: `row_number()` over
`partition by <entity>_hk order by load_date desc`, take row 1. No
`load_end_date` and no `is_current` column — the satellite is never
updated in place.

### PII

`sat_user_details` carries only the user attributes anything downstream
consumes (`date_of_birth`, `signup_date`). The direct identifiers
`bronze.users` still holds — `full_name`, `email`, `address` — are
dropped at vault load, exactly as `silver_transform.py` drops them. The
vault is loss-free for every attribute the pipeline uses; the project's
"direct identifiers never persist past the raw CSV" posture
(`docs/governance.md`) is unchanged. A dedicated locked-down
`sat_user_pii` satellite was considered and rejected as new access-control
surface for data nothing in the project reads.

### Reference tables

`ref_account_type` and `ref_transaction_type` — the same closed
enumerations `silver` holds, seeded from the `ACCOUNT_TYPE_SEED` /
`TRANSACTION_TYPE_SEED` constants **imported from `silver_transform.py`**
(not re-declared), so there is one source of truth for the seed. Same
columns as `silver.account_types` / `silver.transaction_types`. No type
hubs — a three-row and a four-row closed list governed by a literal do
not benefit from hub/satellite historisation.

## Hashing

One helper, matching the convention `gold_star.py`'s `_row_hash` already
uses:

```python
def _hash(*cols):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)
```

- **Hash keys** (`hub.<entity>_hk`, `link.<link>_hk`): business-key
  columns are `trim`-ed first, then hashed in a fixed column order
  (hubs: the single business key; links: parents in the order listed in
  the Links table). Trimming is standard business-key treatment; it is
  harmless for the UUIDs this source uses and correct if a real source
  ever pads a key.
- **Hash diffs** (`sat.hash_diff`): the tracked attributes, cast to
  string, in a fixed sorted-by-name order, nulls rendered `∅`. Not
  trimmed — a leading space in an attribute value is a real change.
- Output is 64-character lowercase hex, stored as `string`.

## Load algorithm

`vault_load.py` follows the same split as `gold_star.py`: pure functions
that take and return DataFrames (unit-tested with synthetic rows, no
catalog), and a thin `__main__` that does the catalog I/O.

### Pure functions

| function | contract |
|---|---|
| `add_hub_key(df, bk_col, hk_col)` | adds `hk_col` = `_hash(trim(bk_col))`. |
| `add_link_key(df, bk_cols, hk_col)` | adds `hk_col` = `_hash(trim(c) for c in bk_cols)`. |
| `add_hash_diff(df, attr_cols)` | adds `hash_diff` = `_hash(*sorted(attr_cols))`. |
| `new_hub_rows(incoming, existing)` | rows of `incoming` whose `_hk` is not in `existing` (`left_anti`), deduped to one row per `_hk`. |
| `new_link_rows(incoming, existing)` | same, on the link `_hk`. |
| `changed_sat_rows(incoming, existing)` | rows of `incoming` whose `(_hk, hash_diff)` does not equal that `_hk`'s latest `hash_diff` in `existing` (latest by `load_date`); includes keys absent from `existing`. |
| `current_sat(sat)` | `row_number()` over `partition by <entity>_hk order by load_date desc` == 1. |

### `__main__`

1. `spark = get_spark()`, `token = load_uc_token()`,
   `load_date = <one F.current_timestamp() for the whole run>`.
2. Seed `ref_account_type` / `ref_transaction_type` (overwrite — closed
   lists, same treatment as `silver`'s lookup tables).
3. For each entity, read `bronze.<table>` and dedupe to one row per
   business key by `_ingested_at` descending — the same
   `dedupe_latest` step `silver_transform.py` applies, so the two
   branches see the same "latest known" row for a key that arrived via
   both a full reload and an incremental append. Then compute keys and
   (for satellites) `hash_diff`.
4. Load in order: all hubs, then all links, then all satellites. For
   each table: read the existing vault table if it is registered (else
   an empty DataFrame with the target schema); compute the delta via the
   matching pure function; `write_delta_table(token, delta, "vault",
   name, mode="append")` when the delta is non-empty, or
   `write_delta_table(..., mode="overwrite")` on the very first load to
   create it.
5. Print one line per table: `vault.<name>: +<n> rows (<total> total)`.

`record_source` is `"bronze.<table>"` for this version. Distinguishing a
full reload from an incremental append is a one-line future change and is
out of scope here.

### Idempotency

A second run against unchanged `bronze` computes an empty delta for every
table and appends nothing — `+0 rows` on every line. This is the
explicit check in the implementation plan's real-run task and in
`verify_e2e.py`.

## Reconciliation

`tests/verify_e2e.py` gains a `vault_checks(spark)` group (testing tier 4
per `docs/standard.md`), added to the `main()` result list. Each check
returns `(name, ok, detail)` like the existing groups; the script still
exits non-zero on any failure.

1. **Hub key uniqueness** — `<entity>_hk` is unique in each hub.
2. **Hub completeness** — each hub's row count equals the distinct
   business-key count in `bronze.<table>`.
3. **Link FK integrity** — every parent `_hk` in each link resolves to a
   row in the corresponding hub (no orphan); the link `_hk` is unique.
4. **Satellite FK integrity** — every `sat.<entity>_hk` resolves to its
   hub.
5. **Satellite no-op suppression** — for each `_hk`, no two consecutive
   versions (ordered by `load_date`) share a `hash_diff`.
6. **Loss-free reconciliation** — reconstruct each current-state entity
   as `hub ⨝ current_sat(sat)` (⨝ `link` for the FK columns), apply
   `silver`'s orphan rule (drop rows whose parent is absent), and assert:
   - row count equals `silver.<entity>`'s row count;
   - on the columns `silver` keeps unchanged (`user_id`, `account_id`,
     `signup_date`, `opened_date`, `amount`, `currency`,
     `transaction_ts`, goal amounts and dates, and `account_type` /
     `transaction_type` resolved to their `*_id` via the `ref_` tables),
     every value matches `silver`;
   - for the columns `silver` derives (`age_band` from `date_of_birth`,
     `account_number_masked` from `account_number`), the same derivation
     applied to the vault's raw attribute matches `silver`.

Check 6 is what makes "model-agnostic raw store" a proven property rather
than a claim.

## Rollout

Additive. No migration, no change to any existing table. `spark_session.py`
creates the `vault` schema on its next run; `manage_access.py` applies
the new grants; `vault_load.py` creates the tables on its first run.
Running order becomes:

```
bronze_ingest.py  →  silver_transform.py  →  gold_marts.py
                  →  gold_star.py
                  →  vault_load.py            (independent of the silver/gold branch)
verify_e2e.py                                 (after all of the above)
```

## Testing

**Unit** (`tests/test_data_quality.py`, synthetic-Row cases, no catalog):

1. `_hash` is deterministic and order-sensitive; `concat_ws('||')` with
   the `∅` null token does not collide for `("a", None)` vs `("a∅",
   "")`-style inputs.
2. `add_hub_key` / `add_link_key` produce the expected hex and are stable
   across a `trim`-able whitespace difference in the business key.
3. `new_hub_rows`: first load (empty `existing`) returns all keys once;
   with overlap returns only the unseen keys; a key present twice in
   `incoming` yields one row.
4. `changed_sat_rows`: unchanged attributes → empty (no-op); a changed
   attribute → one new row; a brand-new key → one row; only the latest
   `existing` version counts when a key has history.
5. `current_sat`: picks the row with the greatest `load_date` per key.

**Real run + idempotency** (implementation plan task, no unit test):
`python3 src/vault_load.py` twice against a full pipeline load; second
run prints `+0 rows` on every line; hub/link/sat totals match the first
run.

**Reconciliation** (`tests/verify_e2e.py`): the `vault_checks` above.

## Explicitly out of scope

- Re-pointing `silver` at the vault (decided: additive and reconciled).
- Business vault: derived/computed satellites, PIT tables, bridge tables.
- Effectivity satellites, record-tracking satellites, multi-source
  satellites, same-as links.
- Distinguishing `record_source` for a full reload vs an incremental
  append.
- Type hubs — `account_type` / `transaction_type` stay reference tables.
- Any change to `bronze_ingest.py`, `silver_transform.py`,
  `gold_marts.py`, `gold_star.py`, or the two existing wide marts.
