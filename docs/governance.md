# Data Governance

## PII Inventory

Every field across the four bronze entities (`users`, `accounts`, `transactions`, `savings_goals` - see [bronze_ingest.py](../src/bronze_ingest.py)), classified into one of four sensitivity tiers:

| Tier | Meaning |
|---|---|
| **Direct identifier** | Uniquely and unambiguously identifies one specific person by itself - no combination with other data needed. |
| **Quasi-identifier** | Not unique alone, but narrows down to a specific person when combined with other quasi-identifiers or external data. |
| **Sensitive-financial** | Reveals financial standing, behavior, or account access details. |
| **Non-PII** | Carries no identifying or sensitive information on its own. |

| Entity | Field | Sensitivity Tier | Notes |
|---|---|---|---|
| users | `user_id` | Direct identifier | System-generated UUID, but maps 1:1 and permanently to one specific person - full re-identification power on its own. |
| users | `full_name` | Direct identifier | |
| users | `email` | Direct identifier | |
| users | `date_of_birth` | Quasi-identifier | Broad alone (shared by many); combined with postcode/gender, narrows to an individual (classic k-anonymity triplet). |
| users | `address` | Direct identifier | A full street address can single out a specific household on its own. |
| users | `signup_date` | Non-PII | Account lifecycle metadata. |
| accounts | `account_id` | Non-PII | Internal surrogate key for the account, not a person. Enables joins back to `users.user_id` - see note below. |
| accounts | `user_id` | Direct identifier | Foreign key copy of the direct identifier in `users`. |
| accounts | `account_type` | Non-PII | Category only (savings/investment/pension). |
| accounts | `account_number` | Sensitive-financial | IBAN - exposure enables unauthorized transactions/fraud. |
| accounts | `opened_date` | Non-PII | |
| accounts | `_ingested_at` | Non-PII | Ingestion metadata, not source data. |
| transactions | `transaction_id` | Non-PII | Internal surrogate key. |
| transactions | `account_id` | Non-PII | Foreign key to `accounts.account_id` (itself Non-PII - see note below). |
| transactions | `transaction_type` | Non-PII | Category only. |
| transactions | `amount` | Sensitive-financial | Directly reveals financial behavior. |
| transactions | `currency` | Non-PII | |
| transactions | `transaction_ts` | Non-PII | |
| transactions | `_ingested_at` | Non-PII | |
| savings_goals | `goal_id` | Non-PII | Internal surrogate key. |
| savings_goals | `user_id` | Direct identifier | Foreign key copy of the direct identifier in `users`. |
| savings_goals | `goal_name` | Non-PII | Free-text category (e.g. "Wedding", "House Deposit") - reveals a savings purpose, not financial standing. |
| savings_goals | `target_amount` | Sensitive-financial | |
| savings_goals | `current_amount` | Sensitive-financial | |
| savings_goals | `created_date` | Non-PII | |
| savings_goals | `target_date` | Non-PII | |
| savings_goals | `_ingested_at` | Non-PII | |

**Note on surrogate keys (`account_id`, `transaction_id`, `goal_id`):** these are Non-PII in isolation - they don't identify a person and don't map 1:1 to one (many transactions share one account; many accounts can share one owner). They're flagged here anyway because each is a join key back to a table that *does* carry direct identifiers. A dataset built by joining bronze tables (e.g. `transactions` + `accounts`) inherits the higher sensitivity tier of whatever it pulls in - the tier of a single column doesn't tell you the tier of a query result.

### Access implications

This inventory is why [iam/access.yaml](../iam/access.yaml) denies `data-analyst@chip-lakehouse.local` any access to the `bronze` schema: bronze holds raw direct identifiers (`full_name`, `email`, `address`) and sensitive-financial fields (`account_number`, `amount`, `target_amount`, `current_amount`) with no masking or aggregation applied. Analyst-facing access is scoped to `silver`/`gold`, where transformation is expected to drop or aggregate away direct identifiers before the data reaches that tier.
