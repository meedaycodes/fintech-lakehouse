"""Checks foreign key integrity across the generated bronze CSVs.

Loads users.csv, accounts.csv, and transactions.csv from data/raw/ and
verifies:
  - primary keys (user_id, account_id, transaction_id) are unique
  - accounts.user_id all reference an existing users.user_id
  - transactions.account_id all reference an existing accounts.account_id

Run after data_gen/generate_data.py:
    python3 data_gen/check_referential_integrity.py

Exits 0 if everything checks out, 1 otherwise (so it can gate a pipeline).
"""
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def load_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"{path} not found - run data_gen/generate_data.py first")
    return pd.read_csv(path)


def check_unique_key(df: pd.DataFrame, key: str, entity: str) -> list[str]:
    duplicates = df[df.duplicated(subset=key, keep=False)]
    if duplicates.empty:
        print(f"OK   {entity}.{key} is unique ({len(df)} rows)")
        return []
    error = f"{entity}.{key} has {duplicates[key].nunique()} duplicated value(s)"
    print(f"FAIL {error}")
    return [error]


def check_foreign_key(
    child: pd.DataFrame, child_key: str, child_name: str,
    parent: pd.DataFrame, parent_key: str, parent_name: str,
) -> list[str]:
    orphans = child[~child[child_key].isin(parent[parent_key])]
    if orphans.empty:
        print(f"OK   {child_name}.{child_key} -> {parent_name}.{parent_key} ({len(child)} rows, 0 orphans)")
        return []
    error = (
        f"{child_name}.{child_key} -> {parent_name}.{parent_key} has "
        f"{len(orphans)} orphaned row(s) with no matching {parent_name}"
    )
    print(f"FAIL {error}")
    return [error]


def main() -> int:
    users = load_csv("users.csv")
    accounts = load_csv("accounts.csv")
    transactions = load_csv("transactions.csv")
    savings_goals = load_csv("savings_goals.csv")

    errors: list[str] = []
    errors += check_unique_key(users, "user_id", "users")
    errors += check_unique_key(accounts, "account_id", "accounts")
    errors += check_unique_key(transactions, "transaction_id", "transactions")
    errors += check_unique_key(savings_goals, "goal_id", "savings_goals")

    errors += check_foreign_key(accounts, "user_id", "accounts", users, "user_id", "users")
    errors += check_foreign_key(transactions, "account_id", "transactions", accounts, "account_id", "accounts")
    errors += check_foreign_key(savings_goals, "user_id", "savings_goals", users, "user_id", "users")

    print()
    if errors:
        print(f"{len(errors)} referential integrity check(s) failed.")
        return 1

    print("All referential integrity checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
