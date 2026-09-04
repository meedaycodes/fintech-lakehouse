"""Generates a small batch of *new* transactions against real, already-
existing accounts - simulating "time has passed, new activity arrived"
for demonstrating incremental (append-only) bronze ingestion.

Run after data_gen/generate_data.py (needs accounts.csv to already exist):
    python3 data_gen/generate_incremental_transactions.py

Writes data/raw/transactions_incremental.csv, meant to be appended via:
    python3 src/bronze_ingest.py --incremental
"""
import csv
import random
from datetime import datetime, timedelta

from generate_data import OUTPUT_DIR, generate_transactions, write_csv

# How much of the account base sees new activity in this batch, and how
# recent that activity is - kept small/tight so the "new data" is visibly
# distinct from the historical bulk load.
N_ACCOUNTS_WITH_ACTIVITY = 100
LOOKBACK = timedelta(hours=6)


def load_accounts(path) -> list:
    with open(path) as f:
        return list(csv.DictReader(f))


if __name__ == "__main__":
    accounts_path = OUTPUT_DIR / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"{accounts_path} not found - run data_gen/generate_data.py first")

    accounts = load_accounts(accounts_path)
    active_accounts = random.sample(accounts, k=min(N_ACCOUNTS_WITH_ACTIVITY, len(accounts)))

    new_transactions = generate_transactions(
        active_accounts,
        min_per_account=1,
        max_per_account=3,
        start_date=datetime.now() - LOOKBACK,
    )

    write_csv(new_transactions, "transactions_incremental.csv")
