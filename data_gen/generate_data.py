from faker import Faker

import csv, random, uuid
from datetime import datetime, timedelta
from pathlib import Path

fake = Faker("en_GB")
N_USERS = 2000
ACCOUNT_TYPES = ["savings", "investment", "pension"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "roundup", "investment_contribution"]
CURRENCY = "GBP"

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Realistic amount ranges per transaction type, in GBP. Roundups are spare
# change from card purchases, so they stay small; deposits/contributions are
# regular sums; withdrawals sit in between.
AMOUNT_RANGES = {
    "deposit": (10.00, 2000.00),
    "withdrawal": (5.00, 1000.00),
    "roundup": (0.01, 0.99),
    "investment_contribution": (10.00, 500.00),
}


def generate_users(n: int) -> list:
    user_rows = []
    for _ in range(n):
        user_rows.append({
            "user_id": str(uuid.uuid4()),
            "full_name": fake.name(),
            "email": fake.email(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=75),
            "address":fake.address().replace("\n", ", "),
            "signup_date": fake.date_between(start_date="-3y", end_date="today"),

        })

    return user_rows


def generate_accounts(users, min_per_user=1, max_per_user=3):
    """
    Each account is built from a real user row, not a fresh random id,
    hence user_id foreign key is genuine rather than coincidental.
    """

    account_rows = []
    for user in users:
        for _ in range(random.randint(min_per_user, max_per_user)):
            account_rows.append({
                "account_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "account_type": random.choice(ACCOUNT_TYPES),
                "account_number": fake.iban(),
                "opened_date": fake.date_between(start_date=user["signup_date"], end_date="today")
            })

    return account_rows


def generate_transactions(accounts, min_per_account=5, max_per_account=50, start_date=None):
    """
    Each transaction is built from a real account row, so account_id (and
    transitively user_id) is a genuine foreign key, and the transaction can
    never predate the account it belongs to.

    start_date overrides the per-account "earliest possible" bound (each
    account's own opened_date by default) - used by
    generate_incremental_transactions.py to anchor a fresh batch to "now"
    instead of spanning each account's whole history.
    """

    transaction_rows = []
    for account in accounts:
        for _ in range(random.randint(min_per_account, max_per_account)):
            transaction_type = random.choice(TRANSACTION_TYPES)
            low, high = AMOUNT_RANGES[transaction_type]
            effective_start = start_date if start_date is not None else account["opened_date"]
            transaction_rows.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id": account["account_id"],
                "transaction_type": transaction_type,
                "amount": round(random.uniform(low, high), 2),
                "currency": CURRENCY,
                "transaction_ts": fake.date_time_between(
                    start_date=effective_start, end_date="now"
                ),
            })

    return transaction_rows


GOAL_NAMES = [
    "Emergency Fund", "Holiday", "House Deposit", "Wedding",
    "New Car", "Rainy Day Fund", "Home Renovation", "Christmas",
]


def generate_savings_goals(users, pct_with_goal=0.6):
    """
    Not every user has a goal - pct_with_goals models that realistically, and the FK should also come from actual user rows,
     not a random id """

    goal_rows = []
    for user in users:
        if random.random() > pct_with_goal:
            continue

        target_amount = round(random.uniform(500.00, 20000.00), 2)
        created_date = fake.date_between(start_date=user["signup_date"], end_date="today")
        # Goals are set with a future target when created, anywhere from
        # 3 months to 3 years out.
        target_date = created_date + timedelta(days=random.randint(90, 1095))

        goal_rows.append({
            "goal_id": str(uuid.uuid4()),
            "user_id": user["user_id"],
            "goal_name": random.choice(GOAL_NAMES),
            "target_amount": target_amount,
            # Progress towards the goal so far - can be anywhere from
            # nothing saved yet up to fully funded.
            "current_amount": round(random.uniform(0, target_amount), 2),
            "created_date": created_date,
            "target_date": target_date,
        })

    return goal_rows


def write_csv(rows: list, filename: str) -> None:
    if not rows:
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows -> {filepath}")


if __name__ == "__main__":
    users = generate_users(N_USERS)
    accounts = generate_accounts(users)
    transactions = generate_transactions(accounts)
    savings_goals = generate_savings_goals(users)

    write_csv(users, "users.csv")
    write_csv(accounts, "accounts.csv")
    write_csv(transactions, "transactions.csv")
    write_csv(savings_goals, "savings_goals.csv")
