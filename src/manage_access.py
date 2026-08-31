"""Declarative user/access management for the Unity Catalog server.

Reads iam/access.yaml (who should have which privileges, on which
securables) and reconciles the live server to match it - the same
IaC idea as Terraform, applied to UC's REST API instead of a cloud
provider.

Usage:
    python src/manage_access.py            # plan only: prints the diff, changes nothing
    python src/manage_access.py --apply    # actually applies the plan

Scope guardrails (this is a FULL reconcile - it will revoke too, so
these boundaries matter):
  - Only ever touches principals listed in access.yaml. Any other
    principal - most importantly the admin service account - is never
    read, added to, or removed from anything.
  - Only ever adds/removes privileges in MANAGED_PRIVILEGES. OWNER and
    anything outside that set stays untouched even for a managed
    principal.
  - Only ever reconciles securables that appear in access.yaml (the
    catalog itself, plus every schema mentioned by at least one user).
  - Never deletes a user, even one removed from access.yaml. Only
    privileges are reconciled; user removal is a manual, deliberate act.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import requests
import yaml

from spark_session import CATALOG_NAME, UC_URI, load_uc_token

ACCESS_CONFIG_FILE = Path(__file__).resolve().parent.parent / "iam" / "access.yaml"

# The only privileges this script will ever add or remove. Anything a
# principal holds outside this set (OWNER, in particular) is left alone
# no matter what access.yaml says or doesn't say.
MANAGED_PRIVILEGES = {
    "USE CATALOG",
    "CREATE SCHEMA",
    "USE SCHEMA",
    "CREATE TABLE",
    "SELECT",
    "MODIFY",
}


def load_config(path: Path = ACCESS_CONFIG_FILE) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def list_existing_user_emails(uri: str, token: str) -> set[str]:
    resp = requests.get(f"{uri}/api/1.0/unity-control/scim2/Users", headers=_headers(token))
    resp.raise_for_status()
    return {
        email["value"]
        for user in resp.json().get("Resources", [])
        for email in user.get("emails", [])
    }


def create_user(uri: str, token: str, email: str, name: str, apply: bool) -> None:
    print(f"+ create user {email} ({name})")
    if not apply:
        return
    resp = requests.post(
        f"{uri}/api/1.0/unity-control/scim2/Users",
        headers=_headers(token),
        json={"displayName": name, "emails": [{"value": email, "primary": True}]},
    )
    resp.raise_for_status()


def get_live_privileges(uri: str, token: str, securable_type: str, name: str) -> dict[str, set[str]]:
    """Returns {principal: {privileges}} for the given securable."""
    resp = requests.get(
        f"{uri}/api/2.1/unity-catalog/permissions/{securable_type}/{name}", headers=_headers(token)
    )
    resp.raise_for_status()
    return {
        assignment["principal"]: set(assignment.get("privileges", []))
        for assignment in resp.json().get("privilege_assignments", [])
    }


def apply_permission_changes(
    uri: str, token: str, securable_type: str, name: str, changes: list[dict], apply: bool
) -> None:
    if not apply:
        return
    resp = requests.patch(
        f"{uri}/api/2.1/unity-catalog/permissions/{securable_type}/{name}",
        headers=_headers(token),
        json={"changes": changes},
    )
    resp.raise_for_status()


def reconcile_securable(
    uri: str,
    token: str,
    securable_type: str,
    name: str,
    desired_by_principal: dict[str, set[str]],
    apply: bool,
) -> int:
    """Diffs desired vs. live privileges for the declared principals only
    and applies (or prints) the changes needed. Returns the number of
    principals that had a change.
    """
    live = get_live_privileges(uri, token, securable_type, name)
    changes = []
    changed_count = 0

    for principal, desired in desired_by_principal.items():
        current = live.get(principal, set()) & MANAGED_PRIVILEGES
        to_add = sorted(desired - current)
        to_remove = sorted(current - desired)
        if not to_add and not to_remove:
            continue
        changed_count += 1
        for priv in to_add:
            print(f"  + grant {priv} on {securable_type} {name} to {principal}")
        for priv in to_remove:
            print(f"  - revoke {priv} on {securable_type} {name} from {principal}")
        change = {"principal": principal}
        if to_add:
            change["add"] = to_add
        if to_remove:
            change["remove"] = to_remove
        changes.append(change)

    if changes:
        apply_permission_changes(uri, token, securable_type, name, changes, apply)
    return changed_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Actually apply the plan (default: dry run / plan only)"
    )
    args = parser.parse_args()

    config = load_config()
    catalog = config["catalog"]
    users = config["users"]
    token = load_uc_token()

    print(f"{'Applying' if args.apply else 'Planning'} access for catalog '{catalog}'...\n")

    existing_emails = list_existing_user_emails(UC_URI, token)
    for user in users:
        if user["email"] not in existing_emails:
            create_user(UC_URI, token, user["email"], user["name"], args.apply)

    # Catalog-level privileges.
    desired_catalog = {u["email"]: set(u.get("catalog_privileges", [])) for u in users}
    total_changes = reconcile_securable(UC_URI, token, "catalog", catalog, desired_catalog, args.apply)

    # Schema-level privileges: reconcile every schema mentioned by ANY
    # user. A user who doesn't mention a schema is treated as wanting
    # zero (managed) privileges there - that's what makes bronze
    # inaccessible to data-analyst without an explicit deny.
    all_schemas = sorted({s for u in users for s in u.get("schema_privileges", {})})
    for schema in all_schemas:
        desired_schema = {
            u["email"]: set(u.get("schema_privileges", {}).get(schema, [])) for u in users
        }
        total_changes += reconcile_securable(
            UC_URI, token, "schema", f"{catalog}.{schema}", desired_schema, args.apply
        )

    if total_changes == 0:
        print("No changes - live server already matches iam/access.yaml.")
    elif not args.apply:
        print(f"\n{total_changes} principal(s) would change. Re-run with --apply to apply.")
    else:
        print(f"\nApplied changes for {total_changes} principal(s).")


if __name__ == "__main__":
    main()
