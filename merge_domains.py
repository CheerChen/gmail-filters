from __future__ import annotations

import argparse
import sys
from typing import Any

from gmail_common import build_gmail_service, execute_request, fetch_filters

MERGE_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.modify",
)

EXPECTED_FILTER_KEYS = {"addLabelIds", "removeLabelIds"}


def _validate_filter_shape(f: dict[str, Any], domain: str) -> None:
    """Exit if the filter was not created by discover_domains.py --apply."""
    criteria = f.get("criteria", {})
    action = f.get("action", {})
    add_labels = action.get("addLabelIds", [])
    remove_labels = action.get("removeLabelIds", [])

    from_value = criteria.get("from", "")
    ok = (
        from_value.lower().startswith("@")
        and len(add_labels) == 1
        and remove_labels == ["INBOX"]
        and set(action.keys()) <= EXPECTED_FILTER_KEYS
    )
    if not ok:
        print(
            f"Error: filter for {domain} does not match expected discover_domains shape.\n"
            f"  criteria: {criteria}\n"
            f"  action:   {action}",
            file=sys.stderr,
        )
        sys.exit(1)


def find_filter_for_domain(
    filters: list[dict[str, Any]], domain: str
) -> dict[str, Any] | None:
    needle = f"@{domain}".lower()
    for f in filters:
        criteria_from = f.get("criteria", {}).get("from", "")
        if criteria_from.lower() == needle:
            return f
    return None


def resolve_domain(
    filters: list[dict[str, Any]],
    label_map: dict[str, str],
    domain: str,
    role: str,
) -> tuple[dict[str, Any], str, str]:
    """Return (filter_dict, label_id, label_name) or exit on error."""
    f = find_filter_for_domain(filters, domain)
    if f is None:
        print(f"Error: no filter found for {role} domain @{domain}", file=sys.stderr)
        sys.exit(1)

    _validate_filter_shape(f, domain)

    label_id = f["action"]["addLabelIds"][0]
    label_name = label_map.get(label_id)
    if label_name is None:
        print(
            f"Error: label ID {label_id} (from {role} filter) not found in label list",
            file=sys.stderr,
        )
        sys.exit(1)

    return f, label_id, label_name


def collect_message_ids(
    service: Any,
    label_id: str,
    *,
    sleep_seconds: float = 0.1,
) -> list[str]:
    msg_ids: list[str] = []
    page_token: str | None = None
    while True:
        req = service.users().messages().list(
            userId="me", labelIds=[label_id], pageToken=page_token, maxResults=500
        )
        resp = execute_request(req, sleep_seconds=sleep_seconds)
        for m in resp.get("messages", []):
            msg_ids.append(m["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return msg_ids


def replacement_filter_exists(
    filters: list[dict[str, Any]], source_domain: str, target_label_id: str
) -> bool:
    needle = f"@{source_domain}".lower()
    for f in filters:
        criteria_from = f.get("criteria", {}).get("from", "")
        add_labels = f.get("action", {}).get("addLabelIds", [])
        if criteria_from.lower() == needle and target_label_id in add_labels:
            return True
    return False


def run_merge(
    service: Any,
    *,
    source_domain: str,
    target_domain: str,
    sleep_seconds: float = 0.1,
    confirm_fn: Any = None,
) -> None:
    # --- Step 0: Resolve ---
    filters = fetch_filters(service, sleep_seconds=sleep_seconds)

    label_req = service.users().labels().list(userId="me")
    label_resp = execute_request(label_req, sleep_seconds=sleep_seconds)
    label_map: dict[str, str] = {}
    for lbl in label_resp.get("labels", []):
        lid = lbl.get("id")
        name = lbl.get("name")
        if lid and name:
            label_map[lid] = name

    src_filter, src_label_id, src_label_name = resolve_domain(
        filters, label_map, source_domain, "source"
    )
    tgt_filter, tgt_label_id, tgt_label_name = resolve_domain(
        filters, label_map, target_domain, "target"
    )

    if src_label_id == tgt_label_id:
        print("Error: source and target resolve to the same label", file=sys.stderr)
        sys.exit(1)

    # Count messages
    msg_ids = collect_message_ids(service, src_label_id, sleep_seconds=sleep_seconds)
    msg_count = len(msg_ids)

    src_filter_id = src_filter["id"]
    tgt_filter_id = tgt_filter["id"]

    # --- Confirmation ---
    print(f'Source: {source_domain} → label "{src_label_name}" (filter: {src_filter_id})')
    print(f'Target: {target_domain} → label "{tgt_label_name}" (filter: {tgt_filter_id})')
    print(f"Messages to migrate: {msg_count}")
    print()
    print("Planned actions:")
    print(f'  1. Migrate {msg_count} messages: add "{tgt_label_name}", remove "{src_label_name}"')
    print(f"  2. Delete filter {src_filter_id} (from:@{source_domain})")
    print(f'  3. Create filter: from:@{source_domain} → "{tgt_label_name}" + archive')
    print(f'  4. Delete label: "{src_label_name}"')
    print()

    if confirm_fn is not None:
        if not confirm_fn():
            print("Aborted.")
            return
    else:
        answer = input("Proceed with merge? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    # --- Step 1: Migrate messages ---
    migrated = 0
    if msg_ids:
        for i in range(0, len(msg_ids), 1000):
            batch = msg_ids[i : i + 1000]
            req = service.users().messages().batchModify(
                userId="me",
                body={
                    "ids": batch,
                    "addLabelIds": [tgt_label_id],
                    "removeLabelIds": [src_label_id],
                },
            )
            try:
                execute_request(req, sleep_seconds=sleep_seconds)
                migrated += len(batch)
            except Exception as exc:
                print(f"  Error migrating batch: {exc}", file=sys.stderr)
    print(f"  Messages migrated: {migrated}")

    # --- Step 2: Delete source filter ---
    try:
        req = service.users().settings().filters().delete(
            userId="me", id=src_filter_id
        )
        execute_request(req, sleep_seconds=sleep_seconds)
        print(f"  Source filter deleted: {src_filter_id}")
    except Exception as exc:
        print(f"  Error deleting source filter: {exc}", file=sys.stderr)

    # --- Step 3: Create replacement filter ---
    # Re-fetch filters to check for duplicates (source filter just deleted)
    current_filters = fetch_filters(service, sleep_seconds=sleep_seconds)
    if replacement_filter_exists(current_filters, source_domain, tgt_label_id):
        print("  Replacement filter already exists, skipping creation")
    else:
        try:
            body = {
                "criteria": {"from": f"@{source_domain}"},
                "action": {
                    "addLabelIds": [tgt_label_id],
                    "removeLabelIds": ["INBOX"],
                },
            }
            req = service.users().settings().filters().create(
                userId="me", body=body
            )
            resp = execute_request(req, sleep_seconds=sleep_seconds)
            new_id = resp.get("id", "?")
            print(f"  Replacement filter created: {new_id}")
        except Exception as exc:
            print(f"  Error creating replacement filter: {exc}", file=sys.stderr)

    # --- Step 4: Delete source label ---
    try:
        req = service.users().labels().delete(userId="me", id=src_label_id)
        execute_request(req, sleep_seconds=sleep_seconds)
        print(f'  Source label deleted: "{src_label_name}"')
    except Exception as exc:
        print(f"  Error deleting source label: {exc}", file=sys.stderr)

    print()
    print("Merge complete.")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge a source domain into a target domain (labels, filters, messages)."
    )
    parser.add_argument("--source", required=True, help="Domain to merge away")
    parser.add_argument("--target", required=True, help="Domain to merge into")
    parser.add_argument(
        "--label-prefix",
        default="Domains",
        help="Label namespace prefix (default: Domains)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    service = build_gmail_service(scopes=MERGE_SCOPES)
    run_merge(service, source_domain=args.source, target_domain=args.target)


if __name__ == "__main__":
    main()
