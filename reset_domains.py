from __future__ import annotations

import argparse
import sys
import time
from typing import Any, Callable, Sequence

from gmail_common import build_gmail_service, execute_request, fetch_filters

RESET_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.modify",
)


# Gmail system label IDs that must never be deleted
_SYSTEM_LABEL_IDS = {
    "INBOX", "SENT", "DRAFT", "TRASH", "SPAM", "STARRED", "UNREAD",
    "IMPORTANT", "CHAT", "CATEGORY_PERSONAL", "CATEGORY_SOCIAL",
    "CATEGORY_PROMOTIONS", "CATEGORY_UPDATES", "CATEGORY_FORUMS",
}


def find_all_user_labels(
    service: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, str]:
    resp = execute_request(
        service.users().labels().list(userId="me"),
        sleep_seconds=sleep_seconds,
        sleep_fn=sleep_fn,
    )
    user_labels: dict[str, str] = {}
    for lbl in resp.get("labels", []):
        name = lbl.get("name", "")
        lid = lbl.get("id", "")
        ltype = lbl.get("type", "")
        if not name or not lid:
            continue
        if ltype == "system" or lid in _SYSTEM_LABEL_IDS:
            continue
        user_labels[name] = lid
    return user_labels


def collect_message_ids_for_label(
    service: Any,
    label_id: str,
    *,
    sleep_seconds: float = 0.05,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[str]:
    msg_ids: list[str] = []
    page_token: str | None = None
    while True:
        req = service.users().messages().list(
            userId="me",
            labelIds=[label_id],
            maxResults=500,
            pageToken=page_token,
        )
        resp = execute_request(req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
        for m in resp.get("messages", []):
            msg_ids.append(m["id"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return msg_ids


def run_reset(
    service: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    stats = {
        "filters_deleted": 0,
        "messages_restored": 0,
        "labels_deleted": 0,
    }

    # --- Step 0: discover all user labels ---
    user_labels = find_all_user_labels(
        service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )

    if not user_labels:
        print("  No user labels found.")

    print(f"  Found {len(user_labels)} user label(s)")

    # --- Step 1: delete ALL filters ---
    all_filters = fetch_filters(
        service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )
    print(f"  Found {len(all_filters)} filter(s) to delete")
    for f in all_filters:
        fid = f.get("id", "")
        criteria = f.get("criteria") or {}
        try:
            execute_request(
                service.users().settings().filters().delete(userId="me", id=fid),
                sleep_seconds=sleep_seconds,
                sleep_fn=sleep_fn,
            )
            stats["filters_deleted"] += 1
            print(f"    Deleted filter {fid} (from={criteria.get('from', '?')})")
        except Exception as exc:  # noqa: BLE001
            print(f"    Failed to delete filter {fid}: {exc}", file=sys.stderr)

    # --- Step 2: restore messages from ALL user labels to inbox ---
    for label_name, label_id in sorted(user_labels.items()):
        msg_ids = collect_message_ids_for_label(
            service, label_id, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
        )
        if not msg_ids:
            print(f"    {label_name}: 0 messages")
            continue
        for i in range(0, len(msg_ids), 1000):
            batch = msg_ids[i : i + 1000]
            execute_request(
                service.users().messages().batchModify(
                    userId="me",
                    body={
                        "ids": batch,
                        "addLabelIds": ["INBOX"],
                        "removeLabelIds": [label_id],
                    },
                ),
                sleep_seconds=sleep_seconds,
                sleep_fn=sleep_fn,
            )
        stats["messages_restored"] += len(msg_ids)
        print(f"    {label_name}: restored {len(msg_ids)} messages to inbox")

    # --- Step 3: delete all user labels (children first, then parents) ---
    # Sort by depth descending so children are deleted before parents
    sorted_labels = sorted(
        user_labels.items(),
        key=lambda item: item[0].count("/"),
        reverse=True,
    )
    for label_name, label_id in sorted_labels:
        try:
            execute_request(
                service.users().labels().delete(userId="me", id=label_id),
                sleep_seconds=sleep_seconds,
                sleep_fn=sleep_fn,
            )
            stats["labels_deleted"] += 1
            print(f"    Deleted label: {label_name}")
        except Exception as exc:  # noqa: BLE001
            print(f"    Failed to delete label {label_name}: {exc}", file=sys.stderr)

    return stats


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Nuclear reset: delete ALL filters, restore ALL labeled messages "
            "to inbox, delete ALL user labels."
        )
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        service = build_gmail_service(scopes=RESET_SCOPES)

        # Dry-run preview
        user_labels = find_all_user_labels(service)
        all_filters = fetch_filters(service)

        if not user_labels and not all_filters:
            print("Nothing to reset: no user labels and no filters.")
            return 0

        print("Will perform FULL reset:")
        print(f"  Filters to delete: {len(all_filters)} (ALL)")
        print(f"  User labels to delete: {len(user_labels)} (ALL)")
        for name in sorted(user_labels):
            print(f"    {name}")
        print()
        answer = input("Proceed with full reset? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return 0

        stats = run_reset(service)
        print()
        print(f"Filters deleted: {stats['filters_deleted']}")
        print(f"Messages restored to inbox: {stats['messages_restored']}")
        print(f"Labels deleted: {stats['labels_deleted']}")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
