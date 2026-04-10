from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from email.utils import parseaddr
from typing import Any, Callable, Sequence

from gmail_common import build_gmail_service, execute_request, fetch_filters

CATCHUP_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.modify",
)


@dataclass(frozen=True)
class CatchupItem:
    domain: str
    label_name: str
    message_ids: tuple[str, ...]


def build_domain_filter_map(
    service: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, tuple[str, str]]:
    """Return {registered_domain: (label_id, label_name)} for all existing filters."""
    filters = fetch_filters(service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
    label_resp = execute_request(
        service.users().labels().list(userId="me"),
        sleep_seconds=sleep_seconds,
        sleep_fn=sleep_fn,
    )
    lid_to_name: dict[str, str] = {
        lbl["id"]: lbl["name"]
        for lbl in label_resp.get("labels", [])
        if lbl.get("id") and lbl.get("name")
    }

    domain_map: dict[str, tuple[str, str]] = {}
    for f in filters:
        from_val = (f.get("criteria") or {}).get("from", "").strip().lower()
        add_labels = (f.get("action") or {}).get("addLabelIds", [])
        if from_val.startswith("@") and add_labels:
            domain = from_val[1:]  # strip leading @
            label_id = add_labels[0]
            label_name = lid_to_name.get(label_id, f"<id:{label_id}>")
            domain_map[domain] = (label_id, label_name)
    return domain_map


def fetch_inbox_message_ids(
    service: Any,
    *,
    days: int,
    limit: int,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[str]:
    query = f"in:inbox newer_than:{days}d"
    page_token: str | None = None
    message_ids: list[str] = []

    while len(message_ids) < limit:
        req = service.users().messages().list(
            userId="me", q=query, maxResults=500, pageToken=page_token
        )
        resp = execute_request(req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
        messages = resp.get("messages", [])
        if not messages:
            break
        remaining = limit - len(message_ids)
        message_ids.extend(m["id"] for m in messages[:remaining])
        if len(messages) > remaining:
            break
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return message_ids


def fetch_metadata_batch(
    service: Any,
    message_ids: Sequence[str],
    *,
    batch_size: int = 50,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    total = len(message_ids)
    for i in range(0, total, batch_size):
        chunk = message_ids[i : i + batch_size]

        def _make_cb(msg_id: str) -> Any:
            def _cb(_req_id: str, response: Any, exception: Any) -> None:
                if exception is None:
                    results[msg_id] = response
            return _cb

        batch = service.new_batch_http_request()
        for msg_id in chunk:
            req = service.users().messages().get(
                userId="me", id=msg_id, format="metadata", metadataHeaders=["From"]
            )
            batch.add(req, callback=_make_cb(msg_id))
        batch.execute()
        if sleep_seconds > 0:
            sleep_fn(sleep_seconds)
        fetched = min(i + batch_size, total)
        print(f"  Fetched {fetched}/{total} messages", end="\r", file=sys.stderr)
    if total:
        print(file=sys.stderr)  # clear the \r line
    return results


def extract_sender_domain(metadata: dict[str, Any]) -> str | None:
    payload = metadata.get("payload") or {}
    for header in payload.get("headers", []):
        if header.get("name", "").lower() == "from":
            value = header.get("value", "")
            _, address = parseaddr(value)
            address = address.strip().lower()
            if "@" in address:
                return address.split("@", 1)[1]
    return None


def normalize_domain(domain: str, extractor: Any) -> str:
    extracted = extractor(domain.lower())
    registered = extracted.top_domain_under_public_suffix
    return registered.lower() if registered else domain.lower()


def find_catchup_items(
    service: Any,
    *,
    days: int,
    limit: int,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
    extractor: Any | None = None,
    domain_filter_map: dict[str, tuple[str, str]] | None = None,
) -> list[CatchupItem]:
    if extractor is None:
        try:
            import tldextract
        except ImportError as exc:
            raise RuntimeError("Missing tldextract dependency.") from exc
        extractor = tldextract.TLDExtract(suffix_list_urls=())

    if domain_filter_map is None:
        domain_filter_map = build_domain_filter_map(
            service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
        )

    print(f"Existing filters: {len(domain_filter_map)} domains", file=sys.stderr)

    msg_ids = fetch_inbox_message_ids(
        service, days=days, limit=limit,
        sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )
    if not msg_ids:
        return []

    print(f"Inbox messages to check: {len(msg_ids)}", file=sys.stderr)

    metadata_map = fetch_metadata_batch(
        service, msg_ids,
        sleep_seconds=sleep_seconds, sleep_fn=sleep_fn,
    )

    # Group by domain → message IDs (only for domains that have filters)
    caught: dict[str, list[str]] = {}
    for msg_id in msg_ids:
        meta = metadata_map.get(msg_id)
        if meta is None:
            continue
        raw_domain = extract_sender_domain(meta)
        if raw_domain is None:
            continue
        normalized = normalize_domain(raw_domain, extractor)
        if normalized in domain_filter_map:
            caught.setdefault(normalized, []).append(msg_id)

    # Build sorted result
    items = []
    for domain in sorted(caught, key=lambda d: (-len(caught[d]), d)):
        _, label_name = domain_filter_map[domain]
        items.append(CatchupItem(
            domain=domain,
            label_name=label_name,
            message_ids=tuple(caught[domain]),
        ))
    return items


def render_report(items: list[CatchupItem]) -> str:
    try:
        from tabulate import tabulate
    except ImportError as exc:
        raise RuntimeError("Missing tabulate dependency.") from exc

    if not items:
        return "No missed messages found in inbox."

    total = sum(len(item.message_ids) for item in items)
    rows = [
        [len(item.message_ids), item.domain, item.label_name]
        for item in items
    ]
    table = tabulate(
        rows,
        headers=["Count", "Domain", "Should be in label"],
        tablefmt="simple",
        stralign="left",
        disable_numparse=True,
    )
    return f"{table}\n\nTotal missed messages: {total}"


def apply_catchup(
    service: Any,
    items: list[CatchupItem],
    domain_filter_map: dict[str, tuple[str, str]],
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    for item in items:
        label_id, label_name = domain_filter_map[item.domain]
        msg_ids = list(item.message_ids)
        labeled = 0
        for i in range(0, len(msg_ids), 1000):
            batch = msg_ids[i : i + 1000]
            req = service.users().messages().batchModify(
                userId="me",
                body={
                    "ids": batch,
                    "addLabelIds": [label_id],
                    "removeLabelIds": ["INBOX"],
                },
            )
            try:
                execute_request(req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
                labeled += len(batch)
            except Exception as exc:
                print(f"  Error batch for {item.domain}: {exc}", file=sys.stderr)
        print(f"  {item.domain}: labeled + archived {labeled} messages → \"{label_name}\"")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find inbox messages that should have been caught by existing filters."
    )
    parser.add_argument(
        "--days", type=int, default=90,
        help="Time window to scan (default: 90).",
    )
    parser.add_argument(
        "--limit", type=int, default=5000,
        help="Max inbox messages to check (default: 5000).",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Scan all matching inbox messages, ignoring --limit.",
    )
    parser.add_argument(
        "--apply", action="store_true", default=False,
        help="Apply labels and archive matched messages after confirmation.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    limit = sys.maxsize if args.all else args.limit
    scopes = CATCHUP_SCOPES
    try:
        service = build_gmail_service(scopes=scopes)
        domain_filter_map = build_domain_filter_map(service, sleep_seconds=0.1)
        items = find_catchup_items(
            service, days=args.days, limit=limit,
            domain_filter_map=domain_filter_map,
        )
        print(render_report(items))
        if args.apply and items:
            print()
            total = sum(len(item.message_ids) for item in items)
            answer = input(
                f"Apply labels + archive for {total} messages across "
                f"{len(items)} domains? [y/N] "
            ).strip().lower()
            if answer != "y":
                print("Aborted.")
                return 0
            apply_catchup(service, items, domain_filter_map)
            print("\nCatchup complete.")
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
