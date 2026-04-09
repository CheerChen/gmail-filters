from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import date
from email.utils import parseaddr
from typing import Any, Callable, Sequence

from gmail_common import build_gmail_service, execute_request, fetch_filters, parse_date_header

DISCOVER_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.settings.basic",
    "https://www.googleapis.com/auth/gmail.modify",
)


@dataclass(frozen=True)
class DomainCandidate:
    domain: str
    count: int
    last_seen: date | None
    sample_senders: tuple[str, ...]
    display_name: str = ""


@dataclass(frozen=True)
class DiscoverySummary:
    total_message_ids: int
    total_messages_inspected: int
    unique_domains_found: int
    displayed_domains: int
    limit_hit: bool
    message_ids_by_domain: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveryProgressUpdate:
    stage: str
    message: str
    completed: int = 0
    total: int = 0
    current_domain: str = ""
    unique_domains: int = 0
    limit_hit: bool = False


ProgressCallback = Callable[[DiscoveryProgressUpdate], None]


def build_domain_extractor() -> Any:
    try:
        import tldextract
    except ImportError as exc:
        raise RuntimeError(
            "Missing tldextract dependency. Install tldextract before running this script."
        ) from exc

    # Use the bundled PSL snapshot to avoid first-run network dependence.
    return tldextract.TLDExtract(suffix_list_urls=())


def emit_progress(
    progress_callback: ProgressCallback | None,
    *,
    stage: str,
    message: str,
    completed: int = 0,
    total: int = 0,
    current_domain: str = "",
    unique_domains: int = 0,
    limit_hit: bool = False,
) -> None:
    if progress_callback is None:
        return

    progress_callback(
        DiscoveryProgressUpdate(
            stage=stage,
            message=message,
            completed=completed,
            total=total,
            current_domain=current_domain,
            unique_domains=unique_domains,
            limit_hit=limit_hit,
        )
    )


def extract_header_value(message_metadata: dict[str, Any], header_name: str) -> str | None:
    payload = message_metadata.get("payload") or {}
    for header in payload.get("headers", []):
        if header.get("name", "").lower() == header_name.lower():
            value = header.get("value")
            if value:
                return value
    return None


def extract_sender_email(from_header: str) -> tuple[str, str] | None:
    display_name, address = parseaddr(from_header)
    address = address.strip().lower()
    if not address:
        return None
    return address, display_name.strip()


def split_sender_email(sender_email: str) -> tuple[str, str] | None:
    local_part, separator, domain = sender_email.partition("@")
    if separator != "@" or not local_part or not domain:
        return None
    return local_part, domain


def normalize_sender_domain(domain: str, extractor: Any) -> str:
    extracted = extractor(domain.lower())
    registered_domain = extracted.top_domain_under_public_suffix
    return registered_domain.lower() if registered_domain else domain.lower()


# Freemail / personal mailbox domains — not useful for automated filters
EXCLUDED_DOMAINS = frozenset({
    "gmail.com",
    "googlemail.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "yahoo.com",
    "yahoo.co.jp",
    "ymail.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "mail.com",
    "protonmail.com",
    "proton.me",
    "zoho.com",
    "yandex.com",
    "qq.com",
    "163.com",
    "126.com",
    "sina.com",
    "foxmail.com",
    "naver.com",
})


def fetch_recent_message_ids(
    service: Any,
    *,
    days: int,
    limit: int,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[str], bool]:
    query = f"in:inbox newer_than:{days}d"
    page_token: str | None = None
    message_ids: list[str] = []
    limit_hit = False

    emit_progress(
        progress_callback,
        stage="list",
        message="Listing recent inbox messages",
        total=limit,
    )

    while len(message_ids) < limit:
        request = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=500,
            pageToken=page_token,
        )
        response = execute_request(
            request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
        )
        messages = response.get("messages", [])
        if not messages:
            break

        remaining = limit - len(message_ids)
        page_ids = [message["id"] for message in messages[:remaining]]
        message_ids.extend(page_ids)
        limit_hit = len(messages) > remaining

        emit_progress(
            progress_callback,
            stage="list",
            message="Collected recent inbox message IDs",
            completed=len(message_ids),
            total=limit,
            limit_hit=limit_hit,
        )

        if limit_hit:
            break

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return message_ids, limit_hit


def fetch_message_metadata(
    service: Any,
    message_id: str,
    *,
    sleep_seconds: float = 0.05,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    request = service.users().messages().get(
        userId="me",
        id=message_id,
        format="metadata",
        metadataHeaders=["From", "Date"],
    )
    return execute_request(request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)


def fetch_message_metadata_batch(
    service: Any,
    message_ids: Sequence[str],
    *,
    batch_size: int = 50,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: ProgressCallback | None = None,
    limit_hit: bool = False,
) -> dict[str, dict[str, Any]]:
    try:
        from googleapiclient.http import BatchHttpRequest
    except ImportError as exc:
        raise RuntimeError(
            "Missing google-api-python-client for batch requests."
        ) from exc

    results: dict[str, dict[str, Any]] = {}
    total = len(message_ids)

    for i in range(0, total, batch_size):
        chunk = message_ids[i : i + batch_size]

        def _make_callback(msg_id: str) -> Any:
            def _cb(request_id: str, response: Any, exception: Any) -> None:
                if exception is None:
                    results[msg_id] = response
            return _cb

        batch = service.new_batch_http_request()
        for msg_id in chunk:
            req = service.users().messages().get(
                userId="me",
                id=msg_id,
                format="metadata",
                metadataHeaders=["From", "Date"],
            )
            batch.add(req, callback=_make_callback(msg_id))
        batch.execute()
        if sleep_seconds > 0:
            sleep_fn(sleep_seconds)

        fetched = min(i + batch_size, total)
        emit_progress(
            progress_callback,
            stage="fetch",
            message=f"Fetched {fetched}/{total} messages",
            completed=fetched,
            total=total,
            limit_hit=limit_hit,
        )

    return results


def sort_domain_candidates(candidates: Sequence[DomainCandidate]) -> list[DomainCandidate]:
    def sort_key(candidate: DomainCandidate) -> tuple[int, int, str]:
        return (
            -candidate.count,
            -(candidate.last_seen.toordinal() if candidate.last_seen else 0),
            candidate.domain,
        )

    return sorted(candidates, key=sort_key)


def _pick_display_name(display_names: dict[str, int], fallback_domain: str) -> str:
    if not display_names:
        return fallback_domain
    return max(display_names, key=display_names.__getitem__)


def discover_domains(
    service: Any,
    *,
    days: int,
    minimum_count: int,
    limit: int,
    list_sleep_seconds: float = 0.1,
    detail_sleep_seconds: float = 0.05,
    sleep_fn: Callable[[float], None] = time.sleep,
    extractor: Any | None = None,
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[DomainCandidate], DiscoverySummary]:
    domain_extractor = extractor or build_domain_extractor()
    message_ids, limit_hit = fetch_recent_message_ids(
        service,
        days=days,
        limit=limit,
        sleep_seconds=list_sleep_seconds,
        sleep_fn=sleep_fn,
        progress_callback=progress_callback,
    )

    emit_progress(
        progress_callback,
        stage="fetch",
        message="Fetching sender metadata",
        total=len(message_ids),
        limit_hit=limit_hit,
    )

    # Batch fetch all metadata (50 per HTTP request, with per-chunk progress)
    metadata_map = fetch_message_metadata_batch(
        service,
        message_ids,
        sleep_seconds=detail_sleep_seconds,
        sleep_fn=sleep_fn,
        progress_callback=progress_callback,
        limit_hit=limit_hit,
    )

    aggregated: dict[str, dict[str, Any]] = {}
    inspected = 0

    for index, message_id in enumerate(message_ids, start=1):
        metadata = metadata_map.get(message_id)
        if metadata is None:
            continue
        inspected += 1
        from_header = extract_header_value(metadata, "From")
        if not from_header:
            emit_progress(
                progress_callback,
                stage="fetch",
                message="Skipped message with no From header",
                completed=index,
                total=len(message_ids),
                unique_domains=len(aggregated),
                limit_hit=limit_hit,
            )
            continue

        sender_result = extract_sender_email(from_header)
        if not sender_result:
            emit_progress(
                progress_callback,
                stage="fetch",
                message="Skipped unparsable sender",
                completed=index,
                total=len(message_ids),
                unique_domains=len(aggregated),
                limit_hit=limit_hit,
            )
            continue

        sender_email, sender_display_name = sender_result
        split_sender = split_sender_email(sender_email)
        if split_sender is None:
            emit_progress(
                progress_callback,
                stage="fetch",
                message="Skipped invalid sender address",
                completed=index,
                total=len(message_ids),
                unique_domains=len(aggregated),
                limit_hit=limit_hit,
            )
            continue

        local_part, sender_domain = split_sender
        normalized_domain = normalize_sender_domain(sender_domain, domain_extractor)
        if normalized_domain in EXCLUDED_DOMAINS:
            continue
        date_header = extract_header_value(metadata, "Date")
        message_date = parse_date_header(date_header).date() if date_header else None

        entry = aggregated.setdefault(
            normalized_domain,
            {
                "count": 0,
                "last_seen": None,
                "sample_senders": [],
                "sender_lookup": set(),
                "message_ids": [],
                "display_names": {},
            },
        )
        entry["count"] += 1
        entry["message_ids"].append(message_id)
        if sender_display_name:
            entry["display_names"][sender_display_name] = (
                entry["display_names"].get(sender_display_name, 0) + 1
            )
        if message_date and (
            entry["last_seen"] is None or message_date > entry["last_seen"]
        ):
            entry["last_seen"] = message_date
        if local_part not in entry["sender_lookup"] and len(entry["sample_senders"]) < 3:
            entry["sender_lookup"].add(local_part)
            entry["sample_senders"].append(local_part)

        emit_progress(
            progress_callback,
            stage="fetch",
            message="Fetched sender metadata",
            completed=index,
            total=len(message_ids),
            current_domain=normalized_domain,
            unique_domains=len(aggregated),
            limit_hit=limit_hit,
        )

    candidates = sort_domain_candidates(
        [
            DomainCandidate(
                domain=domain,
                count=int(data["count"]),
                last_seen=data["last_seen"],
                sample_senders=tuple(data["sample_senders"]),
                display_name=_pick_display_name(data["display_names"], domain),
            )
            for domain, data in aggregated.items()
            if int(data["count"]) >= minimum_count
        ]
    )
    summary = DiscoverySummary(
        total_message_ids=len(message_ids),
        total_messages_inspected=inspected,
        unique_domains_found=len(aggregated),
        displayed_domains=len(candidates),
        limit_hit=limit_hit,
        message_ids_by_domain={
            domain: tuple(data["message_ids"])
            for domain, data in aggregated.items()
        },
    )
    emit_progress(
        progress_callback,
        stage="done",
        message="Domain discovery complete",
        completed=inspected,
        total=len(message_ids),
        unique_domains=len(aggregated),
        limit_hit=limit_hit,
    )
    return candidates, summary


def format_last_seen(value: date | None) -> str:
    return value.isoformat() if value else "(unknown)"


def render_table(
    candidates: Sequence[DomainCandidate],
    *,
    tabulate_fn: Callable[..., str] | None = None,
) -> str:
    if tabulate_fn is None:
        try:
            from tabulate import tabulate as tabulate_fn
        except ImportError as exc:
            raise RuntimeError(
                "Missing tabulate dependency. Install tabulate before running this script."
            ) from exc

    rows = [
        [
            str(candidate.count),
            format_last_seen(candidate.last_seen),
            candidate.domain,
            candidate.display_name,
            ", ".join(candidate.sample_senders) if candidate.sample_senders else "(none)",
        ]
        for candidate in candidates
    ]

    return tabulate_fn(
        rows,
        headers=["Count", "Last seen", "Domain", "Label name", "Sample senders"],
        tablefmt="simple",
        stralign="left",
        disable_numparse=True,
    )


def print_summary(summary: DiscoverySummary) -> None:
    print()
    print(f"Total message IDs collected: {summary.total_message_ids}")
    print(f"Total messages inspected: {summary.total_messages_inspected}")
    print(f"Unique domains found: {summary.unique_domains_found}")
    print(f"Domains above threshold: {summary.displayed_domains}")
    print(f"Safety limit hit: {'yes' if summary.limit_hit else 'no'}")


@dataclass(frozen=True)
class ApplySummary:
    labels_created: int
    labels_reused: int
    filters_created: int
    filters_skipped: int
    domains_applied: int
    domains_failed: int


def run_apply(
    service: Any,
    candidates: Sequence[DomainCandidate],
    message_ids_by_domain: dict[str, tuple[str, ...]],
    label_prefix: str,
    *,
    archive: bool = True,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> ApplySummary:
    label_list_resp = execute_request(
        service.users().labels().list(userId="me"),
        sleep_seconds=sleep_seconds,
        sleep_fn=sleep_fn,
    )
    existing_labels: dict[str, str] = {
        lbl["name"]: lbl["id"]
        for lbl in label_list_resp.get("labels", [])
        if lbl.get("name") and lbl.get("id")
    }

    # Ensure parent label exists for proper hierarchy
    if label_prefix not in existing_labels:
        req = service.users().labels().create(
            userId="me", body={"name": label_prefix}
        )
        parent_label = execute_request(
            req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
        )
        existing_labels[label_prefix] = parent_label["id"]
        print(f"  Parent label created: {label_prefix}")

    existing_filter_map: dict[str, str] = {}
    for f in fetch_filters(service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn):
        criteria = f.get("criteria") or {}
        from_val = criteria.get("from", "").strip().lower()
        if from_val.startswith("@") and f.get("id"):
            existing_filter_map[from_val] = f["id"]

    labels_created = labels_reused = filters_created = filters_skipped = 0
    domains_applied = domains_failed = 0

    for candidate in candidates:
        domain = candidate.domain
        label_name = f"{label_prefix}/{candidate.display_name}"
        msg_ids = message_ids_by_domain.get(domain, ())
        try:
            if label_name in existing_labels:
                label_id = existing_labels[label_name]
                labels_reused += 1
                print(f"  [{domain}] Label reused: {label_name}")
            else:
                req = service.users().labels().create(
                    userId="me", body={"name": label_name}
                )
                new_label = execute_request(
                    req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
                )
                label_id = new_label["id"]
                existing_labels[label_name] = label_id
                labels_created += 1
                print(f"  [{domain}] Label created: {label_name}")

            target_from = f"@{domain}".lower()
            if target_from in existing_filter_map:
                filters_skipped += 1
                print(
                    f"  [{domain}] Filter skipped "
                    f"(already exists: {existing_filter_map[target_from]})"
                )
            else:
                filter_action: dict[str, Any] = {"addLabelIds": [label_id]}
                if archive:
                    filter_action["removeLabelIds"] = ["INBOX"]
                filter_body = {
                    "criteria": {"from": f"@{domain}"},
                    "action": filter_action,
                }
                req = service.users().settings().filters().create(
                    userId="me", body=filter_body
                )
                execute_request(req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
                filters_created += 1
                print(f"  [{domain}] Filter created for @{domain}")

            if msg_ids:
                for i in range(0, len(msg_ids), 1000):
                    batch = list(msg_ids[i : i + 1000])
                    modify_body: dict[str, Any] = {
                        "ids": batch,
                        "addLabelIds": [label_id],
                    }
                    if archive:
                        modify_body["removeLabelIds"] = ["INBOX"]
                    req = service.users().messages().batchModify(
                        userId="me", body=modify_body,
                    )
                    execute_request(req, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
                action_desc = "labeled + archived" if archive else "labeled"
                print(f"  [{domain}] {action_desc} {len(msg_ids)} existing messages")

            domains_applied += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  [{domain}] ERROR: {exc}", file=sys.stderr)
            domains_failed += 1

    return ApplySummary(
        labels_created=labels_created,
        labels_reused=labels_reused,
        filters_created=filters_created,
        filters_skipped=filters_skipped,
        domains_applied=domains_applied,
        domains_failed=domains_failed,
    )


def print_apply_summary(apply_summary: ApplySummary) -> None:
    print()
    print(f"Labels created: {apply_summary.labels_created}")
    print(f"Labels reused: {apply_summary.labels_reused}")
    print(f"Filters created: {apply_summary.filters_created}")
    print(f"Filters reused/skipped: {apply_summary.filters_skipped}")
    print(f"Domains successfully applied: {apply_summary.domains_applied}")
    print(f"Domains failed: {apply_summary.domains_failed}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan recent inbox messages and rank sender domains worth reviewing "
            "for possible Gmail filters."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Time window to scan in days (default: 90).",
    )
    parser.add_argument(
        "--min",
        type=int,
        default=10,
        dest="minimum_count",
        help="Minimum email count required to display a domain (default: 10).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Safety cap on the number of recent messages to inspect (default: 5000).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help=(
            "Create or reuse labels and filters, then apply labels to already-existing "
            "scanned messages after a single y/N confirmation."
        ),
    )
    parser.add_argument(
        "--label-prefix",
        default="Domains",
        dest="label_prefix",
        help='Label namespace prefix (default: "Domains").',
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        default=False,
        dest="no_archive",
        help="Do not remove messages from inbox (skip archiving).",
    )
    return parser.parse_args(argv)


class RichDiscoveryProgress:
    def __init__(self) -> None:
        try:
            from rich.console import Console, Group
            from rich.live import Live
            from rich.panel import Panel
            from rich.progress import (
                BarColumn,
                MofNCompleteColumn,
                Progress,
                SpinnerColumn,
                TaskProgressColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )
            from rich.table import Table
        except ImportError as exc:
            raise RuntimeError(
                "Missing rich dependency. Install rich before using the progress UI."
            ) from exc

        self._Group = Group
        self._Live = Live
        self._Panel = Panel
        self._Table = Table
        self._console = Console(stderr=True)
        self._latest = DiscoveryProgressUpdate(stage="setup", message="Starting")
        self._progress = Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=28),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
            transient=True,
            expand=True,
        )
        self._task_id = self._progress.add_task("Preparing domain discovery", total=1)
        self._live = Live(
            self._render(),
            console=self._console,
            refresh_per_second=8,
            transient=True,
        )

    def __enter__(self) -> "RichDiscoveryProgress":
        self._live.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._live.stop()

    def update(self, event: DiscoveryProgressUpdate) -> None:
        self._latest = event
        task = next(task for task in self._progress.tasks if task.id == self._task_id)
        total = event.total or task.total or 1
        completed = min(event.completed, total)
        self._progress.update(
            self._task_id,
            description=self._build_description(event),
            total=total,
            completed=completed,
        )
        self._live.update(self._render(), refresh=True)

    def _build_description(self, event: DiscoveryProgressUpdate) -> str:
        stage_label = {
            "setup": "Setup",
            "list": "List",
            "fetch": "Fetch",
            "done": "Done",
        }.get(event.stage, event.stage.title())
        return f"{stage_label}: {event.message}"

    def _render(self) -> Any:
        details = self._Table.grid(padding=(0, 1))
        details.add_column(style="bold cyan", no_wrap=True)
        details.add_column()

        details.add_row("Stage", self._latest.stage.title())
        details.add_row("Status", self._latest.message)
        details.add_row(
            "Current domain",
            self._latest.current_domain or "(waiting)",
        )
        details.add_row("Unique domains", str(self._latest.unique_domains))
        details.add_row(
            "Safety limit",
            "hit" if self._latest.limit_hit else "not hit",
        )

        return self._Group(
            self._progress,
            self._Panel(details, title="Discover Domains", border_style="blue"),
        )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        progress_ui = RichDiscoveryProgress() if sys.stderr.isatty() else None
        if progress_ui is None:
            service = build_gmail_service(scopes=DISCOVER_SCOPES)
            candidates, summary = discover_domains(
                service,
                days=args.days,
                minimum_count=args.minimum_count,
                limit=args.limit,
            )
        else:
            with progress_ui:
                progress_ui.update(
                    DiscoveryProgressUpdate(
                        stage="setup",
                        message="Authenticating with Gmail",
                    )
                )
                service = build_gmail_service(scopes=DISCOVER_SCOPES)
                progress_ui.update(
                    DiscoveryProgressUpdate(
                        stage="setup",
                        message="Connected to Gmail",
                    )
                )
                candidates, summary = discover_domains(
                    service,
                    days=args.days,
                    minimum_count=args.minimum_count,
                    limit=args.limit,
                    progress_callback=progress_ui.update,
                )
        if candidates:
            print(render_table(candidates))
        else:
            print("No domains met the selected threshold.")
        print_summary(summary)
        if args.apply:
            if not candidates:
                print("No domains to apply labels to.", file=sys.stderr)
                return 0
            print()
            archive_mode = "archive" if not args.no_archive else "label only"
            print(f"Planned actions (mode: {archive_mode}):")
            for candidate in candidates:
                label_name = f"{args.label_prefix}/{candidate.display_name}"
                msg_count = len(summary.message_ids_by_domain.get(candidate.domain, ()))
                print(
                    f"  {candidate.domain:<30}  label={label_name!r}  "
                    f"filter=from:@{candidate.domain}  existing_msgs={msg_count}"
                )
            print()
            answer = input(
                f"Apply labels and filters for all {len(candidates)} domains? [y/N] "
            ).strip().lower()
            if answer != "y":
                print("Aborted.")
                return 0
            apply_summary = run_apply(
                service,
                candidates,
                summary.message_ids_by_domain,
                args.label_prefix,
                archive=not args.no_archive,
            )
            print_apply_summary(apply_summary)
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
