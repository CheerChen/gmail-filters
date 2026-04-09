from __future__ import annotations

import argparse
import math
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Sequence

from gmail_common import (
    GMAIL_SCOPES,
    build_gmail_service,
    compute_days_ago,
    execute_request,
    extract_date_header,
    fetch_filters,
    fetch_label_map,
    parse_date_header,
    token_looks_like_client_config,
)


@dataclass(frozen=True)
class FilterAuditResult:
    filter_id: str
    query: str
    labels: tuple[str, ...]
    match_count: int
    last_seen: date | None
    days_ago: float


@dataclass(frozen=True)
class AuditProgressUpdate:
    stage: str
    message: str
    completed: int = 0
    total: int = 0
    labels: tuple[str, ...] = ()
    query: str = ""
    match_count: int | None = None
    last_seen: date | None = None
    no_match: int = 0
    over_180: int = 0
    over_90: int = 0
    skipped_empty: int = 0


ProgressCallback = Callable[[AuditProgressUpdate], None]


def build_filter_query(criteria: dict[str, Any] | None) -> str:
    if not criteria:
        return ""

    parts: list[str] = []
    mapping = (
        ("from", "from:({value})"),
        ("to", "to:({value})"),
        ("subject", "subject:({value})"),
    )

    for key, template in mapping:
        value = str(criteria.get(key, "")).strip()
        if value:
            parts.append(template.format(value=value))

    raw_query = str(criteria.get("query", "")).strip()
    if raw_query:
        parts.append(raw_query)

    negated_query = str(criteria.get("negatedQuery", "")).strip()
    if negated_query:
        parts.append(f"-({negated_query})")

    return " ".join(parts)


def resolve_label_names(
    filter_data: dict[str, Any], label_map: dict[str, str]
) -> tuple[str, ...]:
    action = filter_data.get("action") or {}
    label_ids = action.get("addLabelIds") or []
    names = [label_map.get(label_id, f"Unknown({label_id})") for label_id in label_ids]
    return tuple(names)


def fetch_latest_message_info(
    service: Any,
    query: str,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> tuple[date | None, int]:
    page_token: str | None = None
    match_count = 0
    latest_message_id: str | None = None

    while True:
        list_request = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=500,
            pageToken=page_token,
        )
        list_response = execute_request(
            list_request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
        )
        messages = list_response.get("messages", [])
        if latest_message_id is None and messages:
            latest_message_id = messages[0]["id"]
        match_count += len(messages)
        page_token = list_response.get("nextPageToken")
        if not page_token:
            break

    if latest_message_id is None:
        return None, 0

    get_request = service.users().messages().get(
        userId="me",
        id=latest_message_id,
        format="metadata",
        metadataHeaders=["Date"],
    )
    message_response = execute_request(
        get_request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
    )
    date_header = extract_date_header(message_response)
    if not date_header:
        raise ValueError(f"Message {latest_message_id} is missing a Date header")
    return parse_date_header(date_header).date(), match_count


def emit_progress(
    progress_callback: ProgressCallback | None,
    *,
    stage: str,
    message: str,
    completed: int = 0,
    total: int = 0,
    labels: tuple[str, ...] = (),
    query: str = "",
    match_count: int | None = None,
    last_seen: date | None = None,
    no_match: int = 0,
    over_180: int = 0,
    over_90: int = 0,
    skipped_empty: int = 0,
) -> None:
    if progress_callback is None:
        return

    progress_callback(
        AuditProgressUpdate(
            stage=stage,
            message=message,
            completed=completed,
            total=total,
            labels=labels,
            query=query,
            match_count=match_count,
            last_seen=last_seen,
            no_match=no_match,
            over_180=over_180,
            over_90=over_90,
            skipped_empty=skipped_empty,
        )
    )


def audit_filters(
    service: Any,
    *,
    today: date | None = None,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[FilterAuditResult], dict[str, int]]:
    reference_day = today or datetime.now(timezone.utc).date()
    emit_progress(progress_callback, stage="setup", message="Loading labels")
    label_map = fetch_label_map(
        service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
    )
    emit_progress(
        progress_callback,
        stage="setup",
        message=f"Loaded {len(label_map)} labels",
    )
    emit_progress(progress_callback, stage="setup", message="Loading filters")
    filters = fetch_filters(service, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
    emit_progress(
        progress_callback,
        stage="scan",
        message=f"Scanning {len(filters)} filters",
        total=len(filters),
    )

    results: list[FilterAuditResult] = []
    skipped_empty = 0
    no_match_count = 0
    over_180_count = 0
    over_90_count = 0

    for index, filter_data in enumerate(filters, start=1):
        criteria = filter_data.get("criteria") or {}
        labels = resolve_label_names(filter_data, label_map)
        query = build_filter_query(criteria)
        emit_progress(
            progress_callback,
            stage="scan",
            message="Scanning filter",
            completed=index - 1,
            total=len(filters),
            labels=labels,
            query=query,
            no_match=no_match_count,
            over_180=over_180_count,
            over_90=over_90_count,
            skipped_empty=skipped_empty,
        )
        if not query:
            skipped_empty += 1
            emit_progress(
                progress_callback,
                stage="scan",
                message="Skipped empty criteria",
                completed=index,
                total=len(filters),
                labels=labels,
                no_match=no_match_count,
                over_180=over_180_count,
                over_90=over_90_count,
                skipped_empty=skipped_empty,
            )
            continue

        last_seen, match_count = fetch_latest_message_info(
            service, query, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn
        )
        if last_seen is None:
            days_ago = math.inf
            no_match_count += 1
            over_180_count += 1
            over_90_count += 1
        else:
            days_ago = (reference_day - last_seen).days
            if days_ago > 180:
                over_180_count += 1
                over_90_count += 1
            elif days_ago > 90:
                over_90_count += 1

        results.append(
            FilterAuditResult(
                filter_id=str(filter_data.get("id", "")),
                query=query,
                labels=labels,
                match_count=match_count,
                last_seen=last_seen,
                days_ago=days_ago,
            )
        )
        emit_progress(
            progress_callback,
            stage="scan",
            message="No matching email" if last_seen is None else "Loaded latest message",
            completed=index,
            total=len(filters),
            labels=labels,
            query=query,
            match_count=match_count,
            last_seen=last_seen,
            no_match=no_match_count,
            over_180=over_180_count,
            over_90=over_90_count,
            skipped_empty=skipped_empty,
        )

    results.sort(key=lambda result: result.query)
    results.sort(key=lambda result: result.days_ago, reverse=True)

    summary = {
        "total_filters": len(filters),
        "skipped_empty_criteria": skipped_empty,
        "no_match": no_match_count,
        "over_180": over_180_count,
        "over_90": over_90_count,
    }
    emit_progress(
        progress_callback,
        stage="done",
        message="Audit complete",
        completed=len(filters),
        total=len(filters),
        no_match=no_match_count,
        over_180=over_180_count,
        over_90=over_90_count,
        skipped_empty=skipped_empty,
    )
    return results, summary


class RichAuditProgress:
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

        self._Console = Console
        self._Group = Group
        self._Live = Live
        self._Panel = Panel
        self._Table = Table
        self._console = Console(stderr=True)
        self._latest = AuditProgressUpdate(stage="setup", message="Starting")
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
        self._task_id = self._progress.add_task(
            "Preparing Gmail audit", total=1, completed=0
        )
        self._live = Live(
            self._render(),
            console=self._console,
            refresh_per_second=8,
            transient=True,
        )

    def __enter__(self) -> "RichAuditProgress":
        self._live.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._live.stop()

    def update(self, event: AuditProgressUpdate) -> None:
        self._latest = event
        description = self._build_description(event)
        task = next(task for task in self._progress.tasks if task.id == self._task_id)
        total = event.total or task.total or 1
        completed = min(event.completed, total)
        self._progress.update(
            self._task_id,
            description=description,
            total=total,
            completed=completed,
        )
        self._live.update(self._render(), refresh=True)

    def _build_description(self, event: AuditProgressUpdate) -> str:
        stage_label = {
            "setup": "Setup",
            "scan": "Scan",
            "done": "Done",
        }.get(event.stage, event.stage.title())
        return f"{stage_label}: {event.message}"

    def _render(self) -> Any:
        details = self._Table.grid(padding=(0, 1))
        details.add_column(style="bold cyan", no_wrap=True)
        details.add_column()

        label_text = ", ".join(self._latest.labels) if self._latest.labels else "(none)"
        query_text = self._truncate(self._latest.query or "(none)")
        current_text = (
            f"matches={self._latest.match_count} last={format_last_seen(self._latest.last_seen)}"
            if self._latest.match_count is not None
            else "(waiting)"
        )
        stats_text = (
            f"no match={self._latest.no_match}  "
            f"stale>180={self._latest.over_180}  "
            f"stale>90={self._latest.over_90}  "
            f"skipped={self._latest.skipped_empty}"
        )

        details.add_row("Stage", self._latest.stage.title())
        details.add_row("Status", self._latest.message)
        details.add_row("Labels", label_text)
        details.add_row("Query", query_text)
        details.add_row("Current", current_text)
        details.add_row("Stats", stats_text)

        return self._Group(
            self._progress,
            self._Panel(details, title="Gmail Filter Audit", border_style="blue"),
        )

    @staticmethod
    def _truncate(value: str, max_width: int = 72) -> str:
        if len(value) <= max_width:
            return value
        return f"{value[: max_width - 3]}..."


def resolve_selection_criteria(
    *,
    threshold_days: int | None,
    max_matches: int | None,
    show_all: bool,
) -> tuple[int | None, int | None]:
    if show_all:
        return None, None
    if threshold_days is not None:
        return threshold_days, max_matches
    if max_matches is not None:
        return None, max_matches
    return 180, None


def matches_selection(
    result: FilterAuditResult,
    *,
    threshold_days: int | None,
    max_matches: int | None,
) -> bool:
    if threshold_days is not None and not (
        math.isinf(result.days_ago) or result.days_ago > threshold_days
    ):
        return False
    if max_matches is not None and not (result.match_count < max_matches):
        return False
    return True


def select_results(
    results: Sequence[FilterAuditResult],
    *,
    threshold_days: int | None,
    max_matches: int | None,
    show_all: bool,
) -> list[FilterAuditResult]:
    if show_all:
        return list(results)
    return [
        result
        for result in results
        if matches_selection(
            result,
            threshold_days=threshold_days,
            max_matches=max_matches,
        )
    ]


def format_labels(labels: Sequence[str]) -> str:
    return ", ".join(labels) if labels else "(none)"


def format_last_seen(value: date | None) -> str:
    return value.isoformat() if value else "(no match)"


def format_days_ago(value: float) -> str:
    return "∞" if math.isinf(value) else str(int(value))


def render_table(
    results: Sequence[FilterAuditResult],
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
            format_last_seen(result.last_seen),
            format_days_ago(result.days_ago),
            str(result.match_count),
            format_labels(result.labels),
            result.query,
        ]
        for result in results
    ]

    return tabulate_fn(
        rows,
        headers=["Last seen", "Days ago", "Matches", "Labels", "Filter Query"],
        tablefmt="simple",
        stralign="left",
        disable_numparse=True,
    )


def describe_selection(
    *,
    threshold_days: int | None,
    max_matches: int | None,
) -> str:
    parts: list[str] = []
    if threshold_days is not None:
        parts.append(f"last seen > {threshold_days} days ago")
    if max_matches is not None:
        parts.append(f"matches < {max_matches}")
    return " and ".join(parts) if parts else "all filters"


def print_summary(
    summary: dict[str, int],
    *,
    threshold_days: int | None,
    max_matches: int | None,
    selected_count: int,
) -> None:
    print()
    print(f"Total filters: {summary['total_filters']}")
    print(f"Filters with no match at all: {summary['no_match']}")
    print(
        f"Filters matching selection ({describe_selection(threshold_days=threshold_days, max_matches=max_matches)}): "
        f"{selected_count}"
    )
    if summary["skipped_empty_criteria"]:
        print(
            "Filters skipped for empty criteria: "
            f"{summary['skipped_empty_criteria']}"
        )


def confirm_cleanup(
    count: int,
    *,
    input_fn: Callable[[str], str] = input,
) -> bool:
    response = input_fn(f"Delete all {count} filters? [y/N] ")
    return response.strip().lower() in {"y", "yes"}


def delete_filter(
    service: Any,
    filter_id: str,
    *,
    sleep_seconds: float = 0.2,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> None:
    request = service.users().settings().filters().delete(userId="me", id=filter_id)
    execute_request(request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)


def format_delete_error(result: FilterAuditResult, index: int, total: int, exc: Exception) -> str:
    message = (
        f"Failed {index}/{total}: {result.query} [{format_labels(result.labels)}] -> {exc}"
    )
    lowered = str(exc).lower()
    if "403" in lowered or "insufficient" in lowered or "permission" in lowered:
        message += " (check Gmail scope requirements for filters.delete)"
    return message


def delete_filters(
    service: Any,
    results: Sequence[FilterAuditResult],
    *,
    print_fn: Callable[[str], None] = print,
    error_fn: Callable[[str], None] | None = None,
    sleep_seconds: float = 0.2,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    if error_fn is None:
        error_fn = lambda message: print(message, file=sys.stderr)

    deleted = 0
    errors = 0
    total = len(results)

    for index, result in enumerate(results, start=1):
        try:
            delete_filter(
                service,
                result.filter_id,
                sleep_seconds=sleep_seconds,
                sleep_fn=sleep_fn,
            )
            deleted += 1
            print_fn(
                f"Deleted {index}/{total}: {result.query} [{format_labels(result.labels)}]"
            )
        except Exception as exc:
            errors += 1
            error_fn(format_delete_error(result, index, total, exc))

    return {"deleted": deleted, "errors": errors, "total": total}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit Gmail filters by finding the most recent message that matches "
            "each filter query."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=(
            "Only show filters older than this many days. If no selection flags are "
            "provided, the default stale threshold is 180 days."
        ),
    )
    parser.add_argument(
        "--matches",
        type=int,
        default=None,
        help="Only show filters whose exact match count is less than this number.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        dest="show_all",
        help="Show every filter, ignoring --days and --matches (audit mode only).",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete filters matching the active selection after a single y/N confirmation.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        progress_ui = RichAuditProgress() if sys.stderr.isatty() else None
        if progress_ui is None:
            service = build_gmail_service()
            results, summary = audit_filters(service)
        else:
            with progress_ui:
                progress_ui.update(
                    AuditProgressUpdate(
                        stage="setup",
                        message="Authenticating with Gmail",
                    )
                )
                service = build_gmail_service()
                progress_ui.update(
                    AuditProgressUpdate(
                        stage="setup",
                        message="Connected to Gmail",
                    )
                )
                results, summary = audit_filters(
                    service, progress_callback=progress_ui.update
                )
        threshold_days, max_matches = resolve_selection_criteria(
            threshold_days=args.days,
            max_matches=args.matches,
            show_all=args.show_all,
        )
        selected_results = select_results(
            results,
            threshold_days=threshold_days,
            max_matches=max_matches,
            show_all=False,
        )
        selection_description = describe_selection(
            threshold_days=threshold_days,
            max_matches=max_matches,
        )

        if args.cleanup:
            if selected_results:
                print(render_table(selected_results))
            else:
                print("No filters matched the selected cleanup criteria.")
            print()
            print(
                f"Found {len(selected_results)} filters matching cleanup criteria "
                f"({selection_description})"
            )
            if not selected_results:
                return 0
            if not confirm_cleanup(len(selected_results)):
                print("Deletion cancelled.")
                return 0
            delete_summary = delete_filters(service, selected_results)
            print()
            print(f"Deleted filters: {delete_summary['deleted']}")
            print(f"Delete errors: {delete_summary['errors']}")
        else:
            visible_results = select_results(
                results,
                threshold_days=threshold_days,
                max_matches=max_matches,
                show_all=args.show_all,
            )
            if visible_results:
                print(render_table(visible_results))
            else:
                print("No filters matched the selected criteria.")
            print_summary(
                summary,
                threshold_days=threshold_days,
                max_matches=max_matches,
                selected_count=len(selected_results),
            )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
