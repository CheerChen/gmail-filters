from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

GMAIL_SCOPES = (
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.settings.basic",
)


def token_looks_like_client_config(token_path: Path) -> bool:
    try:
        data = json.loads(token_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(data, dict) and any(key in data for key in ("installed", "web"))


def load_credentials(
    token_path: Path = Path("token.json"),
    credentials_path: Path = Path("credentials.json"),
    scopes: tuple[str, ...] | None = None,
) -> Any:
    effective_scopes = scopes if scopes is not None else GMAIL_SCOPES
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise RuntimeError(
            "Missing Google API dependencies. Install google-auth-oauthlib and "
            "google-api-python-client before running this script."
        ) from exc

    creds = None
    token_is_client_config = False
    if token_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), effective_scopes)
        except ValueError as exc:
            token_is_client_config = token_looks_like_client_config(token_path)
            if not token_is_client_config:
                raise RuntimeError(
                    f"{token_path} exists but is not a valid authorized-user token file."
                ) from exc

    if creds and creds.valid:
        granted = set(creds.scopes) if creds.scopes else set()
        if granted >= set(effective_scopes):
            return creds
        # Token valid but missing required scopes – fall through to re-auth
        creds = None

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow_credentials_path = credentials_path
        if not flow_credentials_path.exists() and token_is_client_config:
            flow_credentials_path = token_path

        if not flow_credentials_path.exists():
            raise FileNotFoundError(
                f"Missing OAuth client file: {credentials_path}"
            )
        flow = InstalledAppFlow.from_client_secrets_file(
            str(flow_credentials_path), effective_scopes
        )
        creds = flow.run_local_server(port=0)

    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_gmail_service(
    token_path: Path = Path("token.json"),
    credentials_path: Path = Path("credentials.json"),
    scopes: tuple[str, ...] | None = None,
) -> Any:
    try:
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Missing Google API dependencies. Install google-api-python-client "
            "before running this script."
        ) from exc

    creds = load_credentials(token_path=token_path, credentials_path=credentials_path, scopes=scopes)
    return build("gmail", "v1", credentials=creds)


def execute_request(
    request: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    response = request.execute()
    if sleep_seconds > 0:
        sleep_fn(sleep_seconds)
    return response


def fetch_label_map(
    service: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, str]:
    labels: dict[str, str] = {}
    request = service.users().labels().list(userId="me")
    response = execute_request(request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
    for label in response.get("labels", []):
        label_id = label.get("id")
        name = label.get("name")
        if label_id and name:
            labels[label_id] = name
    return labels


def fetch_filters(
    service: Any,
    *,
    sleep_seconds: float = 0.1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    request = service.users().settings().filters().list(userId="me")
    response = execute_request(request, sleep_seconds=sleep_seconds, sleep_fn=sleep_fn)
    return list(response.get("filter", []))


def extract_date_header(message_metadata: dict[str, Any]) -> str | None:
    payload = message_metadata.get("payload") or {}
    for header in payload.get("headers", []):
        if header.get("name", "").lower() == "date":
            value = header.get("value")
            if value:
                return value
    return None


def parse_date_header(value: str) -> datetime:
    parsed = parsedate_to_datetime(value)
    if parsed is None:
        raise ValueError(f"Unable to parse Date header: {value!r}")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def compute_days_ago(
    message_date: datetime,
    *,
    today: date | None = None,
) -> int:
    reference_day = today or datetime.now(timezone.utc).date()
    return (reference_day - message_date.date()).days
