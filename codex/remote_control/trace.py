from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

_TRACE_ENV_NAMES = ("PY_CODEX_RC_TRACE_FILE", "CODEX_RC_TRACE_FILE")
_SENSITIVE_KEY_PARTS = (
    "authorization",
    "cookie",
    "secret",
    "password",
    "api_key",
    "auth_token",
    "authtoken",
    "access_token",
    "refresh_token",
    "remote_control_token",
    "session_token",
    "session-token",
    "habitat-session",
    "presence_claim_token",
    "installation_id",
)


def append_event(event: dict[str, Any]) -> None:
    path = _trace_path()
    if path is None:
        return
    payload = {"runtime": "python", "ts_unix_ms": int(time.time() * 1000), **sanitize_value(event)}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            handle.write("\n")
    except OSError:
        return


def payload_json(raw: str) -> Any:
    try:
        return sanitize_value(json.loads(raw))
    except Exception:
        return {"raw": raw}


def sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if _is_sensitive_key(key_text):
                sanitized[key_text] = "<redacted>"
            else:
                sanitized[key_text] = sanitize_value(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_value(item) for item in value]
    return value


def redacted_header_value(name: str, value: str) -> str:
    lower = name.lower()
    if any(part in lower for part in ("authorization", "cookie", "habitat-session", "session-token", "token", "secret", "key")):
        return "<redacted>"
    if lower in {"chatgpt-account-id", "x-codex-installation-id", "x-codex-server-id", "x-codex-subscribe-cursor"}:
        return redact_middle(value)
    return value


def headers_dict(headers: dict[str, str] | list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(headers, dict):
        iterable = headers.items()
    else:
        pairs: list[tuple[str, str]] = []
        for header in headers:
            if ":" not in header:
                continue
            name, value = header.split(":", 1)
            pairs.append((name.strip(), value.strip()))
        iterable = pairs
    for name, value in iterable:
        out[str(name)] = redacted_header_value(str(name), str(value))
    return out


def redact_middle(value: str) -> str:
    if len(value) <= 12:
        return "<redacted>"
    return f"{value[:6]}...{value[-4:]}"


def _trace_path() -> Path | None:
    for name in _TRACE_ENV_NAMES:
        value = os.environ.get(name)
        if value:
            return Path(value).expanduser()
    return None


def _is_sensitive_key(key: str) -> bool:
    lower = key.lower().replace("-", "_")
    if (
        "token_usage" in lower
        or "tokenusage" in lower
        or "token_count" in lower
        or lower.endswith("_tokens")
        or lower.endswith("tokens")
        or lower.endswith("_token_limit")
        or lower.endswith("token_limit")
    ):
        return False
    return any(part in lower for part in _SENSITIVE_KEY_PARTS)
