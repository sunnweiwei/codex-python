from __future__ import annotations

import base64
import json
import threading
import urllib.error
import urllib.request
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

from ..auth import load_auth_snapshot, refresh_chatgpt_auth
from .constants import (
    REMOTE_CONTROL_ACCOUNT_ID_HEADER,
    REMOTE_CONTROL_CONNECT_TIMEOUT_SECONDS,
    REMOTE_CONTROL_INSTALLATION_ID_HEADER,
    REMOTE_CONTROL_PROTOCOL_VERSION,
    REMOTE_CONTROL_STATE_FILE,
    REMOTE_CONTROL_SUBSCRIBE_CURSOR_HEADER,
    PYTHON_REMOTE_CONTROL_VERSION,
)
from .types import (
    EnrollRemoteServerRequest,
    RemoteControlAuth,
    RemoteControlConfig,
    RemoteControlEnrollment,
    RemoteControlError,
    RemoteControlTarget,
    RemoteControlUnavailable,
)
from .utils import (
    _codex_user_agent,
    _ensure_trailing_path_slash,
    _invalid_remote_control_url_message,
    _is_allowed_chatgpt_host,
    _is_localhost,
    _remote_control_client_identity,
    _remote_control_enroll_arch,
    _remote_control_enroll_os,
)
from . import trace

def normalize_remote_control_url(remote_control_url: str) -> RemoteControlTarget:
    parsed = urlparse(remote_control_url)
    if not parsed.scheme or not parsed.netloc:
        raise RemoteControlError(
            f"invalid remote control URL `{remote_control_url}`; expected absolute URL"
        )
    normalized = _ensure_trailing_path_slash(parsed)
    enroll_url = urljoin(urlunparse(normalized), "wham/remote/control/server/enroll")
    websocket = urlparse(urljoin(urlunparse(normalized), "wham/remote/control/server"))
    enroll_parsed = urlparse(enroll_url)
    host = enroll_parsed.hostname

    if enroll_parsed.scheme == "https" and (_is_localhost(host) or _is_allowed_chatgpt_host(host)):
        websocket = websocket._replace(scheme="wss")
    elif enroll_parsed.scheme == "http" and _is_localhost(host):
        websocket = websocket._replace(scheme="ws")
    else:
        raise RemoteControlError(_invalid_remote_control_url_message(remote_control_url))
    return RemoteControlTarget(websocket_url=urlunparse(websocket), enroll_url=enroll_url)

def build_enroll_request(
    *,
    name: str,
    installation_id: str,
    app_server_version: str,
) -> EnrollRemoteServerRequest:
    return EnrollRemoteServerRequest(
        name=name,
        os=_remote_control_enroll_os(),
        arch=_remote_control_enroll_arch(),
        app_server_version=app_server_version,
        installation_id=installation_id,
    )



class _RemoteControlPersistentState:
    def __init__(self, codex_home: Path):
        self.codex_home = codex_home
        self.path = codex_home / REMOTE_CONTROL_STATE_FILE
        self.installation_id_path = codex_home / "installation_id"
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def installation_id(self) -> str:
        with self._lock:
            try:
                installation_id = self.installation_id_path.read_text(encoding="utf-8").strip()
            except OSError:
                installation_id = ""
            if not installation_id:
                installation_id = str(uuid.uuid4())
                tmp = self.installation_id_path.with_suffix(".tmp")
                tmp.write_text(installation_id + "\n", encoding="utf-8")
                tmp.replace(self.installation_id_path)
            return installation_id

    def enrollment(
        self,
        websocket_url: str,
        account_id: str,
        app_server_client_name: str | None,
    ) -> RemoteControlEnrollment | None:
        key = _enrollment_key(websocket_url, account_id, app_server_client_name)
        raw = self._load().get("enrollments", {}).get(key)
        if not isinstance(raw, dict):
            return None
        try:
            return RemoteControlEnrollment(
                account_id=str(raw["account_id"]),
                environment_id=str(raw["environment_id"]),
                server_id=str(raw["server_id"]),
                server_name=str(raw["server_name"]),
                app_server_version=(
                    str(raw["app_server_version"]) if isinstance(raw.get("app_server_version"), str) else None
                ),
                enroll_os=str(raw["enroll_os"]) if isinstance(raw.get("enroll_os"), str) else None,
                enroll_arch=str(raw["enroll_arch"]) if isinstance(raw.get("enroll_arch"), str) else None,
            )
        except Exception:
            return None

    def prune_enrollments(
        self,
        websocket_url: str,
        account_id: str,
        app_server_client_name: str | None,
        *,
        server_name: str,
    ) -> int:
        keep_key = _enrollment_key(websocket_url, account_id, app_server_client_name)
        removed = 0
        with self._lock:
            state = self._load()
            enrollments = state.get("enrollments")
            if not isinstance(enrollments, dict):
                return 0
            prefix = f"{websocket_url}\n{account_id}\n"
            for key in list(enrollments):
                raw = enrollments.get(key)
                raw_name = raw.get("server_name") if isinstance(raw, dict) else None
                if key.startswith(prefix) and (key != keep_key or raw_name != server_name):
                    enrollments.pop(key, None)
                    removed += 1
            if removed:
                self._save(state)
        return removed

    def save_enrollment(
        self,
        websocket_url: str,
        account_id: str,
        app_server_client_name: str | None,
        enrollment: RemoteControlEnrollment,
    ) -> None:
        with self._lock:
            state = self._load()
            enrollments = state.setdefault("enrollments", {})
            if isinstance(enrollments, dict):
                enrollments[_enrollment_key(websocket_url, account_id, app_server_client_name)] = asdict(enrollment)
            self._save(state)

    def _load(self) -> dict[str, Any]:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save(self, state: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        tmp.replace(self.path)


def enroll_remote_control_server(
    target: RemoteControlTarget,
    auth: RemoteControlAuth,
    *,
    installation_id: str,
    server_name: str,
    app_server_client_name: str | None = None,
    app_server_client_version: str | None = None,
    allow_desktop_compat_identity: bool = False,
    user_agent_override: str | None = None,
) -> RemoteControlEnrollment:
    request_payload = build_enroll_request(
        name=server_name,
        installation_id=installation_id,
        app_server_version=PYTHON_REMOTE_CONTROL_VERSION,
    )
    body = json.dumps(asdict(request_payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    originator, user_agent_suffix = _remote_control_client_identity(
        app_server_client_name,
        app_server_client_version,
        allow_desktop_compat_identity=allow_desktop_compat_identity,
    )
    request = urllib.request.Request(
        target.enroll_url,
        data=body,
        method="POST",
        headers={
            "originator": originator,
            "User-Agent": _codex_user_agent(
                originator,
                user_agent_suffix if not user_agent_override else None,
                override=user_agent_override,
            ),
            **_remote_auth_headers(auth),
            "Accept": "application/json",
            "Content-Type": "application/json",
            REMOTE_CONTROL_ACCOUNT_ID_HEADER: auth.account_id,
            REMOTE_CONTROL_INSTALLATION_ID_HEADER: installation_id,
        },
    )
    trace.append_event(
        {
            "event": "remote_control_enroll_request",
            "url": target.enroll_url,
            "body": asdict(request_payload),
            "headers": trace.headers_dict(dict(request.header_items())),
        }
    )
    try:
        with urllib.request.urlopen(request, timeout=REMOTE_CONTROL_CONNECT_TIMEOUT_SECONDS) as response:
            status = int(getattr(response, "status", 200))
            response_body = response.read()
            response_headers = dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RemoteControlError(
            f"remote control server enrollment failed at `{target.enroll_url}`: HTTP {exc.code}, body: {detail or '<empty>'}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RemoteControlError(f"failed to enroll remote control server at `{target.enroll_url}`: {exc.reason}") from exc

    if status < 200 or status >= 300:
        detail = response_body.decode("utf-8", errors="replace")
        raise RemoteControlError(
            f"remote control server enrollment failed at `{target.enroll_url}`: HTTP {status}, body: {detail or '<empty>'}"
        )
    trace.append_event(
        {
            "event": "remote_control_enroll_response",
            "url": target.enroll_url,
            "status": status,
            "headers": trace.headers_dict(response_headers),
            "body": trace.payload_json(response_body.decode("utf-8", errors="replace")),
        }
    )
    try:
        payload = json.loads(response_body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        preview = response_body.decode("utf-8", errors="replace")[:4096]
        raise RemoteControlError(
            f"failed to parse remote control enrollment response from `{target.enroll_url}`: {preview}"
        ) from exc
    server_id = payload.get("server_id") if isinstance(payload, dict) else None
    environment_id = payload.get("environment_id") if isinstance(payload, dict) else None
    if not isinstance(server_id, str) or not isinstance(environment_id, str):
        raise RemoteControlError("remote control enrollment response missing server_id or environment_id")
    return RemoteControlEnrollment(
        account_id=auth.account_id,
        environment_id=environment_id,
        server_id=server_id,
        server_name=server_name,
        app_server_version=PYTHON_REMOTE_CONTROL_VERSION,
        enroll_os=request_payload.os,
        enroll_arch=request_payload.arch,
    )


def _load_remote_control_auth(config: RemoteControlConfig) -> RemoteControlAuth:
    try:
        snapshot = load_auth_snapshot(config.auth_codex_home, mode="chatgpt")
        if snapshot is not None and snapshot.needs_proactive_refresh():
            snapshot = refresh_chatgpt_auth(snapshot)
    except Exception as exc:
        raise RemoteControlUnavailable(
            "remote control requires ChatGPT login credentials; run `python -m agents.codex login` first"
        ) from exc
    if snapshot is None or not snapshot.access_token or not snapshot.account_id:
        raise RemoteControlUnavailable(
            "remote control requires ChatGPT login credentials; API-key-only auth cannot be used for phone relay"
        )
    return RemoteControlAuth(
        access_token=snapshot.access_token,
        account_id=snapshot.account_id,
        is_fedramp_account=snapshot.is_fedramp_account,
    )


def _remote_auth_headers(auth: RemoteControlAuth) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {auth.access_token}",
    }
    if auth.is_fedramp_account:
        headers["X-OpenAI-Fedramp"] = "true"
    return headers


def _websocket_headers(
    auth: RemoteControlAuth,
    enrollment: RemoteControlEnrollment,
    *,
    installation_id: str,
    subscribe_cursor: str | None,
) -> list[str]:
    headers = {
        **_remote_auth_headers(auth),
        "x-codex-server-id": enrollment.server_id,
        "x-codex-name": base64.b64encode(enrollment.server_name.encode("utf-8")).decode("ascii"),
        "x-codex-protocol-version": REMOTE_CONTROL_PROTOCOL_VERSION,
        REMOTE_CONTROL_ACCOUNT_ID_HEADER: auth.account_id,
        REMOTE_CONTROL_INSTALLATION_ID_HEADER: installation_id,
    }
    if subscribe_cursor:
        headers[REMOTE_CONTROL_SUBSCRIBE_CURSOR_HEADER] = subscribe_cursor
    return [f"{key}: {value}" for key, value in headers.items()]


def _enrollment_key(websocket_url: str, account_id: str, app_server_client_name: str | None) -> str:
    suffix = app_server_client_name or ""
    return f"{websocket_url}\n{account_id}\n{suffix}"
