from __future__ import annotations

import json
import os
import platform
import base64
import queue
import shlex
import signal
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

from dataclasses import asdict, dataclass, field
from enum import Enum
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Literal
from urllib.parse import ParseResult, urljoin, urlparse, urlunparse

from .auth import (
    ORIGINATOR,
    auth_json_path,
    chatgpt_backend_base_url,
    complete_device_code_login,
    login_with_api_key,
    load_auth_snapshot,
    request_device_code,
    refresh_chatgpt_auth,
    run_browser_login,
    _write_auth_json,
)
from .core import CodexSession, SteerInputError
from .state import load_rollout_records, reconstruct_history_from_rollout
from .types import CodexConfig


REMOTE_CONTROL_PROTOCOL_VERSION = "3"
REMOTE_CONTROL_ACCOUNT_ID_HEADER = "chatgpt-account-id"
REMOTE_CONTROL_INSTALLATION_ID_HEADER = "x-codex-installation-id"
REMOTE_CONTROL_SUBSCRIBE_CURSOR_HEADER = "x-codex-subscribe-cursor"
DEFAULT_REMOTE_CONTROL_BASE_URL = "https://chatgpt.com/backend-api"
REMOTE_CONTROL_COMPAT_VERSION = os.environ.get("PY_CODEX_REMOTE_CONTROL_VERSION", "0.133.0")
PYTHON_REMOTE_CONTROL_VERSION = REMOTE_CONTROL_COMPAT_VERSION
REMOTE_CONTROL_CONNECT_TIMEOUT_SECONDS = 30
REMOTE_CONTROL_RECONNECT_SECONDS = 5
REMOTE_CONTROL_PID_FILE = "remote-control.pid"
REMOTE_CONTROL_STATE_FILE = "remote-control.json"
REMOTE_CONTROL_SEGMENT_TARGET_BYTES = 100 * 1024
REMOTE_CONTROL_SEGMENT_MAX_BYTES = 150 * 1024
REMOTE_CONTROL_REASSEMBLED_MAX_BYTES = 100 * 1024 * 1024
REMOTE_CONTROL_SEGMENT_COUNT_MAX = 1024
REMOTE_CONTROL_SEGMENT_ASSEMBLY_MAX_COUNT = 128
DEFAULT_REMOTE_PROCESS_OUTPUT_BYTES_CAP = 1024 * 1024

RemoteControlConnectionStatus = Literal["disabled", "connecting", "connected", "errored"]
RemoteControlMode = Literal["foreground", "daemon"]


def _env_truthy(name: str) -> bool:
    value = os.environ.get(name)
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _default_remote_control_server_name() -> str:
    configured = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
    if configured:
        return configured
    hostname = socket.gethostname() or "codex-python"
    return hostname


class RemoteControlError(RuntimeError):
    pass


class RemoteControlUnavailable(RemoteControlError):
    pass


@dataclass(frozen=True)
class RemoteControlTarget:
    websocket_url: str
    enroll_url: str


@dataclass(frozen=True)
class EnrollRemoteServerRequest:
    name: str
    os: str
    arch: str
    app_server_version: str
    installation_id: str


@dataclass(frozen=True)
class RemoteControlReadyStatus:
    status: RemoteControlConnectionStatus
    server_name: str
    environment_id: str | None = None
    timed_out: bool = False


@dataclass(frozen=True)
class RemoteControlStartJsonOutput:
    mode: RemoteControlMode
    status: RemoteControlConnectionStatus
    server_name: str
    environment_id: str | None
    timed_out: bool
    daemon: dict[str, Any] | None = None

    def to_json(self) -> str:
        payload = {
            "mode": self.mode,
            "status": self.status,
            "serverName": self.server_name,
            "environmentId": self.environment_id,
            "timedOut": self.timed_out,
        }
        if self.daemon is not None:
            payload["daemon"] = self.daemon
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class RemoteControlEnrollment:
    account_id: str
    environment_id: str
    server_id: str
    server_name: str
    app_server_version: str | None = None
    enroll_os: str | None = None
    enroll_arch: str | None = None


@dataclass(frozen=True)
class RemoteControlAuth:
    access_token: str
    account_id: str
    is_fedramp_account: bool = False


@dataclass(frozen=True)
class RemoteControlConfig:
    codex_home: Path
    auth_codex_home: Path | None = None
    cwd: Path = field(default_factory=Path.cwd)
    model: str | None = None
    remote_control_url: str = DEFAULT_REMOTE_CONTROL_BASE_URL
    server_name: str = field(default_factory=_default_remote_control_server_name)
    app_server_client_name: str | None = field(
        default_factory=lambda: os.environ.get("PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_NAME") or None
    )
    app_server_client_version: str | None = field(
        default_factory=lambda: os.environ.get("PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_VERSION") or None
    )
    allow_desktop_compat_identity: bool = field(
        default_factory=lambda: _env_truthy("PY_CODEX_REMOTE_CONTROL_ALLOW_DESKTOP_COMPAT")
    )
    json_output: bool = False
    foreground: bool = True

    @classmethod
    def from_codex_config(
        cls,
        config: CodexConfig,
        *,
        json_output: bool = False,
        foreground: bool = True,
    ) -> "RemoteControlConfig":
        return cls(
            codex_home=config.resolved_codex_home(),
            auth_codex_home=config.resolved_auth_codex_home(),
            cwd=config.resolved_cwd(),
            model=config.model,
            remote_control_url=chatgpt_backend_base_url(config.chatgpt_base_url),
            json_output=json_output,
            foreground=foreground,
        )


class RemoteClientEvent(str, Enum):
    CLIENT_MESSAGE = "client_message"
    CLIENT_MESSAGE_CHUNK = "client_message_chunk"
    ACK = "ack"
    PING = "ping"
    CLIENT_CLOSED = "client_closed"


class RemoteServerEvent(str, Enum):
    SERVER_MESSAGE = "server_message"
    SERVER_MESSAGE_CHUNK = "server_message_chunk"
    ACK = "ack"
    PONG = "pong"


@dataclass
class _ClientSegmentAssembly:
    stream_id: str
    seq_id: int
    segment_count: int
    message_size_bytes: int
    raw: bytearray = field(default_factory=bytearray)
    next_segment_id: int = 0
    last_chunk_seen_at: float = field(default_factory=time.monotonic)


class _ClientSegmentReassembler:
    def __init__(self) -> None:
        self._assemblies: dict[str, _ClientSegmentAssembly] = {}

    def observe(self, envelope: "ClientEnvelope") -> "ClientEnvelope | None":
        if envelope.event.get("type") != RemoteClientEvent.CLIENT_MESSAGE_CHUNK.value:
            return envelope
        segment_id = _optional_int(envelope.event.get("segment_id"))
        segment_count = _optional_int(envelope.event.get("segment_count"))
        message_size_bytes = _optional_int(envelope.event.get("message_size_bytes"))
        chunk_base64 = _optional_string(envelope.event.get("message_chunk_base64"))
        if (
            envelope.seq_id is None
            or envelope.stream_id is None
            or segment_id is None
            or segment_count is None
            or message_size_bytes is None
            or not chunk_base64
        ):
            return None
        if self.should_ignore_chunk(envelope.client_id, envelope.stream_id, envelope.seq_id, segment_id):
            return None
        if (
            segment_count <= 0
            or segment_count > REMOTE_CONTROL_SEGMENT_COUNT_MAX
            or segment_id >= segment_count
            or message_size_bytes <= 0
            or message_size_bytes > REMOTE_CONTROL_REASSEMBLED_MAX_BYTES
        ):
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None

        assembly = self._assemblies.get(envelope.client_id)
        if assembly is None or assembly.stream_id != envelope.stream_id:
            self._evict_assemblies_if_full()
            assembly = _ClientSegmentAssembly(
                stream_id=envelope.stream_id,
                seq_id=envelope.seq_id,
                segment_count=segment_count,
                message_size_bytes=message_size_bytes,
            )
            self._assemblies[envelope.client_id] = assembly
        elif (
            assembly.seq_id != envelope.seq_id
            or assembly.segment_count != segment_count
            or assembly.message_size_bytes != message_size_bytes
        ):
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None

        if segment_id < assembly.next_segment_id:
            return None
        if segment_id != assembly.next_segment_id:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        try:
            chunk = base64.b64decode(chunk_base64.encode("ascii"), validate=True)
        except Exception:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        if len(assembly.raw) + len(chunk) > message_size_bytes:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        assembly.raw.extend(chunk)
        assembly.next_segment_id += 1
        assembly.last_chunk_seen_at = time.monotonic()
        if assembly.next_segment_id < segment_count:
            return None
        if len(assembly.raw) != message_size_bytes:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        try:
            message = json.loads(bytes(assembly.raw).decode("utf-8"))
        except Exception:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        self.invalidate_stream(envelope.client_id, envelope.stream_id)
        if not isinstance(message, dict):
            return None
        return ClientEnvelope(
            client_id=envelope.client_id,
            stream_id=envelope.stream_id,
            seq_id=envelope.seq_id,
            cursor=envelope.cursor,
            event={"type": RemoteClientEvent.CLIENT_MESSAGE.value, "message": message},
        )

    def should_ignore_chunk(self, client_id: str, stream_id: str, seq_id: int, segment_id: int) -> bool:
        assembly = self._assemblies.get(client_id)
        return bool(
            assembly
            and assembly.stream_id == stream_id
            and (seq_id < assembly.seq_id or (seq_id == assembly.seq_id and segment_id < assembly.next_segment_id))
        )

    def invalidate_stream(self, client_id: str, stream_id: str) -> None:
        assembly = self._assemblies.get(client_id)
        if assembly and assembly.stream_id == stream_id:
            self._assemblies.pop(client_id, None)

    def invalidate_client(self, client_id: str) -> None:
        self._assemblies.pop(client_id, None)

    def _evict_assemblies_if_full(self) -> None:
        while len(self._assemblies) >= REMOTE_CONTROL_SEGMENT_ASSEMBLY_MAX_COUNT:
            oldest = min(self._assemblies.items(), key=lambda item: item[1].last_chunk_seen_at, default=None)
            if oldest is None:
                return
            self._assemblies.pop(oldest[0], None)


class _OutboundBuffer:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._buffer_by_stream: dict[tuple[str, str], list[ServerEnvelope]] = {}

    def insert(self, envelope: "ServerEnvelope") -> None:
        with self._lock:
            self._buffer_by_stream.setdefault((envelope.client_id, envelope.stream_id), []).append(envelope)

    def ack(self, client_id: str, stream_id: str, acked_seq_id: int, acked_segment_id: int | None) -> None:
        acked_cursor = (acked_seq_id, acked_segment_id if acked_segment_id is not None else sys.maxsize)
        key = (client_id, stream_id)
        with self._lock:
            buffer = self._buffer_by_stream.get(key)
            if not buffer:
                return
            remaining = [
                envelope
                for envelope in buffer
                if (envelope.seq_id, _server_envelope_segment_id(envelope)) > acked_cursor
            ]
            if remaining:
                self._buffer_by_stream[key] = remaining
            else:
                self._buffer_by_stream.pop(key, None)

    def remove_stream(self, client_id: str, stream_id: str) -> None:
        with self._lock:
            self._buffer_by_stream.pop((client_id, stream_id), None)

    def remove_client(self, client_id: str) -> None:
        with self._lock:
            for key in list(self._buffer_by_stream):
                if key[0] == client_id:
                    self._buffer_by_stream.pop(key, None)

    def server_envelopes(self) -> list["ServerEnvelope"]:
        with self._lock:
            return [envelope for buffer in self._buffer_by_stream.values() for envelope in buffer]


@dataclass(frozen=True)
class ClientEnvelope:
    client_id: str
    event: dict[str, Any]
    stream_id: str | None = None
    seq_id: int | None = None
    cursor: str | None = None

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "ClientEnvelope":
        if not isinstance(payload, dict):
            raise RemoteControlError("remote-control client envelope must be a JSON object")
        client_id = payload.get("client_id")
        if not isinstance(client_id, str) or not client_id:
            raise RemoteControlError("remote-control client envelope is missing client_id")
        event_type = payload.get("type")
        if not isinstance(event_type, str):
            raise RemoteControlError("remote-control client envelope is missing type")
        event = {"type": event_type}
        for key, value in payload.items():
            if key not in {"client_id", "stream_id", "seq_id", "cursor", "type"}:
                event[key] = value
        return cls(
            client_id=client_id,
            stream_id=_optional_string(payload.get("stream_id")),
            seq_id=_optional_int(payload.get("seq_id")),
            cursor=_optional_string(payload.get("cursor")),
            event=event,
        )

    def to_wire(self) -> dict[str, Any]:
        event = dict(self.event)
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "type": event.pop("type"),
            **event,
        }
        if self.stream_id is not None:
            payload["stream_id"] = self.stream_id
        if self.seq_id is not None:
            payload["seq_id"] = self.seq_id
        if self.cursor is not None:
            payload["cursor"] = self.cursor
        return payload


@dataclass(frozen=True)
class ServerEnvelope:
    client_id: str
    stream_id: str
    seq_id: int
    event: dict[str, Any]

    def to_wire(self) -> dict[str, Any]:
        event = dict(self.event)
        return {
            "client_id": self.client_id,
            "stream_id": self.stream_id,
            "seq_id": self.seq_id,
            "type": event.pop("type"),
            **event,
        }


@dataclass
class _RemoteCommandProcess:
    process_id: str
    popen: subprocess.Popen[bytes]
    cwd: Path
    pty_master_fd: int | None = None
    stdin_enabled: bool = False
    stdout_chunks: list[bytes] = field(default_factory=list)
    stderr_chunks: list[bytes] = field(default_factory=list)
    stdout_cap_reached: bool = False
    stderr_cap_reached: bool = False
    reader_threads: list[threading.Thread] = field(default_factory=list)


class _DeferredResponse:
    pass


_DEFERRED_RESPONSE = _DeferredResponse()


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


def _remote_control_enroll_os() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform in {"win32", "cygwin", "msys"}:
        return "windows"
    if sys.platform.startswith("freebsd"):
        return "freebsd"
    if sys.platform.startswith("openbsd"):
        return "openbsd"
    if sys.platform.startswith("netbsd"):
        return "netbsd"
    return sys.platform


def _remote_control_enroll_arch() -> str:
    machine = (platform.machine() or platform.processor() or "unknown").lower()
    rust_arch_aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "arm64": "aarch64",
    }
    return rust_arch_aliases.get(machine, machine)


def _remote_control_os_display_and_version() -> tuple[str, str]:
    system = platform.system().lower()
    if system == "darwin":
        return "Mac OS", platform.mac_ver()[0] or platform.release()
    if system.startswith("win"):
        return "Windows", platform.release()
    if system:
        return system.capitalize(), platform.release()
    return sys.platform, platform.release()


def _codex_user_agent(originator: str = ORIGINATOR, suffix: str | None = None) -> str:
    os_display, os_version = _remote_control_os_display_and_version()
    arch = platform.machine() or platform.processor() or "unknown"
    value = f"{originator}/{PYTHON_REMOTE_CONTROL_VERSION} ({os_display} {os_version}; {arch}) unknown"
    suffix = (suffix or "").strip()
    if suffix:
        value = f"{value} ({suffix})"
    return value


def _remote_control_client_identity(
    app_server_client_name: str | None,
    app_server_client_version: str | None,
    *,
    allow_desktop_compat_identity: bool = False,
) -> tuple[str, str | None]:
    if (
        app_server_client_name
        and app_server_client_name.strip().lower() == "codex desktop"
        and not allow_desktop_compat_identity
    ):
        return ORIGINATOR, None
    if app_server_client_name:
        version = app_server_client_version or PYTHON_REMOTE_CONTROL_VERSION
        return app_server_client_name, f"{app_server_client_name}; {version}"
    return ORIGINATOR, None


def _effective_app_server_client_name(config: RemoteControlConfig) -> str | None:
    name = config.app_server_client_name
    if name and name.strip().lower() == "codex desktop" and not config.allow_desktop_compat_identity:
        return None
    return name


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


def remote_control_start_human_lines(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
) -> list[str]:
    return _remote_control_start_human_lines(status, mode=mode)


def remote_control_start_json_output(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
    daemon: dict[str, Any] | None = None,
) -> RemoteControlStartJsonOutput:
    _ensure_remote_control_startable(status)
    return RemoteControlStartJsonOutput(
        mode=mode,
        status=status.status,
        server_name=status.server_name,
        environment_id=status.environment_id,
        timed_out=status.timed_out,
        daemon=daemon,
    )


def remote_control_stop_human_message(status: str) -> str:
    if status == "stopped":
        return "Remote control stopped."
    if status == "notRunning":
        return "Remote control is not running."
    return f"Remote control stop completed with status {status}."


def remote_control_official_args(
    subcommand: str | None = None,
    *,
    json_output: bool = False,
) -> list[str]:
    args = ["remote-control"]
    if json_output:
        args.append("--json")
    if subcommand:
        if subcommand not in {"start", "stop"}:
            raise ValueError(f"unsupported remote-control subcommand `{subcommand}`")
        args.append(subcommand)
    return args


def run_native_remote_control(
    subcommand: str | None = None,
    *,
    json_output: bool = False,
    codex_config: CodexConfig | None = None,
) -> int:
    config = RemoteControlConfig.from_codex_config(
        codex_config or CodexConfig(),
        json_output=json_output,
        foreground=subcommand is None,
    )
    if subcommand == "stop":
        return _stop_remote_control(config)
    if subcommand == "start":
        return _start_remote_control_daemon(config)
    if subcommand is not None:
        raise ValueError(f"unsupported remote-control subcommand `{subcommand}`")
    service = RemoteControlService(config)
    return service.run_foreground()


class RemoteControlService:
    def __init__(self, config: RemoteControlConfig):
        self.config = config
        self.target = normalize_remote_control_url(config.remote_control_url)
        self.state = _RemoteControlPersistentState(config.codex_home)
        self.installation_id = (
            os.environ.get("PY_CODEX_REMOTE_CONTROL_INSTALLATION_ID")
            or self.state.installation_id()
        )
        self.status: RemoteControlConnectionStatus = "connecting"
        self.environment_id: str | None = None
        self._stop = threading.Event()
        self._ws: Any | None = None
        self._server = _RemoteAppServer(self)
        self._seq_lock = threading.Lock()
        self._next_seq_by_stream: dict[tuple[str, str], int] = {}
        self._segment_reassembler = _ClientSegmentReassembler()
        self._outbound_buffer = _OutboundBuffer()
        self._last_completed_client_chunk_seq_by_stream: dict[tuple[str, str | None], int] = {}
        self._client_streams: set[tuple[str, str]] = set()

    def run_foreground(self) -> int:
        self.config.codex_home.mkdir(parents=True, exist_ok=True)
        _write_pid_file(self.config)
        try:
            while not self._stop.is_set():
                try:
                    self._connect_once()
                except KeyboardInterrupt:
                    self._stop.set()
                    break
                except Exception as exc:
                    self.status = "errored"
                    if self.config.json_output:
                        print(
                            json.dumps(
                                {
                                    "mode": "foreground",
                                    "status": "errored",
                                    "serverName": self.config.server_name,
                                    "environmentId": self.environment_id,
                                    "error": str(exc),
                                },
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            flush=True,
                        )
                        return 1
                    print(f"Remote control connection failed: {exc}", file=sys.stderr, flush=True)
                    if not self._stop.wait(REMOTE_CONTROL_RECONNECT_SECONDS):
                        continue
            return 0
        finally:
            _clear_pid_file(self.config)

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _connect_once(self) -> None:
        auth = _load_remote_control_auth(self.config)
        app_server_client_name = _effective_app_server_client_name(self.config)
        enrollment = self.state.enrollment(
            self.target.websocket_url,
            auth.account_id,
            app_server_client_name,
        )
        if (
            enrollment is None
            or enrollment.server_name != self.config.server_name
            or enrollment.app_server_version != PYTHON_REMOTE_CONTROL_VERSION
            or enrollment.enroll_os != _remote_control_enroll_os()
            or enrollment.enroll_arch != _remote_control_enroll_arch()
        ):
            if enrollment is not None and enrollment.app_server_version != PYTHON_REMOTE_CONTROL_VERSION:
                _remote_log(
                    "enrollment_version_mismatch",
                    old_version=enrollment.app_server_version,
                    new_version=PYTHON_REMOTE_CONTROL_VERSION,
                )
            if enrollment is not None and (
                enrollment.enroll_os != _remote_control_enroll_os()
                or enrollment.enroll_arch != _remote_control_enroll_arch()
            ):
                _remote_log(
                    "enrollment_platform_mismatch",
                    old_os=enrollment.enroll_os,
                    new_os=_remote_control_enroll_os(),
                    old_arch=enrollment.enroll_arch,
                    new_arch=_remote_control_enroll_arch(),
                )
            enrollment = enroll_remote_control_server(
                self.target,
                auth,
                installation_id=self.installation_id,
                server_name=self.config.server_name,
                app_server_client_name=app_server_client_name,
                app_server_client_version=self.config.app_server_client_version,
                allow_desktop_compat_identity=self.config.allow_desktop_compat_identity,
            )
            self.state.save_enrollment(
                self.target.websocket_url,
                auth.account_id,
                app_server_client_name,
                enrollment,
            )
        self.environment_id = enrollment.environment_id
        self.status = "connecting"

        try:
            import websocket
        except Exception as exc:  # pragma: no cover - dependency is present in the workspace image.
            raise RemoteControlUnavailable(
                "Python remote control requires the `websocket-client` package"
            ) from exc

        headers = _websocket_headers(
            auth,
            enrollment,
            installation_id=self.installation_id,
            subscribe_cursor=self.state.subscribe_cursor(),
        )
        self._ws = websocket.WebSocketApp(
            self.target.websocket_url,
            header=headers,
            on_open=lambda ws: self._on_open(ws),
            on_message=lambda ws, message: self._on_message(ws, message),
            on_error=lambda ws, error: self._on_error(error),
            on_close=lambda ws, code, reason: self._on_close(code, reason),
        )
        self._ws.run_forever(
            ping_interval=30,
            ping_timeout=10,
            http_proxy_host=None,
            http_proxy_port=None,
        )

    def _on_open(self, ws: Any) -> None:
        self.status = "connected"
        _remote_log("websocket_open", environment_id=self.environment_id, server_name=self.config.server_name)
        for envelope in self._outbound_buffer.server_envelopes():
            try:
                self._send_wire_envelope(ws, envelope)
            except Exception:
                break
        ready = RemoteControlReadyStatus(
            status="connected",
            server_name=self.config.server_name,
            environment_id=self.environment_id,
            timed_out=False,
        )
        if self.config.json_output:
            print(remote_control_start_json_output(ready, mode="foreground").to_json(), flush=True)
        else:
            for line in remote_control_start_human_lines(ready, mode="foreground"):
                print(line, flush=True)

    def _on_error(self, error: Any) -> None:
        self.status = "errored"
        _remote_log("websocket_error", error=str(error) if error else "")
        if error and not self._stop.is_set():
            print(f"Remote control websocket error: {error}", file=sys.stderr, flush=True)

    def _on_close(self, code: Any, reason: Any) -> None:
        _remote_log("websocket_close", code=code, reason=reason)
        if not self._stop.is_set():
            self.status = "connecting"

    def _on_message(self, ws: Any, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
            envelope = ClientEnvelope.from_wire(payload)
        except Exception as exc:
            print(f"Dropping invalid remote-control message: {exc}", file=sys.stderr, flush=True)
            return
        if envelope.cursor:
            self.state.save_subscribe_cursor(envelope.cursor)
        stream_id = envelope.stream_id or self.state.legacy_stream_id(envelope.client_id)
        if self._should_drop_completed_client_chunk(envelope):
            return
        was_client_message_chunk = envelope.event.get("type") == RemoteClientEvent.CLIENT_MESSAGE_CHUNK.value
        envelope = self._segment_reassembler.observe(envelope)
        if envelope is None:
            return
        if (
            was_client_message_chunk
            and envelope.event.get("type") == RemoteClientEvent.CLIENT_MESSAGE.value
            and envelope.seq_id is not None
        ):
            self._remember_completed_client_message(envelope)
        event = envelope.event
        event_type = event.get("type")
        message = event.get("message")
        method = message.get("method") if isinstance(message, dict) else None
        _remote_log(
            "client_event",
            event_type=event_type,
            method=method,
            client_id=envelope.client_id,
            stream_id=stream_id,
            seq_id=envelope.seq_id,
        )
        if event_type == RemoteClientEvent.ACK.value:
            if envelope.seq_id is not None and envelope.stream_id is not None:
                self._outbound_buffer.ack(
                    envelope.client_id,
                    envelope.stream_id,
                    envelope.seq_id,
                    _optional_int(event.get("segment_id")),
                )
            return
        if event_type == RemoteClientEvent.PING.value:
            status = "active" if (envelope.client_id, stream_id) in self._client_streams else "unknown"
            self._send_event(
                ws,
                envelope.client_id,
                stream_id,
                {"type": RemoteServerEvent.PONG.value, "status": status},
            )
            return
        if event_type == RemoteClientEvent.CLIENT_CLOSED.value:
            self._close_client_transport(envelope.client_id, envelope.stream_id)
            self._server.close_client(envelope.client_id, stream_id)
            self._client_streams.discard((envelope.client_id, stream_id))
            return
        if event_type != RemoteClientEvent.CLIENT_MESSAGE.value:
            return
        if not isinstance(message, dict):
            return
        if message.get("method") == "initialize" and envelope.stream_id is None:
            self.state.save_legacy_stream_id(envelope.client_id, stream_id)
        if message.get("method") == "initialize":
            self._client_streams.add((envelope.client_id, stream_id))
        self._server.handle_message(ws, envelope.client_id, stream_id, message)

    def send_message(self, ws: Any, client_id: str, stream_id: str, message: dict[str, Any]) -> None:
        result = message.get("result") if isinstance(message.get("result"), dict) else {}
        _remote_log(
            "server_message",
            method=message.get("method"),
            response_id=message.get("id"),
            client_id=client_id,
            stream_id=stream_id,
            user_agent=result.get("userAgent") if isinstance(result, dict) else None,
        )
        self._send_event(
            ws,
            client_id,
            stream_id,
            {"type": RemoteServerEvent.SERVER_MESSAGE.value, "message": message},
        )

    def send_notification(self, ws: Any, client_id: str, stream_id: str, method: str, params: dict[str, Any]) -> None:
        self.send_message(ws, client_id, stream_id, {"method": method, "params": params})

    def _send_event(self, ws: Any, client_id: str, stream_id: str, event: dict[str, Any]) -> None:
        seq_id = self._next_seq_id(client_id, stream_id)
        envelope = ServerEnvelope(client_id=client_id, stream_id=stream_id, seq_id=seq_id, event=event)
        for outbound in _split_server_envelope_for_transport(envelope):
            self._outbound_buffer.insert(outbound)
            self._send_wire_envelope(ws, outbound)

    def _send_wire_envelope(self, ws: Any, envelope: ServerEnvelope) -> None:
        ws.send(json.dumps(envelope.to_wire(), ensure_ascii=False, separators=(",", ":")))

    def _next_seq_id(self, client_id: str, stream_id: str) -> int:
        key = (client_id, stream_id)
        with self._seq_lock:
            seq_id = self._next_seq_by_stream.get(key, 1)
            self._next_seq_by_stream[key] = seq_id + 1
            return seq_id

    def _should_drop_completed_client_chunk(self, envelope: ClientEnvelope) -> bool:
        if envelope.event.get("type") != RemoteClientEvent.CLIENT_MESSAGE_CHUNK.value:
            return False
        if envelope.seq_id is None:
            return False
        segment_id = _optional_int(envelope.event.get("segment_id"))
        if segment_id is None:
            return False
        key = (envelope.client_id, envelope.stream_id)
        completed_seq_id = self._last_completed_client_chunk_seq_by_stream.get(key)
        return completed_seq_id is not None and completed_seq_id >= envelope.seq_id

    def _remember_completed_client_message(self, envelope: ClientEnvelope) -> None:
        if envelope.seq_id is None:
            return
        self._last_completed_client_chunk_seq_by_stream[(envelope.client_id, envelope.stream_id)] = envelope.seq_id

    def _close_client_transport(self, client_id: str, stream_id: str | None) -> None:
        if stream_id is None:
            self._segment_reassembler.invalidate_client(client_id)
            self._outbound_buffer.remove_client(client_id)
            self._client_streams = {key for key in self._client_streams if key[0] != client_id}
            for key in list(self._last_completed_client_chunk_seq_by_stream):
                if key[0] == client_id:
                    self._last_completed_client_chunk_seq_by_stream.pop(key, None)
            return
        self._segment_reassembler.invalidate_stream(client_id, stream_id)
        self._outbound_buffer.remove_stream(client_id, stream_id)
        self._client_streams.discard((client_id, stream_id))
        self._last_completed_client_chunk_seq_by_stream.pop((client_id, stream_id), None)


class _RemoteAppServer:
    def __init__(self, service: RemoteControlService):
        self.service = service
        self._lock = threading.RLock()
        self._sessions: dict[str, CodexSession] = {}
        self._active_turn_clients: dict[str, tuple[Any, str, str]] = {}
        self._turn_threads: dict[str, threading.Thread] = {}
        self._thread_names: dict[str, str] = {}
        self._thread_git_info: dict[str, dict[str, str | None]] = {}
        self._fs_watches: dict[tuple[str, str], Path] = {}
        self._command_processes: dict[tuple[str, str], _RemoteCommandProcess] = {}
        self._process_processes: dict[tuple[str, str], _RemoteCommandProcess] = {}
        self._pending_server_requests: dict[Any, queue.Queue[dict[str, Any]]] = {}
        self._next_server_request_id = 1

    def close_client(self, client_id: str, stream_id: str) -> None:
        with self._lock:
            for key in list(self._fs_watches):
                if key[0] == client_id:
                    self._fs_watches.pop(key, None)
            processes = [
                self._command_processes.pop(key)
                for key in list(self._command_processes)
                if key[0] == client_id
            ]
            processes.extend(
                self._process_processes.pop(key)
                for key in list(self._process_processes)
                if key[0] == client_id
            )
        for process in processes:
            _terminate_remote_process(process)

    def handle_message(self, ws: Any, client_id: str, stream_id: str, message: dict[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") if isinstance(message.get("params"), dict) else {}
        if not isinstance(method, str) and request_id is not None:
            self._handle_server_request_response(request_id, message)
            return
        if not isinstance(method, str):
            return
        try:
            result = self._dispatch(ws, client_id, stream_id, method, params, request_id=request_id)
        except Exception as exc:
            _remote_log("dispatch_error", method=method, request_id=request_id, error=str(exc))
            if request_id is not None:
                self.service.send_message(ws, client_id, stream_id, _jsonrpc_error(request_id, str(exc)))
            return
        if isinstance(result, _DeferredResponse):
            return
        if request_id is not None:
            self.service.send_message(ws, client_id, stream_id, {"id": request_id, "result": result})
            if method == "initialize":
                self.service.send_notification(
                    ws,
                    client_id,
                    stream_id,
                    "remoteControl/status/changed",
                    _remote_control_status_payload(self.service),
                )

    def _dispatch(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        method: str,
        params: dict[str, Any],
        *,
        request_id: Any = None,
    ) -> dict[str, Any]:
        if method == "initialize":
            return _initialize_response(self.service.config, params)
        if method == "initialized":
            return {}
        if method == "remoteControl/status/read":
            return _remote_control_status_payload(self.service)
        if method == "remoteControl/enable":
            return _remote_control_status_payload(self.service)
        if method == "remoteControl/disable":
            self.service.stop()
            return _remote_control_status_payload(self.service, status="disabled")
        if method == "config/read":
            return _config_read_response(self.service.config)
        if method == "model/list":
            return _model_list_response(self.service.config)
        if method == "collaborationMode/list":
            return _collaboration_mode_list_response()
        if method == "modelProvider/capabilities/read":
            return {"namespaceTools": True, "imageGeneration": False, "webSearch": True}
        if method == "permissionProfile/list":
            return {
                "data": [
                    {"id": "read-only", "description": "Read files without writing."},
                    {"id": "workspace-write", "description": "Read files and write inside the workspace."},
                    {"id": "danger-full-access", "description": "Run without filesystem sandboxing."},
                ],
                "nextCursor": None,
            }
        if method == "experimentalFeature/list":
            return {"data": [], "nextCursor": None}
        if method in {"skills/list", "hooks/list"}:
            return {"data": []}
        if method == "app/list":
            return {"data": [], "nextCursor": None}
        if method == "plugin/list":
            return {"marketplaces": [], "marketplaceLoadErrors": [], "featuredPluginIds": []}
        if method == "plugin/installed":
            return {"marketplaces": [], "marketplaceLoadErrors": []}
        if method == "plugin/read":
            return {"plugin": _empty_plugin_detail(params, self.service.config)}
        if method == "plugin/install":
            return {"authPolicy": "ON_INSTALL", "appsNeedingAuth": []}
        if method == "plugin/uninstall":
            return {}
        if method in {"marketplace/add", "marketplace/remove", "marketplace/upgrade"}:
            return _marketplace_empty_response(method, params, self.service.config)
        if method == "plugin/share/list":
            return {"data": []}
        if method == "plugin/skill/read":
            return {"contents": None}
        if method in {"plugin/share/save", "plugin/share/updateTargets", "plugin/share/checkout", "plugin/share/delete"}:
            return _plugin_share_empty_response(method, params, self.service.config)
        if method == "skills/config/write":
            return {"effectiveEnabled": bool(params.get("enabled"))}
        if method == "mcpServerStatus/list":
            return {"data": [], "nextCursor": None}
        if method == "mcpServer/oauth/login":
            raise RemoteControlError("MCP OAuth login is unavailable because no MCP servers are configured")
        if method == "config/mcpServer/reload":
            return {}
        if method == "mcpServer/resource/read":
            return {"contents": []}
        if method == "mcpServer/tool/call":
            return {"content": [{"type": "text", "text": "MCP servers are not configured in this Python remote-control runtime."}], "isError": True}
        if method == "experimentalFeature/enablement/set":
            enablement = params.get("enablement")
            return {"enablement": enablement if isinstance(enablement, dict) else {}}
        if method in {"externalAgentConfig/detect"}:
            return {"items": []}
        if method in {"externalAgentConfig/import"}:
            return {}
        if method == "windowsSandbox/setupStart":
            return {"started": False}
        if method == "configRequirements/read":
            return {"requirements": None}
        if method == "windowsSandbox/readiness":
            return {}
        if method == "account/read":
            return _account_read_response(self.service.config)
        if method == "account/login/start":
            return self._account_login_start(ws, client_id, stream_id, params)
        if method == "account/login/cancel":
            return {"status": "notFound"}
        if method == "account/logout":
            _account_logout(self.service.config)
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "account/updated",
                {"authMode": None, "planType": None},
            )
            return {}
        if method == "account/rateLimits/read":
            return _account_rate_limits_response(self.service.config)
        if method == "account/sendAddCreditsNudgeEmail":
            return {"status": "cooldown_active"}
        if method == "feedback/upload":
            return {"threadId": str(params.get("threadId") or "")}
        if method == "getAuthStatus":
            return _auth_status_response(self.service.config, include_token=bool(params.get("includeToken")))
        if method in {"config/value/write", "config/batchWrite"}:
            return {}
        if method == "thread/list":
            return {"data": self._thread_list(params), "nextCursor": None, "backwardsCursor": None}
        if method == "thread/loaded/list":
            return self._thread_loaded_list(params)
        if method == "thread/start":
            session = self._create_session(params)
            payload = self._thread_start_response(session)
            self.service.send_notification(ws, client_id, stream_id, "thread/started", {"thread": payload["thread"]})
            return payload
        if method == "thread/read":
            session = self._session_from_params(params)
            return {"thread": self._thread_payload(session, include_turns=bool(params.get("includeTurns", True)))}
        if method == "thread/resume":
            session = self._resume_session(params)
            return self._thread_start_response(session, include_turns=True)
        if method == "thread/fork":
            session = self._fork_session(params)
            payload = self._thread_start_response(session, include_turns=True)
            self.service.send_notification(ws, client_id, stream_id, "thread/started", {"thread": payload["thread"]})
            return payload
        if method == "thread/name/set":
            session = self._session_by_id(str(params.get("threadId") or ""))
            name = str(params.get("name") or "").strip()
            if not name:
                raise RemoteControlError("thread name must not be empty")
            with self._lock:
                self._thread_names[session.state.thread_id] = name
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "thread/name/updated",
                {"threadId": session.state.thread_id, "threadName": name},
            )
            return {}
        if method == "thread/metadata/update":
            session = self._session_by_id(str(params.get("threadId") or ""))
            git_info = params.get("gitInfo")
            if isinstance(git_info, dict):
                with self._lock:
                    current = dict(self._thread_git_info.get(session.state.thread_id) or {})
                    for source_key, target_key in (("sha", "sha"), ("branch", "branch"), ("originUrl", "originUrl")):
                        if source_key in git_info:
                            value = git_info.get(source_key)
                            current[target_key] = value if isinstance(value, str) and value else None
                    self._thread_git_info[session.state.thread_id] = current
            return {"thread": self._thread_payload(session, include_turns=False)}
        if method == "thread/inject_items":
            session = self._session_by_id(str(params.get("threadId") or ""))
            items = params.get("items")
            if not isinstance(items, list):
                raise RemoteControlError("thread/inject_items requires items")
            self._inject_thread_items(session, items)
            return {}
        if method == "thread/compact/start":
            session = self._session_by_id(str(params.get("threadId") or ""))
            threading.Thread(
                target=self._compact_thread,
                args=(ws, client_id, stream_id, session),
                daemon=True,
            ).start()
            return {}
        if method == "thread/rollback":
            session = self._session_by_id(str(params.get("threadId") or ""))
            num_turns = _optional_int(params.get("numTurns")) or 0
            self._rollback_thread(session, num_turns)
            return {"thread": self._thread_payload(session, include_turns=True)}
        if method == "thread/shellCommand":
            session = self._session_by_id(str(params.get("threadId") or ""))
            command = str(params.get("command") or "").strip()
            if not command:
                raise RemoteControlError("command must not be empty")
            threading.Thread(
                target=self._run_thread_shell_command,
                args=(ws, client_id, stream_id, session, command),
                daemon=True,
            ).start()
            return {}
        if method == "thread/approveGuardianDeniedAction":
            return {}
        if method == "thread/goal/get":
            session = self._session_by_id(str(params.get("threadId") or params.get("thread_id") or ""))
            goal = session.goals.get_goal()
            return {"goal": goal.to_protocol() if goal is not None else None}
        if method == "thread/goal/set":
            session = self._session_by_id(str(params.get("threadId") or params.get("thread_id") or ""))
            goal_kwargs: dict[str, Any] = {
                "objective": params.get("objective"),
                "status": _goal_status_from_param(params.get("status")),
            }
            if "tokenBudget" in params:
                goal_kwargs["token_budget"] = params.get("tokenBudget")
            elif "token_budget" in params:
                goal_kwargs["token_budget"] = params.get("token_budget")
            goal, _events = session.goals.set_goal_external(**goal_kwargs)
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "thread/goal/updated",
                {"goal": goal.to_protocol()},
            )
            return {"goal": goal.to_protocol()}
        if method == "thread/goal/clear":
            session = self._session_by_id(str(params.get("threadId") or params.get("thread_id") or ""))
            cleared, _events = session.goals.clear_goal_external()
            if cleared:
                self.service.send_notification(
                    ws,
                    client_id,
                    stream_id,
                    "thread/goal/cleared",
                    {"threadId": session.state.thread_id},
                )
            return {"cleared": cleared}
        if method == "turn/start":
            session = self._session_by_id(str(params.get("threadId") or ""))
            prompt = _input_text(params.get("input"))
            if not prompt:
                raise RemoteControlError("turn/start input is empty")
            started_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=1)
            thread = threading.Thread(
                target=self._run_turn,
                args=(ws, client_id, stream_id, session, prompt, started_queue),
                daemon=True,
            )
            with self._lock:
                self._active_turn_clients[session.state.thread_id] = (ws, client_id, stream_id)
                self._turn_threads[session.state.thread_id] = thread
            thread.start()
            try:
                turn = started_queue.get(timeout=5) or _turn_payload(session.state.turn_id, status="inProgress")
            except queue.Empty:
                turn = _turn_payload(session.state.turn_id, status="inProgress")
            return {"turn": turn}
        if method == "turn/steer":
            session = self._session_by_id(str(params.get("threadId") or ""))
            turn_id = session.steer_input(
                _input_text(params.get("input")),
                expected_turn_id=str(params.get("expectedTurnId") or ""),
            )
            return {"turnId": turn_id}
        if method == "turn/interrupt":
            session = self._session_by_id(str(params.get("threadId") or ""))
            session.interrupt()
            return {}
        if method == "review/start":
            session = self._session_by_id(str(params.get("threadId") or ""))
            turn = _turn_payload(f"review_{uuid.uuid4().hex}", status="completed", started_at=int(time.time()), completed_at=int(time.time()))
            return {"turn": turn, "reviewThreadId": session.state.thread_id}
        if method == "fs/readFile":
            return _fs_read_file_response(params)
        if method == "fs/writeFile":
            _fs_write_file(params)
            return {}
        if method == "fs/createDirectory":
            _fs_create_directory(params)
            return {}
        if method == "fs/getMetadata":
            return _fs_get_metadata_response(params)
        if method == "fs/readDirectory":
            return _fs_read_directory_response(params)
        if method == "fs/remove":
            _fs_remove(params)
            return {}
        if method == "fs/copy":
            _fs_copy(params)
            return {}
        if method == "fs/watch":
            path = _fs_path(params)
            watch_id = str(params.get("watchId") or "")
            if watch_id:
                with self._lock:
                    self._fs_watches[(client_id, watch_id)] = path
            return {"path": str(path)}
        if method == "fs/unwatch":
            watch_id = str(params.get("watchId") or "")
            if watch_id:
                with self._lock:
                    self._fs_watches.pop((client_id, watch_id), None)
            return {}
        if method == "fuzzyFileSearch":
            return _fuzzy_file_search_response(params)
        if method == "gitDiffToRemote":
            return _git_diff_to_remote_response(params)
        if method == "getConversationSummary":
            return _conversation_summary_response(params, self.service.config)
        if method == "command/exec":
            if request_id is not None and _command_exec_is_streaming(params):
                self._start_command_exec(ws, client_id, stream_id, request_id, params)
                return _DEFERRED_RESPONSE  # type: ignore[return-value]
            return _command_exec_response(params, self.service.config)
        if method == "command/exec/write":
            self._write_command_exec(client_id, params)
            return {}
        if method == "command/exec/terminate":
            self._terminate_command_exec(client_id, params)
            return {}
        if method == "command/exec/resize":
            self._resize_command_exec(client_id, params)
            return {}
        if method == "process/spawn":
            self._start_process_spawn(ws, client_id, stream_id, params)
            return {}
        if method == "process/writeStdin":
            self._write_process_spawn(client_id, params)
            return {}
        if method == "process/kill":
            self._kill_process_spawn(client_id, params)
            return {}
        if method == "process/resizePty":
            self._resize_process_spawn(client_id, params)
            return {}
        if method in {"thread/unsubscribe", "thread/archive", "thread/unarchive"}:
            return {}
        raise RemoteControlError(f"method `{method}` is not implemented in Python remote control yet")

    def _run_turn(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        session: CodexSession,
        prompt: str,
        started_queue: queue.Queue[dict[str, Any] | None] | None = None,
    ) -> None:
        started_at = int(time.time())
        current_agent_item_id: str | None = None
        announced_start = False
        final_turn = _turn_payload(session.state.turn_id, status="completed", started_at=started_at)
        try:
            for event in session.stream(prompt):
                if event.type == "turn.started":
                    started_at = int(time.time())
                    turn = _turn_payload(session.state.turn_id, status="inProgress", started_at=started_at)
                    final_turn = turn
                    if started_queue is not None and not announced_start:
                        try:
                            started_queue.put_nowait(turn)
                        except queue.Full:
                            pass
                    announced_start = True
                    self.service.send_notification(
                        ws,
                        client_id,
                        stream_id,
                        "thread/status/changed",
                        {
                            "threadId": session.state.thread_id,
                            "status": {"type": "active", "activeFlags": []},
                        },
                    )
                    self.service.send_notification(
                        ws,
                        client_id,
                        stream_id,
                        "turn/started",
                        {"threadId": session.state.thread_id, "turn": turn},
                    )
                elif event.type == "item.completed":
                    item = _thread_item_from_response_item(event.payload.get("item"))
                    if item is not None:
                        self.service.send_notification(
                            ws,
                            client_id,
                            stream_id,
                            "item/completed",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "completedAtMs": _now_ms(),
                            },
                        )
                elif event.type == "item.started":
                    item = _thread_item_from_response_item(event.payload.get("item"), item_id=event.payload.get("item_id"))
                    if item is not None:
                        current_agent_item_id = item.get("id") if item.get("type") == "agentMessage" else current_agent_item_id
                        self.service.send_notification(
                            ws,
                            client_id,
                            stream_id,
                            "item/started",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "startedAtMs": _now_ms(),
                            },
                        )
                elif event.type == "item.delta":
                    delta = event.payload.get("delta")
                    if isinstance(delta, str) and delta:
                        item_id = str(event.payload.get("item_id") or current_agent_item_id or f"msg_{uuid.uuid4()}")
                        current_agent_item_id = item_id
                        self.service.send_notification(
                            ws,
                            client_id,
                            stream_id,
                            "item/agentMessage/delta",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "itemId": item_id,
                                "delta": delta,
                            },
                        )
                elif event.type == "token_count":
                    usage = event.payload.get("usage")
                    if isinstance(usage, dict):
                        self.service.send_notification(
                            ws,
                            client_id,
                            stream_id,
                            "thread/tokenUsage/updated",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "tokenUsage": _token_usage_payload(usage),
                            },
                        )
                elif event.type == "thread.goal.updated":
                    goal = event.payload.get("goal")
                    if isinstance(goal, dict):
                        self.service.send_notification(
                            ws,
                            client_id,
                            stream_id,
                            "thread/goal/updated",
                            {"goal": goal},
                        )
                elif event.type == "thread.goal.cleared":
                    self.service.send_notification(
                        ws,
                        client_id,
                        stream_id,
                        "thread/goal/cleared",
                        {"threadId": session.state.thread_id},
                    )
                elif event.type == "turn.completed":
                    final_turn = _turn_payload(
                        session.state.turn_id,
                        status="completed",
                        started_at=started_at,
                        completed_at=int(time.time()),
                    )
                elif event.type in {"turn.failed", "turn.aborted"}:
                    status = "interrupted" if event.type == "turn.aborted" else "failed"
                    final_turn = _turn_payload(
                        session.state.turn_id,
                        status=status,
                        started_at=started_at,
                        completed_at=int(time.time()),
                        error=str(event.payload.get("error") or event.payload.get("reason") or ""),
                    )
        except Exception as exc:
            final_turn = _turn_payload(
                session.state.turn_id,
                status="failed",
                started_at=started_at,
                completed_at=int(time.time()),
                error=str(exc),
            )
        finally:
            if started_queue is not None and not announced_start:
                try:
                    started_queue.put_nowait(None)
                except queue.Full:
                    pass
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "turn/completed",
                {"threadId": session.state.thread_id, "turn": final_turn},
            )
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "thread/status/changed",
                {"threadId": session.state.thread_id, "status": {"type": "idle"}},
            )
            with self._lock:
                self._active_turn_clients.pop(session.state.thread_id, None)
                self._turn_threads.pop(session.state.thread_id, None)

    def _create_session(self, params: dict[str, Any]) -> CodexSession:
        session_ref: list[CodexSession] = []
        config = self._config_from_params(params, session_ref=session_ref)
        session = CodexSession(config)
        session_ref.append(session)
        with self._lock:
            self._sessions[session.state.thread_id] = session
        return session

    def _resume_session(self, params: dict[str, Any]) -> CodexSession:
        thread_id = params.get("threadId")
        path = params.get("path")
        rollout_path = Path(path).expanduser() if isinstance(path, str) and path else None
        if rollout_path is None and isinstance(thread_id, str):
            rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
        if rollout_path is None:
            raise RemoteControlError("thread/resume could not find the requested thread")
        session_ref: list[CodexSession] = []
        session = CodexSession.resume_from_rollout(rollout_path, self._config_from_params(params, session_ref=session_ref))
        session_ref.append(session)
        with self._lock:
            self._sessions[session.state.thread_id] = session
        return session

    def _fork_session(self, params: dict[str, Any]) -> CodexSession:
        thread_id = params.get("threadId")
        path = params.get("path")
        rollout_path = Path(path).expanduser() if isinstance(path, str) and path else None
        if rollout_path is None and isinstance(thread_id, str):
            rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
        if rollout_path is None:
            raise RemoteControlError("thread/fork could not find the requested thread")
        session_ref: list[CodexSession] = []
        session = CodexSession.fork_from_rollout(rollout_path, self._config_from_params(params, session_ref=session_ref))
        session_ref.append(session)
        with self._lock:
            self._sessions[session.state.thread_id] = session
        return session

    def _session_from_params(self, params: dict[str, Any]) -> CodexSession:
        thread_id = params.get("threadId")
        if isinstance(thread_id, str) and thread_id:
            return self._session_by_id(thread_id)
        path = params.get("path")
        if isinstance(path, str) and path:
            session_ref: list[CodexSession] = []
            session = CodexSession.resume_from_rollout(path, self._config_from_params({}, session_ref=session_ref))
            session_ref.append(session)
            return session
        raise RemoteControlError("missing threadId")

    def _session_by_id(self, thread_id: str) -> CodexSession:
        with self._lock:
            session = self._sessions.get(thread_id)
        if session is not None:
            return session
        rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
        if rollout_path is None:
            raise RemoteControlError(f"unknown thread `{thread_id}`")
        session_ref: list[CodexSession] = []
        session = CodexSession.resume_from_rollout(rollout_path, self._config_from_params({}, session_ref=session_ref))
        session_ref.append(session)
        with self._lock:
            self._sessions[session.state.thread_id] = session
        return session

    def _thread_list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        cwd_filter = _cwd_filter(params.get("cwd"))
        source_kinds = _source_kinds_filter(params.get("sourceKinds"))
        rows: list[dict[str, Any]] = []
        with self._lock:
            rows.extend(self._thread_payload(session, include_turns=False) for session in self._sessions.values())
        for path in _rollout_paths(self.service.config.codex_home):
            thread = _thread_payload_from_rollout(path, self.service.config, include_turns=False)
            if thread is None:
                continue
            if any(row.get("id") == thread.get("id") for row in rows):
                continue
            if cwd_filter and thread.get("cwd") not in cwd_filter:
                continue
            rows.append(thread)
        if cwd_filter:
            rows = [row for row in rows if row.get("cwd") in cwd_filter]
        rows = [row for row in rows if _thread_source_matches(row.get("source"), source_kinds)]
        rows.sort(key=lambda row: int(row.get("updatedAt") or 0), reverse=True)
        limit = params.get("limit")
        if isinstance(limit, int) and limit > 0:
            rows = rows[:limit]
        return rows

    def _thread_loaded_list(self, params: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = sorted(str(thread_id) for thread_id in self._sessions)
        cursor = params.get("cursor")
        if isinstance(cursor, str) and cursor:
            start = 0
            for index, thread_id in enumerate(data):
                if thread_id > cursor:
                    start = index
                    break
            else:
                start = len(data)
        else:
            start = 0
        limit = params.get("limit")
        effective_limit = max(1, int(limit)) if isinstance(limit, int) and limit > 0 else max(1, len(data))
        end = min(len(data), start + effective_limit)
        page = data[start:end]
        next_cursor = page[-1] if page and end < len(data) else None
        return {"data": page, "nextCursor": next_cursor}

    def _thread_start_response(self, session: CodexSession, *, include_turns: bool = False) -> dict[str, Any]:
        return {
            "thread": self._thread_payload(session, include_turns=include_turns),
            "model": session.config.model,
            "modelProvider": session.config.model_provider_id,
            "cwd": str(session.config.resolved_cwd()),
            "approvalPolicy": session.config.approval_policy,
            "approvalsReviewer": "user",
            "sandbox": _sandbox_policy_payload(session.config),
            "serviceTier": session.config.resolved_service_tier(),
            "reasoningEffort": session.config.model_reasoning_effort,
            "instructionSources": [],
        }

    def _thread_payload(self, session: CodexSession, *, include_turns: bool) -> dict[str, Any]:
        active = session.state.thread_id in self._active_turn_clients
        with self._lock:
            name = self._thread_names.get(session.state.thread_id)
            git_info = self._thread_git_info.get(session.state.thread_id)
        return _thread_payload(
            thread_id=session.state.thread_id,
            session_id=session.state.thread_id,
            cwd=str(session.config.resolved_cwd()),
            model_provider=session.config.model_provider_id,
            source=_api_session_source(session.config.session_source or "cli"),
            preview=_preview_from_history(session.state.history),
            path=str(session.state.rollout_path()) if not session.config.ephemeral else None,
            status={"type": "active", "activeFlags": []} if active else {"type": "idle"},
            turns=_turns_from_history(session) if include_turns else [],
            created_at=int(time.time()),
            updated_at=int(time.time()),
            ephemeral=session.config.ephemeral,
            name=name,
            git_info=git_info,
        )

    def _inject_thread_items(self, session: CodexSession, items: list[Any]) -> None:
        normalized_items = [dict(item) for item in items if isinstance(item, dict)]
        if not normalized_items:
            return
        for item in normalized_items:
            session.state.append_history(item)
            session.state.emit("item.completed", item=item)

    def _compact_thread(self, ws: Any, client_id: str, stream_id: str, session: CodexSession) -> None:
        try:
            session.compact()
        except Exception as exc:
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "error",
                {"message": str(exc)},
            )
            return
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "thread/compacted",
            {"threadId": session.state.thread_id, "turnId": session.state.turn_id},
        )

    def _rollback_thread(self, session: CodexSession, num_turns: int) -> None:
        if num_turns < 1:
            raise RemoteControlError("numTurns must be >= 1")
        history = list(session.state.history)
        for _ in range(num_turns):
            rollback_index = _last_user_message_index(history)
            if rollback_index is None:
                history = []
                break
            history = history[:rollback_index]
        session.state.history = history

    def _run_thread_shell_command(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        session: CodexSession,
        command: str,
    ) -> None:
        session.state.start_turn()
        turn = _turn_payload(session.state.turn_id, status="inProgress", started_at=int(time.time()))
        item_id = f"shell_{uuid.uuid4().hex}"
        item = _command_execution_item(
            item_id,
            command=command,
            cwd=str(session.config.resolved_cwd()),
            status="inProgress",
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "thread/status/changed",
            {"threadId": session.state.thread_id, "status": {"type": "active", "activeFlags": []}},
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "turn/started",
            {"threadId": session.state.thread_id, "turn": turn},
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "item/started",
            {"threadId": session.state.thread_id, "turnId": session.state.turn_id, "item": item, "startedAtMs": _now_ms()},
        )
        started_at = time.time()
        try:
            result = subprocess.run(
                command,
                cwd=str(session.config.resolved_cwd()),
                shell=True,
                text=True,
                capture_output=True,
                check=False,
            )
            output = (result.stdout or "") + (result.stderr or "")
            item = _command_execution_item(
                item_id,
                command=command,
                cwd=str(session.config.resolved_cwd()),
                status="completed" if result.returncode == 0 else "failed",
                aggregated_output=output,
                exit_code=result.returncode,
                duration_ms=int((time.time() - started_at) * 1000),
            )
            session.state.append_history(
                {
                    "type": "function_call_output",
                    "call_id": item_id,
                    "output": output,
                    "status": "completed" if result.returncode == 0 else "failed",
                }
            )
        except Exception as exc:
            item = _command_execution_item(
                item_id,
                command=command,
                cwd=str(session.config.resolved_cwd()),
                status="failed",
                aggregated_output=str(exc),
                exit_code=1,
                duration_ms=int((time.time() - started_at) * 1000),
            )
        completed_turn = _turn_payload(
            session.state.turn_id,
            status="completed",
            started_at=turn.get("startedAt"),
            completed_at=int(time.time()),
            items=[item],
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "item/completed",
            {"threadId": session.state.thread_id, "turnId": session.state.turn_id, "item": item, "completedAtMs": _now_ms()},
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "turn/completed",
            {"threadId": session.state.thread_id, "turn": completed_turn},
        )
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "thread/status/changed",
            {"threadId": session.state.thread_id, "status": {"type": "idle"}},
        )

    def _start_command_exec(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        request_id: Any,
        params: dict[str, Any],
    ) -> None:
        command = _command_exec_argv(params)
        process_id = str(params.get("processId") or "")
        if not process_id:
            raise RemoteControlError("streaming command/exec requires processId")
        with self._lock:
            if (client_id, process_id) in self._command_processes:
                raise RemoteControlError(f"duplicate active command/exec process id: {process_id}")
        cwd = _command_exec_cwd(params, self.service.config)
        env = _command_exec_env(params)
        tty = bool(params.get("tty"))
        stream_stdin = tty or bool(params.get("streamStdin"))
        stream_output = tty or bool(params.get("streamStdoutStderr"))
        process = _spawn_remote_process(
            command,
            cwd=cwd,
            env=env,
            process_id=process_id,
            tty=tty,
            stream_stdin=stream_stdin,
            size=params.get("size"),
        )
        with self._lock:
            self._command_processes[(client_id, process_id)] = process
        output_cap = _command_exec_output_bytes_cap(params)
        self._start_output_readers(
            ws,
            client_id,
            stream_id,
            process,
            notification_method="command/exec/outputDelta",
            id_param="processId",
            stream_output=stream_output,
            output_bytes_cap=output_cap,
        )
        threading.Thread(
            target=self._finish_command_exec,
            args=(ws, client_id, stream_id, request_id, process),
            daemon=True,
        ).start()

    def _start_process_spawn(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        params: dict[str, Any],
    ) -> None:
        command = _process_spawn_argv(params)
        process_handle = str(params.get("processHandle") or "")
        if not process_handle:
            raise RemoteControlError("processHandle must not be empty")
        with self._lock:
            if (client_id, process_handle) in self._process_processes:
                raise RemoteControlError(f"duplicate active process handle: {process_handle!r}")
        tty = bool(params.get("tty"))
        if params.get("size") is not None and not tty:
            raise RemoteControlError("process/spawn size requires tty: true")
        stream_stdin = tty or bool(params.get("streamStdin"))
        stream_output = tty or bool(params.get("streamStdoutStderr"))
        process = _spawn_remote_process(
            command,
            cwd=_process_spawn_cwd(params, self.service.config),
            env=_command_exec_env(params),
            process_id=process_handle,
            tty=tty,
            stream_stdin=stream_stdin,
            size=params.get("size"),
        )
        with self._lock:
            self._process_processes[(client_id, process_handle)] = process
        self._start_output_readers(
            ws,
            client_id,
            stream_id,
            process,
            notification_method="process/outputDelta",
            id_param="processHandle",
            stream_output=stream_output,
            output_bytes_cap=_process_spawn_output_bytes_cap(params),
        )
        threading.Thread(
            target=self._finish_process_spawn,
            args=(ws, client_id, stream_id, process),
            daemon=True,
        ).start()

    def _start_output_readers(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        process: _RemoteCommandProcess,
        *,
        notification_method: str,
        id_param: str,
        stream_output: bool,
        output_bytes_cap: int | None,
    ) -> None:
        if process.pty_master_fd is not None:
            thread = threading.Thread(
                target=self._stream_fd_output,
                args=(ws, client_id, stream_id, process, "stdout", process.pty_master_fd, notification_method, id_param, stream_output, output_bytes_cap),
                daemon=True,
            )
            process.reader_threads.append(thread)
            thread.start()
            return
        for stream_name, pipe in (("stdout", process.popen.stdout), ("stderr", process.popen.stderr)):
            if pipe is None:
                continue
            thread = threading.Thread(
                target=self._stream_pipe_output,
                args=(ws, client_id, stream_id, process, stream_name, pipe, notification_method, id_param, stream_output, output_bytes_cap),
                daemon=True,
            )
            process.reader_threads.append(thread)
            thread.start()

    def _stream_pipe_output(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        process: _RemoteCommandProcess,
        stream_name: str,
        pipe: Any,
        notification_method: str,
        id_param: str,
        stream_output: bool,
        output_bytes_cap: int | None,
    ) -> None:
        observed = 0
        while True:
            chunk = pipe.read(4096)
            if not chunk:
                break
            observed = self._observe_process_output(
                ws,
                client_id,
                stream_id,
                process,
                stream_name,
                chunk,
                notification_method,
                id_param,
                stream_output,
                output_bytes_cap,
                observed,
            )
            if _process_stream_cap_reached(process, stream_name):
                break
        try:
            pipe.close()
        except OSError:
            pass

    def _stream_fd_output(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        process: _RemoteCommandProcess,
        stream_name: str,
        fd: int,
        notification_method: str,
        id_param: str,
        stream_output: bool,
        output_bytes_cap: int | None,
    ) -> None:
        observed = 0
        while True:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                break
            if not chunk:
                break
            observed = self._observe_process_output(
                ws,
                client_id,
                stream_id,
                process,
                stream_name,
                chunk,
                notification_method,
                id_param,
                stream_output,
                output_bytes_cap,
                observed,
            )
            if _process_stream_cap_reached(process, stream_name):
                break

    def _observe_process_output(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        process: _RemoteCommandProcess,
        stream_name: str,
        chunk: bytes,
        notification_method: str,
        id_param: str,
        stream_output: bool,
        output_bytes_cap: int | None,
        observed: int,
    ) -> int:
        capped, observed, cap_reached = _cap_process_chunk(chunk, observed, output_bytes_cap)
        if stream_name == "stdout":
            process.stdout_cap_reached = cap_reached
        else:
            process.stderr_cap_reached = cap_reached
        if stream_output:
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                notification_method,
                {
                    id_param: process.process_id,
                    "stream": stream_name,
                    "deltaBase64": base64.b64encode(capped).decode("ascii"),
                    "capReached": cap_reached,
                },
            )
        elif stream_name == "stdout":
            process.stdout_chunks.append(capped)
        else:
            process.stderr_chunks.append(capped)
        return observed

    def _finish_command_exec(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        request_id: Any,
        process: _RemoteCommandProcess,
    ) -> None:
        exit_code = process.popen.wait()
        _join_reader_threads(process)
        with self._lock:
            self._command_processes.pop((client_id, process.process_id), None)
        _close_remote_process_fds(process)
        self.service.send_message(
            ws,
            client_id,
            stream_id,
            {
                "id": request_id,
                "result": {
                    "exitCode": exit_code,
                    "stdout": _decode_process_capture(process.stdout_chunks),
                    "stderr": _decode_process_capture(process.stderr_chunks),
                },
            },
        )

    def _finish_process_spawn(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        process: _RemoteCommandProcess,
    ) -> None:
        exit_code = process.popen.wait()
        _join_reader_threads(process)
        with self._lock:
            self._process_processes.pop((client_id, process.process_id), None)
        _close_remote_process_fds(process)
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "process/exited",
            {
                "processHandle": process.process_id,
                "exitCode": exit_code,
                "stdout": _decode_process_capture(process.stdout_chunks),
                "stdoutCapReached": process.stdout_cap_reached,
                "stderr": _decode_process_capture(process.stderr_chunks),
                "stderrCapReached": process.stderr_cap_reached,
            },
        )

    def _write_command_exec(self, client_id: str, params: dict[str, Any]) -> None:
        process_id = str(params.get("processId") or "")
        with self._lock:
            process = self._command_processes.get((client_id, process_id))
        if process is None:
            raise RemoteControlError(f"unknown command/exec process `{process_id}`")
        _write_remote_process(process, params)

    def _terminate_command_exec(self, client_id: str, params: dict[str, Any]) -> None:
        process_id = str(params.get("processId") or "")
        with self._lock:
            process = self._command_processes.pop((client_id, process_id), None)
        if process is None:
            raise RemoteControlError(f"unknown command/exec process `{process_id}`")
        _terminate_remote_process(process)

    def _resize_command_exec(self, client_id: str, params: dict[str, Any]) -> None:
        process_id = str(params.get("processId") or "")
        with self._lock:
            process = self._command_processes.get((client_id, process_id))
        if process is None:
            raise RemoteControlError(f"unknown command/exec process `{process_id}`")
        _resize_remote_process_pty(process, params.get("size"))

    def _write_process_spawn(self, client_id: str, params: dict[str, Any]) -> None:
        process_handle = str(params.get("processHandle") or "")
        with self._lock:
            process = self._process_processes.get((client_id, process_handle))
        if process is None:
            raise RemoteControlError(f"no active process for process handle {process_handle!r}")
        _write_remote_process(process, params)

    def _kill_process_spawn(self, client_id: str, params: dict[str, Any]) -> None:
        process_handle = str(params.get("processHandle") or "")
        with self._lock:
            process = self._process_processes.pop((client_id, process_handle), None)
        if process is None:
            raise RemoteControlError(f"no active process for process handle {process_handle!r}")
        _terminate_remote_process(process)

    def _resize_process_spawn(self, client_id: str, params: dict[str, Any]) -> None:
        process_handle = str(params.get("processHandle") or "")
        with self._lock:
            process = self._process_processes.get((client_id, process_handle))
        if process is None:
            raise RemoteControlError(f"no active process for process handle {process_handle!r}")
        _resize_remote_process_pty(process, params.get("size"))

    def _account_login_start(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        kind = str(params.get("type") or "")
        login_id = str(uuid.uuid4())
        if kind == "apiKey":
            api_key = str(params.get("apiKey") or "").strip()
            if not api_key:
                raise RemoteControlError("apiKey login requires apiKey")
            login_with_api_key(api_key, self.service.config.auth_codex_home)
            self._send_login_success(ws, client_id, stream_id, None)
            return {"type": "apiKey"}
        if kind == "chatgptAuthTokens":
            access_token = str(params.get("accessToken") or params.get("access_token") or "").strip()
            account_id = str(params.get("chatgptAccountId") or params.get("chatgpt_account_id") or "").strip()
            plan_type = params.get("chatgptPlanType") or params.get("chatgpt_plan_type")
            if not access_token or not account_id:
                raise RemoteControlError("chatgptAuthTokens login requires accessToken and chatgptAccountId")
            tokens: dict[str, Any] = {"access_token": access_token, "account_id": account_id}
            if isinstance(plan_type, str) and plan_type:
                tokens["chatgpt_plan_type"] = plan_type
            _write_auth_json(
                auth_json_path(self.service.config.auth_codex_home),
                {
                    "auth_mode": "chatgptAuthTokens",
                    "tokens": tokens,
                    "last_refresh": _utc_now_iso(),
                },
            )
            self._send_login_success(ws, client_id, stream_id, None)
            return {"type": "chatgptAuthTokens"}
        if kind == "chatgptDeviceCode":
            code = request_device_code()
            threading.Thread(
                target=self._complete_device_code_login,
                args=(ws, client_id, stream_id, login_id, code),
                daemon=True,
            ).start()
            return {
                "type": "chatgptDeviceCode",
                "loginId": login_id,
                "verificationUrl": code.verification_url,
                "userCode": code.user_code,
            }
        if kind == "chatgpt":
            started: queue.Queue[str | Exception] = queue.Queue(maxsize=1)
            threading.Thread(
                target=self._run_browser_login,
                args=(ws, client_id, stream_id, login_id, started),
                daemon=True,
            ).start()
            try:
                auth_url = started.get(timeout=10)
            except queue.Empty as exc:
                raise RemoteControlError("ChatGPT login server did not start") from exc
            if isinstance(auth_url, Exception):
                raise RemoteControlError(str(auth_url)) from auth_url
            return {"type": "chatgpt", "loginId": login_id, "authUrl": auth_url}
        raise RemoteControlError(f"unsupported account/login/start type `{kind}`")

    def _run_browser_login(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        login_id: str,
        started: queue.Queue[str | Exception],
    ) -> None:
        def on_start(_port: int, auth_url: str) -> None:
            try:
                started.put_nowait(auth_url)
            except queue.Full:
                pass

        try:
            run_browser_login(
                codex_home=self.service.config.auth_codex_home,
                open_browser=False,
                on_start=on_start,
            )
        except Exception as exc:
            try:
                started.put_nowait(exc)
            except queue.Full:
                pass
            self._send_login_completed(ws, client_id, stream_id, login_id, success=False, error=str(exc))
            return
        self._send_login_success(ws, client_id, stream_id, login_id)

    def _complete_device_code_login(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        login_id: str,
        code: Any,
    ) -> None:
        try:
            complete_device_code_login(code, codex_home=self.service.config.auth_codex_home)
        except Exception as exc:
            self._send_login_completed(ws, client_id, stream_id, login_id, success=False, error=str(exc))
            return
        self._send_login_success(ws, client_id, stream_id, login_id)

    def _send_login_success(self, ws: Any, client_id: str, stream_id: str, login_id: str | None) -> None:
        self._send_login_completed(ws, client_id, stream_id, login_id, success=True, error=None)
        account = _account_read_response(self.service.config).get("account")
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "account/updated",
            {
                "authMode": _auth_mode_from_account(account),
                "planType": (account or {}).get("planType") if isinstance(account, dict) else None,
            },
        )

    def _send_login_completed(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        login_id: str | None,
        *,
        success: bool,
        error: str | None,
    ) -> None:
        self.service.send_notification(
            ws,
            client_id,
            stream_id,
            "account/login/completed",
            {"loginId": login_id, "success": success, "error": error},
        )

    def _config_from_params(
        self,
        params: dict[str, Any],
        *,
        session_ref: list[CodexSession] | None = None,
    ) -> CodexConfig:
        base = self.service.config
        cwd = params.get("cwd")
        model = params.get("model")
        model_provider = params.get("modelProvider")
        sandbox = params.get("sandbox") or params.get("sandboxPolicy")
        approval = params.get("approvalPolicy")
        return CodexConfig(
            cwd=Path(cwd).expanduser() if isinstance(cwd, str) and cwd else base.cwd,
            codex_home=base.codex_home,
            auth_codex_home=base.auth_codex_home,
            model=model if isinstance(model, str) and model else (base.model or CodexConfig().model),
            model_provider_id=model_provider if isinstance(model_provider, str) and model_provider else "openai",
            session_source="cli",
            sandbox=sandbox if sandbox in {"read-only", "workspace-write", "danger-full-access"} else "workspace-write",
            approval_policy=approval if approval in {"untrusted", "on-failure", "on-request", "never"} else "never",
            approval_provider=self._remote_approval_provider(session_ref) if session_ref is not None else None,
            request_user_input_provider=self._remote_request_user_input_provider(session_ref) if session_ref is not None else None,
            skip_git_repo_check=True,
        )

    def _remote_request_user_input_provider(self, session_ref: list[CodexSession]):
        def provider(questions: list[dict[str, Any]]) -> dict[str, Any] | None:
            session = session_ref[0] if session_ref else None
            if session is None:
                return None
            target = self._active_target_for_thread(session.state.thread_id)
            if target is None:
                return None
            ws, client_id, stream_id = target
            result = self._send_server_request(
                ws,
                client_id,
                stream_id,
                "item/tool/requestUserInput",
                {
                    "threadId": session.state.thread_id,
                    "turnId": session.state.turn_id,
                    "itemId": f"request_user_input_{uuid.uuid4().hex}",
                    "questions": [_request_user_input_question_payload(question) for question in questions],
                },
                timeout_seconds=None,
            )
            return result if isinstance(result, dict) else None

        return provider

    def _remote_approval_provider(self, session_ref: list[CodexSession]):
        def provider(request: dict[str, Any]) -> dict[str, Any] | str | None:
            session = session_ref[0] if session_ref else None
            if session is None:
                return None
            target = self._active_target_for_thread(session.state.thread_id)
            if target is None:
                return None
            ws, client_id, stream_id = target
            method, params = _approval_server_request(session, request)
            result = self._send_server_request(ws, client_id, stream_id, method, params, timeout_seconds=None)
            if not isinstance(result, dict):
                return None
            decision = result.get("decision")
            if decision is None and isinstance(result.get("permissions"), dict):
                return {"decision": "accept"}
            if _remote_approval_decision_grants(decision):
                scope = result.get("scope")
                return {"decision": "approved_for_session" if scope == "session" or decision == "acceptForSession" else "approved"}
            return {"decision": "denied"}

        return provider

    def _active_target_for_thread(self, thread_id: str) -> tuple[Any, str, str] | None:
        with self._lock:
            return self._active_turn_clients.get(thread_id)

    def _send_server_request(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        method: str,
        params: dict[str, Any],
        *,
        timeout_seconds: float | None,
    ) -> dict[str, Any] | None:
        request_id = self._next_server_request_id_value()
        response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        with self._lock:
            self._pending_server_requests[request_id] = response_queue
        self.service.send_message(ws, client_id, stream_id, {"id": request_id, "method": method, "params": params})
        try:
            response = response_queue.get(timeout=timeout_seconds)
        except queue.Empty:
            with self._lock:
                self._pending_server_requests.pop(request_id, None)
            return None
        if isinstance(response.get("error"), dict):
            return None
        result = response.get("result")
        return result if isinstance(result, dict) else {}

    def _next_server_request_id_value(self) -> int:
        with self._lock:
            request_id = self._next_server_request_id
            self._next_server_request_id += 1
            return request_id

    def _handle_server_request_response(self, request_id: Any, message: dict[str, Any]) -> None:
        with self._lock:
            response_queue = self._pending_server_requests.pop(request_id, None)
        if response_queue is None:
            return
        try:
            response_queue.put_nowait(message)
        except queue.Full:
            pass


class _RemoteControlPersistentState:
    def __init__(self, codex_home: Path):
        self.path = codex_home / REMOTE_CONTROL_STATE_FILE
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def installation_id(self) -> str:
        with self._lock:
            state = self._load()
            installation_id = state.get("installation_id")
            if not isinstance(installation_id, str) or not installation_id:
                installation_id = str(uuid.uuid4())
                state["installation_id"] = installation_id
                self._save(state)
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

    def subscribe_cursor(self) -> str | None:
        value = self._load().get("subscribe_cursor")
        return value if isinstance(value, str) and value else None

    def save_subscribe_cursor(self, cursor: str) -> None:
        with self._lock:
            state = self._load()
            state["subscribe_cursor"] = cursor
            self._save(state)

    def legacy_stream_id(self, client_id: str) -> str:
        state = self._load()
        streams = state.get("legacy_stream_ids")
        if not isinstance(streams, dict):
            streams = {}
        value = streams.get(client_id)
        if isinstance(value, str) and value:
            return value
        value = str(uuid.uuid4())
        streams[client_id] = value
        state["legacy_stream_ids"] = streams
        self._save(state)
        return value

    def save_legacy_stream_id(self, client_id: str, stream_id: str) -> None:
        with self._lock:
            state = self._load()
            streams = state.setdefault("legacy_stream_ids", {})
            if isinstance(streams, dict):
                streams[client_id] = stream_id
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
            "User-Agent": _codex_user_agent(originator, user_agent_suffix),
            **_remote_auth_headers(auth),
            "Accept": "application/json",
            "Content-Type": "application/json",
            REMOTE_CONTROL_ACCOUNT_ID_HEADER: auth.account_id,
            REMOTE_CONTROL_INSTALLATION_ID_HEADER: installation_id,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REMOTE_CONTROL_CONNECT_TIMEOUT_SECONDS) as response:
            status = int(getattr(response, "status", 200))
            response_body = response.read()
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
        "ChatGPT-Account-ID": auth.account_id,
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


def _split_server_envelope_for_transport(envelope: ServerEnvelope) -> list[ServerEnvelope]:
    if envelope.event.get("type") != RemoteServerEvent.SERVER_MESSAGE.value:
        return [envelope]
    if len(_serialized_envelope_bytes(envelope)) <= REMOTE_CONTROL_SEGMENT_MAX_BYTES:
        return [envelope]
    message = envelope.event.get("message")
    raw = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    message_size_bytes = len(raw)
    if message_size_bytes > REMOTE_CONTROL_REASSEMBLED_MAX_BYTES:
        return []
    minimal_segment_count = min(max(1, message_size_bytes), REMOTE_CONTROL_SEGMENT_COUNT_MAX)
    if (
        len(_serialized_envelope_bytes(_server_message_chunk_envelope(envelope, 0, minimal_segment_count, message_size_bytes, raw[:1])))
        > REMOTE_CONTROL_SEGMENT_MAX_BYTES
    ):
        return []
    segment_count = max(2, _ceil_div(message_size_bytes, REMOTE_CONTROL_SEGMENT_TARGET_BYTES))
    while True:
        chunk_size = max(1, _ceil_div(message_size_bytes, segment_count))
        segment_count = _ceil_div(message_size_bytes, chunk_size)
        chunks = list(raw[i : i + chunk_size] for i in range(0, message_size_bytes, chunk_size))
        if len(chunks) <= REMOTE_CONTROL_SEGMENT_COUNT_MAX:
            envelopes = [
                _server_message_chunk_envelope(envelope, segment_id, len(chunks), message_size_bytes, chunk)
                for segment_id, chunk in enumerate(chunks)
            ]
            if all(len(_serialized_envelope_bytes(item)) <= REMOTE_CONTROL_SEGMENT_MAX_BYTES for item in envelopes):
                return envelopes
        if chunk_size == 1 or len(chunks) >= REMOTE_CONTROL_SEGMENT_COUNT_MAX:
            return []
        next_segment_count = segment_count + 1
        next_chunk_size = max(1, _ceil_div(message_size_bytes, next_segment_count))
        segment_count = message_size_bytes if next_chunk_size == chunk_size else next_segment_count


def _server_message_chunk_envelope(
    envelope: ServerEnvelope,
    segment_id: int,
    segment_count: int,
    message_size_bytes: int,
    chunk: bytes,
) -> ServerEnvelope:
    return ServerEnvelope(
        client_id=envelope.client_id,
        stream_id=envelope.stream_id,
        seq_id=envelope.seq_id,
        event={
            "type": RemoteServerEvent.SERVER_MESSAGE_CHUNK.value,
            "segment_id": segment_id,
            "segment_count": segment_count,
            "message_size_bytes": message_size_bytes,
            "message_chunk_base64": base64.b64encode(chunk).decode("ascii"),
        },
    )


def _serialized_envelope_bytes(envelope: ServerEnvelope) -> bytes:
    return json.dumps(envelope.to_wire(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _server_envelope_segment_id(envelope: ServerEnvelope) -> int:
    return _optional_int(envelope.event.get("segment_id")) or 0


def _ceil_div(left: int, right: int) -> int:
    return -(-left // right)


def _start_remote_control_daemon(config: RemoteControlConfig) -> int:
    config.codex_home.mkdir(parents=True, exist_ok=True)
    pid = _read_pid_file(config)
    if pid is not None and _process_is_running(pid):
        return _print_daemon_start_status(config, pid=pid, log_path=_remote_control_log_path(config))

    log_path = _remote_control_log_path(config)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["CODEX_PY_HOME"] = str(config.codex_home)
    if config.auth_codex_home is not None:
        env["CODEX_AUTH_HOME"] = str(config.auth_codex_home)
    command = [sys.executable, "-m", "agents.codex", "remote-control"]
    with log_path.open("ab") as log_file:
        process = subprocess.Popen(
            command,
            cwd=str(config.cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    _write_pid_file(config, process.pid)
    return _print_daemon_start_status(config, pid=process.pid, log_path=log_path)


def _print_daemon_start_status(config: RemoteControlConfig, *, pid: int, log_path: Path) -> int:
    status = RemoteControlReadyStatus(
        status="connecting",
        server_name=config.server_name,
        environment_id=None,
        timed_out=True,
    )
    if config.json_output:
        print(
            remote_control_start_json_output(
                status,
                mode="daemon",
                daemon={"pid": pid, "logPath": str(log_path)},
            ).to_json(),
            flush=True,
        )
    else:
        for line in remote_control_start_human_lines(status, mode="daemon"):
            print(line, flush=True)
    return 0


def _stop_remote_control(config: RemoteControlConfig) -> int:
    pid = _read_pid_file(config)
    if pid is None or not _process_is_running(pid):
        _clear_pid_file(config)
        message = remote_control_stop_human_message("notRunning")
        if config.json_output:
            print(json.dumps({"status": "notRunning"}, ensure_ascii=False, separators=(",", ":")), flush=True)
        else:
            print(message, flush=True)
        return 0
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        _clear_pid_file(config)
    except PermissionError as exc:
        raise RemoteControlError(f"cannot stop remote control process {pid}: {exc}") from exc
    for _ in range(40):
        if not _process_is_running(pid):
            break
        time.sleep(0.05)
    _clear_pid_file(config)
    if config.json_output:
        print(json.dumps({"status": "stopped"}, ensure_ascii=False, separators=(",", ":")), flush=True)
    else:
        print(remote_control_stop_human_message("stopped"), flush=True)
    return 0


def _remote_control_log_path(config: RemoteControlConfig) -> Path:
    return config.codex_home / "remote-control.log"


def _pid_file(config: RemoteControlConfig) -> Path:
    return config.codex_home / REMOTE_CONTROL_PID_FILE


def _read_pid_file(config: RemoteControlConfig) -> int | None:
    try:
        return int(_pid_file(config).read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_pid_file(config: RemoteControlConfig, pid: int | None = None) -> None:
    config.codex_home.mkdir(parents=True, exist_ok=True)
    _pid_file(config).write_text(str(pid or os.getpid()), encoding="utf-8")


def _clear_pid_file(config: RemoteControlConfig) -> None:
    path = _pid_file(config)
    try:
        if path.exists() and _read_pid_file(config) == os.getpid():
            path.unlink()
        elif path.exists():
            pid = _read_pid_file(config)
            if pid is None or not _process_is_running(pid):
                path.unlink()
    except OSError:
        pass


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _initialize_response(config: RemoteControlConfig, params: dict[str, Any] | None = None) -> dict[str, Any]:
    system = platform.system().lower()
    if system == "darwin":
        platform_os = "macos"
    elif system.startswith("win"):
        platform_os = "windows"
    elif system:
        platform_os = system
    else:
        platform_os = sys.platform
    platform_family = "windows" if platform_os == "windows" else "unix"
    client_info = params.get("clientInfo") if isinstance(params, dict) else None
    client_name = client_info.get("name") if isinstance(client_info, dict) else None
    client_version = client_info.get("version") if isinstance(client_info, dict) else None
    mutates_global_identity = isinstance(client_name, str) and client_name not in {
        "codex_app_server_daemon",
        "codex-backend",
    }
    if mutates_global_identity:
        originator = client_name
        suffix = f"{client_name}; {client_version}" if isinstance(client_version, str) else None
    else:
        originator, suffix = _remote_control_client_identity(
            config.app_server_client_name,
            config.app_server_client_version,
            allow_desktop_compat_identity=config.allow_desktop_compat_identity,
        )
    user_agent = _codex_user_agent(originator, suffix)
    return {
        "userAgent": user_agent,
        "codexHome": str(config.codex_home),
        "platformFamily": platform_family,
        "platformOs": platform_os,
    }


def _remote_control_status_payload(
    service: RemoteControlService,
    *,
    status: RemoteControlConnectionStatus | None = None,
) -> dict[str, Any]:
    return {
        "status": status or service.status,
        "serverName": service.config.server_name,
        "installationId": service.installation_id,
        "environmentId": service.environment_id,
    }


def _config_read_response(config: RemoteControlConfig) -> dict[str, Any]:
    active = CodexConfig(
        cwd=config.cwd,
        codex_home=config.codex_home,
        auth_codex_home=config.auth_codex_home,
        model=config.model or CodexConfig().model,
        skip_git_repo_check=True,
    )
    return {
        "config": {
            "model": active.model,
            "review_model": None,
            "model_context_window": active.resolved_model_context_window(),
            "model_auto_compact_token_limit": active.resolved_auto_compact_token_limit(),
            "model_auto_compact_token_limit_scope": None,
            "model_provider": active.model_provider_id,
            "approval_policy": active.approval_policy,
            "approvals_reviewer": "user",
            "sandbox_mode": active.sandbox,
            "sandbox_workspace_write": None,
            "forced_chatgpt_workspace_id": None,
            "forced_login_method": None,
            "web_search": "enabled" if active.include_web_search_tool else None,
            "tools": None,
            "instructions": None,
            "developer_instructions": None,
            "compact_prompt": active.compact_prompt,
            "model_reasoning_effort": active.model_reasoning_effort,
            "model_reasoning_summary": active.model_reasoning_summary,
            "model_verbosity": active.model_verbosity,
            "service_tier": active.resolved_service_tier(),
            "analytics": None,
            "desktop": None,
        },
        "origins": {},
        "layers": None,
    }


def _sandbox_policy_payload(config: CodexConfig) -> dict[str, Any]:
    network_access = config.network_access == "enabled"
    if config.sandbox == "danger-full-access":
        return {"type": "dangerFullAccess"}
    if config.sandbox == "read-only":
        return {"type": "readOnly", "networkAccess": network_access}
    return {
        "type": "workspaceWrite",
        "writableRoots": [str(Path(root).expanduser()) for root in config.writable_roots],
        "networkAccess": network_access,
        "excludeTmpdirEnvVar": config.exclude_tmpdir_env_var,
        "excludeSlashTmp": config.exclude_slash_tmp,
    }


def _model_list_response(config: RemoteControlConfig) -> dict[str, Any]:
    active = CodexConfig(model=config.model or CodexConfig().model)
    effort = active.resolved_reasoning() or {}
    default_effort = str(effort.get("effort") or "medium")
    tiers = active.resolved_model_service_tiers()
    return {
        "data": [
            {
                "id": active.model,
                "model": active.model,
                "upgrade": None,
                "upgradeInfo": None,
                "availabilityNux": None,
                "displayName": active.model,
                "description": "Configured Python Codex model",
                "hidden": False,
                "supportedReasoningEfforts": [
                    {"reasoningEffort": "low", "description": "Low reasoning effort"},
                    {"reasoningEffort": "medium", "description": "Medium reasoning effort"},
                    {"reasoningEffort": "high", "description": "High reasoning effort"},
                    {"reasoningEffort": "xhigh", "description": "Extra high reasoning effort"},
                ],
                "defaultReasoningEffort": default_effort,
                "inputModalities": ["text", "image"] if active.resolved_supports_image_input() else ["text"],
                "supportsPersonality": False,
                "additionalSpeedTiers": [tier.get("id", "") for tier in tiers if isinstance(tier.get("id"), str)],
                "serviceTiers": tiers,
                "defaultServiceTier": active.resolved_service_tier(),
                "isDefault": True,
            }
        ],
        "nextCursor": None,
    }


def _collaboration_mode_list_response() -> dict[str, Any]:
    # Mirrors the reference builtin_collaboration_mode_presets() after app-server protocol
    # conversion: developer_instructions are intentionally omitted on this wire API.
    return {
        "data": [
            {
                "name": "Plan",
                "mode": "plan",
                "model": None,
                "reasoning_effort": "medium",
            },
            {
                "name": "Default",
                "mode": "default",
                "model": None,
                "reasoning_effort": None,
            },
        ],
    }


def _empty_plugin_detail(params: dict[str, Any], config: RemoteControlConfig) -> dict[str, Any]:
    plugin_id = str(params.get("pluginId") or params.get("id") or "python-codex")
    plugin_name = str(params.get("pluginName") or plugin_id)
    return {
        "marketplaceName": "local",
        "marketplacePath": str(config.codex_home / "plugins"),
        "summary": {
            "id": plugin_id,
            "remotePluginId": None,
            "localVersion": None,
            "name": plugin_name,
            "shareContext": None,
            "source": {"type": "local", "path": str(config.codex_home / "plugins" / plugin_id)},
            "installed": False,
            "enabled": False,
            "installPolicy": "NOT_AVAILABLE",
            "authPolicy": "ON_INSTALL",
            "availability": "AVAILABLE",
            "interface": None,
            "keywords": [],
        },
        "description": None,
        "skills": [],
        "hooks": [],
        "apps": [],
        "mcpServers": [],
    }


def _marketplace_empty_response(method: str, params: dict[str, Any], config: RemoteControlConfig) -> dict[str, Any]:
    marketplace_name = str(params.get("name") or params.get("marketplaceName") or "local")
    root = str(config.codex_home / "plugins")
    if method == "marketplace/add":
        return {"marketplaceName": marketplace_name, "installedRoot": root, "alreadyAdded": True}
    if method == "marketplace/remove":
        return {"marketplaceName": marketplace_name, "installedRoot": None}
    return {"selectedMarketplaces": [], "upgradedRoots": [], "errors": []}


def _plugin_share_empty_response(method: str, params: dict[str, Any], config: RemoteControlConfig) -> dict[str, Any]:
    remote_plugin_id = str(params.get("remotePluginId") or params.get("pluginId") or "python-codex")
    if method == "plugin/share/save":
        return {"remotePluginId": remote_plugin_id, "shareUrl": ""}
    if method == "plugin/share/updateTargets":
        return {"principals": [], "discoverability": "PRIVATE"}
    if method == "plugin/share/checkout":
        plugin_id = str(params.get("pluginId") or remote_plugin_id)
        plugin_path = str(config.codex_home / "plugins" / plugin_id)
        marketplace_path = str(config.codex_home / "plugins")
        return {
            "remotePluginId": remote_plugin_id,
            "pluginId": plugin_id,
            "pluginName": plugin_id,
            "pluginPath": plugin_path,
            "marketplaceName": "local",
            "marketplacePath": marketplace_path,
            "remoteVersion": None,
        }
    return {}


def _account_read_response(config: RemoteControlConfig) -> dict[str, Any]:
    try:
        from .auth import auth_status

        status = auth_status(config.auth_codex_home)
    except Exception:
        status = {}
    if status.get("has_chatgpt_tokens"):
        account = {
            "type": "chatgpt",
            "email": status.get("email") or "unknown",
            "planType": status.get("plan_type") or "unknown",
        }
    elif status.get("has_api_key"):
        account = {"type": "apiKey"}
    else:
        account = None
    return {"account": account, "requiresOpenaiAuth": account is None}


def _account_logout(config: RemoteControlConfig) -> None:
    try:
        auth_json_path(config.auth_codex_home).unlink()
    except FileNotFoundError:
        pass


def _auth_mode_from_account(account: Any) -> str | None:
    if not isinstance(account, dict):
        return None
    account_type = account.get("type")
    if account_type == "apiKey":
        return "apikey"
    if account_type == "chatgpt":
        return "chatgpt"
    return account_type if isinstance(account_type, str) else None


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _account_rate_limits_response(config: RemoteControlConfig) -> dict[str, Any]:
    snapshots: list[Any] = []
    try:
        from .auth import fetch_chatgpt_rate_limits

        snapshots = fetch_chatgpt_rate_limits(config.auth_codex_home, base_url=config.remote_control_url, timeout=10)
    except Exception:
        snapshots = []
    if snapshots:
        primary = _rate_limit_snapshot_payload(snapshots[0])
        by_id = {
            str(snapshot.limit_id or "codex"): _rate_limit_snapshot_payload(snapshot)
            for snapshot in snapshots
        }
    else:
        primary = _empty_rate_limit_snapshot()
        by_id = {"codex": primary}
    return {"rateLimits": primary, "rateLimitsByLimitId": by_id}


def _auth_status_response(config: RemoteControlConfig, *, include_token: bool = False) -> dict[str, Any]:
    try:
        snapshot = load_auth_snapshot(config.auth_codex_home, mode="auto")
    except Exception:
        snapshot = None
    if snapshot is None:
        return {"authMethod": None, "authToken": None, "requiresOpenaiAuth": True}
    auth_method = "chatgptAuthTokens" if snapshot.is_chatgpt else "apikey"
    token = snapshot.access_token if snapshot.is_chatgpt else snapshot.api_key
    return {
        "authMethod": auth_method,
        "authToken": token if include_token else None,
        "requiresOpenaiAuth": False,
    }


def _request_user_input_question_payload(question: dict[str, Any]) -> dict[str, Any]:
    options = question.get("options")
    return {
        "id": str(question.get("id") or ""),
        "header": str(question.get("header") or ""),
        "question": str(question.get("question") or ""),
        "isOther": bool(question.get("isOther") or question.get("is_other")),
        "isSecret": bool(question.get("isSecret") or question.get("is_secret")),
        "options": [
            {
                "label": str(option.get("label") or ""),
                "description": str(option.get("description") or ""),
            }
            for option in options
            if isinstance(option, dict)
        ]
        if isinstance(options, list)
        else None,
    }


def _approval_server_request(session: CodexSession, request: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    tool = str(request.get("tool") or "")
    item_id = f"approval_{uuid.uuid4().hex}"
    if tool == "apply_patch":
        files = request.get("files")
        grant_root = _common_parent_root(files) if isinstance(files, list) else None
        return (
            "item/fileChange/requestApproval",
            {
                "threadId": session.state.thread_id,
                "turnId": session.state.turn_id,
                "itemId": item_id,
                "startedAtMs": _now_ms(),
                "reason": request.get("reason"),
                "grantRoot": grant_root,
            },
        )
    return (
        "item/commandExecution/requestApproval",
        {
            "threadId": session.state.thread_id,
            "turnId": session.state.turn_id,
            "itemId": item_id,
            "startedAtMs": _now_ms(),
            "approvalId": None,
            "reason": request.get("reason") or request.get("justification"),
            "networkApprovalContext": None,
            "command": request.get("cmd") or request.get("command"),
            "cwd": str(session.config.resolved_cwd()),
            "commandActions": None,
            "proposedExecpolicyAmendment": None,
            "proposedNetworkPolicyAmendments": None,
        },
    )


def _common_parent_root(paths: list[Any]) -> str | None:
    strings = [str(path) for path in paths if isinstance(path, str) and path]
    if not strings:
        return None
    try:
        return str(Path(os.path.commonpath(strings)).expanduser())
    except Exception:
        return None


def _remote_approval_decision_grants(decision: Any) -> bool:
    if decision in {"accept", "acceptForSession"}:
        return True
    if isinstance(decision, dict):
        return bool({"acceptWithExecpolicyAmendment", "applyNetworkPolicyAmendment"} & set(decision))
    return False


def _fs_path(params: dict[str, Any], key: str = "path") -> Path:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise RemoteControlError(f"{key} must be an absolute path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise RemoteControlError(f"{key} must be an absolute path")
    return path


def _fs_read_file_response(params: dict[str, Any]) -> dict[str, Any]:
    return {"dataBase64": base64.b64encode(_fs_path(params).read_bytes()).decode("ascii")}


def _fs_write_file(params: dict[str, Any]) -> None:
    path = _fs_path(params)
    data_base64 = params.get("dataBase64")
    if not isinstance(data_base64, str):
        raise RemoteControlError("dataBase64 must be provided")
    try:
        data = base64.b64decode(data_base64.encode("ascii"), validate=True)
    except Exception as exc:
        raise RemoteControlError("dataBase64 is not valid base64") from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _fs_create_directory(params: dict[str, Any]) -> None:
    _fs_path(params).mkdir(parents=True, exist_ok=True)


def _fs_get_metadata_response(params: dict[str, Any]) -> dict[str, Any]:
    path = _fs_path(params)
    info = path.lstat()
    try:
        resolved_info = path.stat()
    except OSError:
        resolved_info = info
    return {
        "isDirectory": path.is_dir(),
        "isFile": path.is_file(),
        "isSymlink": path.is_symlink(),
        "createdAtMs": int(getattr(resolved_info, "st_birthtime", resolved_info.st_ctime) * 1000),
        "modifiedAtMs": int(resolved_info.st_mtime * 1000),
    }


def _fs_read_directory_response(params: dict[str, Any]) -> dict[str, Any]:
    path = _fs_path(params)
    entries = []
    for child in sorted(path.iterdir(), key=lambda item: item.name.lower()):
        entries.append(
            {
                "fileName": child.name,
                "isDirectory": child.is_dir(),
                "isFile": child.is_file(),
            }
        )
    return {"entries": entries}


def _fs_remove(params: dict[str, Any]) -> None:
    path = _fs_path(params)
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _fs_copy(params: dict[str, Any]) -> None:
    source = _fs_path(params, "sourcePath")
    if "destinationPath" in params:
        destination = _fs_path(params, "destinationPath")
    else:
        destination = _fs_path(params, "destPath")
    if source.is_dir() and not source.is_symlink():
        if params.get("recursive") is not True:
            raise RemoteControlError("recursive=true is required to copy directories")
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _fuzzy_file_search_response(params: dict[str, Any]) -> dict[str, Any]:
    query = str(params.get("query") or "").lower()
    roots = params.get("roots")
    if not query or not isinstance(roots, list):
        return {"files": []}
    results: list[dict[str, Any]] = []
    for root_raw in roots:
        if not isinstance(root_raw, str) or not root_raw:
            continue
        root = Path(root_raw).expanduser()
        if not root.is_dir():
            continue
        for path in root.rglob("*"):
            if len(results) >= 50:
                break
            if not path.is_file():
                continue
            name = path.name
            rel = str(path.relative_to(root))
            haystack = rel.lower()
            if query not in haystack:
                continue
            indices = _substring_indices(haystack, query)
            results.append(
                {
                    "root": str(root),
                    "path": rel,
                    "match_type": "file",
                    "file_name": name,
                    "score": max(0, 100 - len(rel)),
                    "indices": indices,
                }
            )
        if len(results) >= 50:
            break
    return {"files": results}


def _substring_indices(haystack: str, needle: str) -> list[int] | None:
    start = haystack.find(needle)
    if start < 0:
        return None
    return list(range(start, start + len(needle)))


def _git_diff_to_remote_response(params: dict[str, Any]) -> dict[str, Any]:
    cwd = Path(str(params.get("cwd") or os.getcwd())).expanduser()
    if not cwd.is_absolute():
        cwd = cwd.resolve()
    sha = "0" * 40
    diff = ""
    try:
        sha_result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd),
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        if sha_result.returncode == 0 and sha_result.stdout.strip():
            sha = sha_result.stdout.strip()
        diff_result = subprocess.run(
            ["git", "diff", "--no-ext-diff", "--"],
            cwd=str(cwd),
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if diff_result.returncode == 0:
            diff = diff_result.stdout
    except Exception:
        pass
    return {"sha": sha, "diff": diff}


def _conversation_summary_response(params: dict[str, Any], config: RemoteControlConfig) -> dict[str, Any]:
    path_raw = params.get("path")
    path = Path(path_raw).expanduser() if isinstance(path_raw, str) and path_raw else None
    thread = _thread_payload_from_rollout(path, config) if path is not None else None
    if thread is None:
        thread_id = str(params.get("conversationId") or params.get("threadId") or "")
        rollout = _find_rollout_path(config.codex_home, thread_id) if thread_id else None
        thread = _thread_payload_from_rollout(rollout, config) if rollout is not None else None
    if thread is None:
        raise RemoteControlError("conversation summary could not find the requested thread")
    timestamp = _seconds_to_iso(_optional_int(thread.get("createdAt")))
    updated_at = _seconds_to_iso(_optional_int(thread.get("updatedAt")))
    return {
        "summary": {
            "conversationId": thread.get("id"),
            "path": thread.get("path") or "",
            "preview": thread.get("preview") or "",
            "timestamp": timestamp,
            "updatedAt": updated_at,
            "modelProvider": thread.get("modelProvider") or "openai",
            "cwd": thread.get("cwd") or str(config.cwd),
            "cliVersion": thread.get("cliVersion") or PYTHON_REMOTE_CONTROL_VERSION,
            "source": _api_session_source(thread.get("source") or "cli"),
            "gitInfo": thread.get("gitInfo"),
        }
    }


def _spawn_remote_process(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    process_id: str,
    tty: bool,
    stream_stdin: bool,
    size: Any,
) -> _RemoteCommandProcess:
    if tty:
        try:
            import pty as pty_module
        except ImportError as exc:
            raise RemoteControlError("PTY mode is not available on this platform") from exc
        master_fd, slave_fd = pty_module.openpty()
        try:
            _set_pty_size(master_fd, size)
            popen = subprocess.Popen(
                command,
                cwd=str(cwd),
                env=env,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                close_fds=True,
                start_new_session=True,
            )
        except Exception:
            os.close(master_fd)
            os.close(slave_fd)
            raise
        os.close(slave_fd)
        return _RemoteCommandProcess(
            process_id=process_id,
            popen=popen,
            cwd=cwd,
            pty_master_fd=master_fd,
            stdin_enabled=True,
        )

    popen = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdin=subprocess.PIPE if stream_stdin else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return _RemoteCommandProcess(
        process_id=process_id,
        popen=popen,
        cwd=cwd,
        stdin_enabled=stream_stdin,
    )


def _write_remote_process(process: _RemoteCommandProcess, params: dict[str, Any]) -> None:
    if params.get("deltaBase64") is None and params.get("closeStdin") is not True:
        raise RemoteControlError("stdin write requires deltaBase64 or closeStdin")
    if not process.stdin_enabled:
        raise RemoteControlError("stdin streaming is not enabled for this process")
    delta = b""
    if isinstance(params.get("deltaBase64"), str):
        try:
            delta = base64.b64decode(str(params["deltaBase64"]).encode("ascii"), validate=True)
        except Exception as exc:
            raise RemoteControlError(f"invalid deltaBase64: {exc}") from exc
    if delta:
        if process.pty_master_fd is not None:
            try:
                os.write(process.pty_master_fd, delta)
            except OSError:
                pass
        else:
            stdin = process.popen.stdin
            if stdin is not None:
                try:
                    stdin.write(delta)
                    stdin.flush()
                except BrokenPipeError:
                    pass
    if params.get("closeStdin") is True and process.pty_master_fd is None:
        stdin = process.popen.stdin
        if stdin is not None:
            try:
                stdin.close()
            except OSError:
                pass


def _resize_remote_process_pty(process: _RemoteCommandProcess, size: Any) -> None:
    if process.pty_master_fd is None:
        return
    _set_pty_size(process.pty_master_fd, size)


def _set_pty_size(fd: int, size: Any) -> None:
    if not isinstance(size, dict):
        return
    rows = _optional_int(size.get("rows"))
    cols = _optional_int(size.get("cols"))
    if rows is None or cols is None:
        return
    if rows <= 0 or cols <= 0:
        raise RemoteControlError("PTY size rows and cols must be greater than 0")
    try:
        import fcntl
        import struct
        import termios

        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
    except OSError:
        pass


def _cap_process_chunk(chunk: bytes, observed: int, output_bytes_cap: int | None) -> tuple[bytes, int, bool]:
    if output_bytes_cap is None:
        return chunk, observed + len(chunk), False
    remaining = max(0, output_bytes_cap - observed)
    capped = chunk[:remaining]
    observed += len(capped)
    return capped, observed, observed >= output_bytes_cap


def _process_stream_cap_reached(process: _RemoteCommandProcess, stream_name: str) -> bool:
    return process.stdout_cap_reached if stream_name == "stdout" else process.stderr_cap_reached


def _decode_process_capture(chunks: list[bytes]) -> str:
    if not chunks:
        return ""
    return b"".join(chunks).decode("utf-8", errors="replace")


def _join_reader_threads(process: _RemoteCommandProcess) -> None:
    for thread in process.reader_threads:
        thread.join(timeout=2)


def _close_remote_process_fds(process: _RemoteCommandProcess) -> None:
    if process.pty_master_fd is not None:
        try:
            os.close(process.pty_master_fd)
        except OSError:
            pass
        process.pty_master_fd = None


def _command_exec_response(params: dict[str, Any], config: RemoteControlConfig) -> dict[str, Any]:
    command = _command_exec_argv(params)
    cwd = _command_exec_cwd(params, config)
    timeout_ms = params.get("timeoutMs")
    timeout = None
    if isinstance(timeout_ms, (int, float)) and timeout_ms >= 0:
        timeout = timeout_ms / 1000
    if params.get("disableTimeout") is True:
        timeout = None
    env = _command_exec_env(params)
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return {"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    except subprocess.TimeoutExpired as exc:
        return {
            "exitCode": 124,
            "stdout": exc.stdout if isinstance(exc.stdout, str) else "",
            "stderr": exc.stderr if isinstance(exc.stderr, str) else "command timed out",
        }


def _command_exec_is_streaming(params: dict[str, Any]) -> bool:
    return bool(params.get("tty") or params.get("streamStdin") or params.get("streamStdoutStderr"))


def _command_exec_output_bytes_cap(params: dict[str, Any]) -> int | None:
    if params.get("disableOutputCap") is True:
        return None
    cap = params.get("outputBytesCap")
    if isinstance(cap, int) and cap >= 0:
        return cap
    return DEFAULT_REMOTE_PROCESS_OUTPUT_BYTES_CAP


def _command_exec_argv(params: dict[str, Any]) -> list[str]:
    command = params.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(part, str) for part in command):
        raise RemoteControlError("command/exec requires a non-empty command array")
    return list(command)


def _command_exec_cwd(params: dict[str, Any], config: RemoteControlConfig) -> Path:
    cwd_raw = params.get("cwd")
    cwd = Path(cwd_raw).expanduser() if isinstance(cwd_raw, str) and cwd_raw else config.cwd
    return cwd if cwd.is_absolute() else cwd.resolve()


def _command_exec_env(params: dict[str, Any]) -> dict[str, str]:
    env = os.environ.copy()
    env_params = params.get("env")
    if isinstance(env_params, dict):
        for key, value in env_params.items():
            if not isinstance(key, str):
                continue
            if value is None:
                env.pop(key, None)
            elif isinstance(value, str):
                env[key] = value
    return env


def _process_spawn_argv(params: dict[str, Any]) -> list[str]:
    command = params.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(part, str) for part in command):
        raise RemoteControlError("command must not be empty")
    return list(command)


def _process_spawn_cwd(params: dict[str, Any], config: RemoteControlConfig) -> Path:
    cwd_raw = params.get("cwd")
    cwd = Path(cwd_raw).expanduser() if isinstance(cwd_raw, str) and cwd_raw else config.cwd
    return cwd if cwd.is_absolute() else cwd.resolve()


def _process_spawn_output_bytes_cap(params: dict[str, Any]) -> int | None:
    if "outputBytesCap" not in params:
        return DEFAULT_REMOTE_PROCESS_OUTPUT_BYTES_CAP
    cap = params.get("outputBytesCap")
    if cap is None:
        return None
    if isinstance(cap, int) and cap >= 0:
        return cap
    raise RemoteControlError("process/spawn outputBytesCap must be non-negative")


def _terminate_process(popen: subprocess.Popen[Any]) -> None:
    if popen.poll() is not None:
        return
    popen.terminate()
    try:
        popen.wait(timeout=2)
    except subprocess.TimeoutExpired:
        popen.kill()


def _terminate_remote_process(process: _RemoteCommandProcess) -> None:
    _terminate_process(process.popen)
    _close_remote_process_fds(process)


def _goal_status_from_param(value: Any) -> str | None:
    if value is None:
        return None
    from .goal import GOAL_STATUS_FROM_WIRE

    key = str(value)
    return GOAL_STATUS_FROM_WIRE.get(key, key)


def _rate_limit_snapshot_payload(snapshot: Any) -> dict[str, Any]:
    return {
        "limitId": snapshot.limit_id,
        "limitName": snapshot.limit_name,
        "primary": _rate_limit_window_payload(snapshot.primary),
        "secondary": _rate_limit_window_payload(snapshot.secondary),
        "credits": _credits_payload(snapshot.credits),
        "planType": snapshot.plan_type,
        "rateLimitReachedType": snapshot.rate_limit_reached_type,
    }


def _rate_limit_window_payload(window: Any) -> dict[str, Any] | None:
    if window is None:
        return None
    return {
        "usedPercent": float(window.used_percent),
        "windowDurationMins": window.window_minutes,
        "resetsAt": window.resets_at,
    }


def _credits_payload(credits: Any) -> dict[str, Any] | None:
    if credits is None:
        return None
    return {"hasCredits": credits.has_credits, "unlimited": credits.unlimited, "balance": credits.balance}


def _empty_rate_limit_snapshot() -> dict[str, Any]:
    return {
        "limitId": "codex",
        "limitName": None,
        "primary": None,
        "secondary": None,
        "credits": None,
        "planType": None,
        "rateLimitReachedType": None,
    }


def _jsonrpc_error(request_id: Any, message: str, *, code: int = -32000) -> dict[str, Any]:
    return {"id": request_id, "error": {"code": code, "message": message}}


def _remote_log(event: str, **fields: Any) -> None:
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "event": event,
        **{key: value for key, value in fields.items() if value is not None},
    }
    print(f"remote-control {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}", flush=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _turn_payload(
    turn_id: str,
    *,
    status: str,
    started_at: int | None = None,
    completed_at: int | None = None,
    error: str | None = None,
    items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    duration_ms = None
    if started_at is not None and completed_at is not None:
        duration_ms = max(0, int((completed_at - started_at) * 1000))
    payload: dict[str, Any] = {
        "id": turn_id,
        "items": items or [],
        "itemsView": "full" if items else "notLoaded",
        "status": status,
        "error": (
            {
                "message": error,
                "codexErrorInfo": None,
                "additionalDetails": None,
            }
            if error and status == "failed"
            else None
        ),
        "startedAt": started_at,
        "completedAt": completed_at,
        "durationMs": duration_ms,
    }
    return payload


def _thread_payload(
    *,
    thread_id: str,
    session_id: str,
    cwd: str,
    model_provider: str,
    source: Any,
    preview: str,
    path: str | None,
    status: dict[str, Any],
    turns: list[dict[str, Any]],
    created_at: int,
    updated_at: int,
    ephemeral: bool,
    forked_from_id: str | None = None,
    cli_version: str = PYTHON_REMOTE_CONTROL_VERSION,
    name: str | None = None,
    git_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": thread_id,
        "sessionId": session_id,
        "forkedFromId": forked_from_id,
        "preview": preview,
        "ephemeral": ephemeral,
        "modelProvider": model_provider,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "status": status,
        "path": path,
        "cwd": cwd,
        "cliVersion": cli_version,
        "source": source,
        "threadSource": None,
        "agentNickname": None,
        "agentRole": None,
        "gitInfo": git_info,
        "name": name,
        "turns": turns,
    }


_DEFAULT_INTERACTIVE_SESSION_SOURCE_STRINGS = {"cli", "vscode"}
_DEFAULT_INTERACTIVE_CUSTOM_SESSION_SOURCES = {"atlas", "chatgpt"}


def _api_session_source(source: Any) -> Any:
    """Return the app-server protocol SessionSource value for Python metadata.

    Early Python remote-control builds used the non-upstream string
    ``"appServer"``. Keep those legacy rollouts readable, but expose them as the
    official interactive CLI source on the wire.
    """
    if source == "appServer":
        return "cli"
    return source


def _source_kinds_filter(value: Any) -> tuple[str, ...] | None:
    if not isinstance(value, list) or not value:
        return None
    return tuple(str(item) for item in value if isinstance(item, str) and item)


def _thread_source_matches(source: Any, source_kinds: tuple[str, ...] | None) -> bool:
    source = _api_session_source(source)
    if source_kinds is None:
        return _is_default_interactive_session_source(source)
    return any(_thread_source_kind_matches(source, kind) for kind in source_kinds)


def _is_default_interactive_session_source(source: Any) -> bool:
    if isinstance(source, str):
        return source in _DEFAULT_INTERACTIVE_SESSION_SOURCE_STRINGS
    if isinstance(source, dict):
        custom = source.get("custom")
        return isinstance(custom, str) and custom in _DEFAULT_INTERACTIVE_CUSTOM_SESSION_SOURCES
    return False


def _thread_source_kind_matches(source: Any, source_kind: str) -> bool:
    if isinstance(source, str):
        if source_kind == "cli":
            return source == "cli"
        if source_kind == "vscode":
            return source == "vscode"
        if source_kind == "exec":
            return source == "exec"
        if source_kind == "appServer":
            return source in {"mcp", "appServer"}
        if source_kind == "unknown":
            return source == "unknown"
        return False
    if isinstance(source, dict):
        if source_kind == "subAgent":
            return "subagent" in source
        if source_kind == "subAgentReview":
            return source.get("subagent") == "review"
        if source_kind == "subAgentCompact":
            return source.get("subagent") == "compact"
        if source_kind == "subAgentThreadSpawn":
            subagent = source.get("subagent")
            return isinstance(subagent, dict) and "thread_spawn" in subagent
        if source_kind == "subAgentOther":
            subagent = source.get("subagent")
            return isinstance(subagent, dict) and "other" in subagent
    return False


def _thread_payload_from_rollout(path: Path, config: RemoteControlConfig, *, include_turns: bool = True) -> dict[str, Any] | None:
    try:
        records = load_rollout_records(path)
        reconstruction = reconstruct_history_from_rollout(records, CodexConfig(codex_home=config.codex_home))
    except Exception:
        return None
    meta = reconstruction.session_meta or {}
    thread_id = str(meta.get("id") or _thread_id_from_rollout_path(path) or path.stem)
    cwd = str(meta.get("cwd") or config.cwd)
    timestamp = _timestamp_to_seconds(meta.get("timestamp")) or int(path.stat().st_mtime)
    updated_at = int(path.stat().st_mtime)
    turns: list[dict[str, Any]] = []
    if include_turns:
        turns = _turns_from_rollout_records(records)
        if not turns:
            items = _thread_items_from_response_history(reconstruction.history)
            turns = [_turn_payload("history", status="completed", items=items)] if items else []
    return _thread_payload(
        thread_id=thread_id,
        session_id=str(meta.get("session_id") or thread_id),
        forked_from_id=_optional_string(meta.get("forked_from_id")),
        cwd=cwd,
        model_provider=str(meta.get("model_provider") or config.model or "openai"),
        source=_api_session_source(meta.get("source") or "cli"),
        preview=_preview_from_history(reconstruction.history),
        path=str(path),
        status={"type": "idle"},
        turns=turns,
        created_at=timestamp,
        updated_at=updated_at,
        ephemeral=False,
        cli_version=str(meta.get("cli_version") or PYTHON_REMOTE_CONTROL_VERSION),
    )


def _turns_from_history(session: CodexSession) -> list[dict[str, Any]]:
    rollout_path = getattr(session.state, "_rollout_path", None)
    if isinstance(rollout_path, Path) and rollout_path.exists():
        turns = _turns_from_rollout_records(load_rollout_records(rollout_path))
        if turns:
            return turns
    compact_items = _thread_items_from_response_history(session.state.history)
    if not compact_items:
        return []
    return [_turn_payload(session.state.turn_id, status="completed", items=compact_items)]


def _thread_items_from_response_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    command_items_by_call_id: dict[str, dict[str, Any]] = {}
    for response_item in history:
        if not isinstance(response_item, dict):
            continue
        if response_item.get("type") == "function_call_output":
            call_id = str(response_item.get("call_id") or "")
            command_item = command_items_by_call_id.get(call_id)
            if command_item is not None:
                output = str(response_item.get("output") or "")
                command_item["aggregatedOutput"] = output or None
                command_item["status"] = "completed"
            continue
        item = _thread_item_from_response_item(response_item)
        if item is None:
            continue
        items.append(item)
        if item.get("type") == "commandExecution":
            command_items_by_call_id[str(item.get("id") or "")] = item
    return items


def _thread_item_from_response_item(item: Any, *, item_id: Any | None = None) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    response_type = item.get("type")
    item_id_str = str(item_id or item.get("id") or f"item_{uuid.uuid4()}")
    if response_type == "message":
        role = item.get("role")
        text = _message_text(item)
        if role == "user":
            if _is_remote_contextual_user_message(item):
                return None
            return {
                "type": "userMessage",
                "id": item_id_str,
                "content": [{"type": "text", "text": text, "text_elements": []}],
            }
        if role == "assistant":
            return {
                "type": "agentMessage",
                "id": item_id_str,
                "text": text,
                "phase": None,
                "memoryCitation": None,
            }
    if response_type in {"function_call", "custom_tool_call"}:
        tool = str(item.get("name") or item.get("call_id") or "tool")
        arguments = _json_tool_arguments(item.get("arguments") or item.get("input") or {})
        if tool in {"exec_command", "shell_command"}:
            command, cwd = _command_from_tool_arguments(arguments)
            if command:
                return _command_execution_item(
                    str(item.get("call_id") or item_id_str),
                    command=command,
                    cwd=cwd or str(Path.cwd()),
                    source="agent",
                    status=_command_status_from_response_item(item),
                )
        return {
            "type": "dynamicToolCall",
            "id": item_id_str,
            "namespace": None,
            "tool": tool,
            "arguments": arguments,
            "status": "completed",
            "contentItems": None,
            "success": None,
            "durationMs": None,
        }
    return None


def _command_execution_item(
    item_id: str,
    *,
    command: str,
    cwd: str,
    status: str,
    source: str = "userShell",
    process_id: str | None = None,
    command_actions: list[dict[str, Any]] | None = None,
    aggregated_output: str | None = None,
    exit_code: int | None = None,
    duration_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "type": "commandExecution",
        "id": item_id,
        "command": command,
        "cwd": cwd,
        "processId": process_id,
        "source": source,
        "status": status,
        "commandActions": command_actions or [],
        "aggregatedOutput": aggregated_output,
        "exitCode": exit_code,
        "durationMs": duration_ms,
    }


def _turns_from_rollout_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    turns: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    item_positions: dict[str, int] = {}
    turn_id = f"history_{len(turns)}"
    started_at: int | None = None
    completed_at: int | None = None
    turn_open = False

    def start_turn(new_turn_id: str | None = None, *, new_started_at: int | None = None) -> None:
        nonlocal items, item_positions, turn_id, started_at, completed_at, turn_open
        if turn_open and items:
            finish_turn()
        items = []
        item_positions = {}
        turn_id = new_turn_id or f"history_{len(turns)}"
        started_at = new_started_at
        completed_at = None
        turn_open = True

    def ensure_turn() -> None:
        if not turn_open:
            start_turn()

    def finish_turn(*, status: str = "completed") -> None:
        nonlocal items, item_positions, turn_open, completed_at, started_at, turn_id
        if not items:
            turn_open = False
            return
        turns.append(
            _turn_payload(
                turn_id,
                status=status,
                items=items,
                started_at=started_at,
                completed_at=completed_at,
            )
        )
        items = []
        item_positions = {}
        turn_open = False
        started_at = None
        completed_at = None
        turn_id = f"history_{len(turns)}"

    def append_item(item: dict[str, Any] | None) -> None:
        if item is None:
            return
        ensure_turn()
        item_id = str(item.get("id") or "")
        if item_id and item_id in item_positions:
            items[item_positions[item_id]] = item
            return
        if item_id:
            item_positions[item_id] = len(items)
        items.append(item)

    for index, record in enumerate(records):
        if record.get("type") != "event_msg":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        event_type = str(payload.get("type") or "")
        if event_type in {"task_started", "turn_started"}:
            start_turn(
                str(payload.get("turn_id") or f"history_{len(turns)}"),
                new_started_at=_optional_int(payload.get("started_at")),
            )
            continue
        if event_type == "user_message":
            text = str(payload.get("message") or "")
            user_item = {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}],
            }
            append_item(_thread_item_from_response_item(user_item, item_id=f"event_{index}"))
            continue
        if event_type == "agent_message":
            text = str(payload.get("message") or "")
            if text:
                append_item(
                    {
                        "type": "agentMessage",
                        "id": f"event_{index}",
                        "text": text,
                        "phase": payload.get("phase"),
                        "memoryCitation": payload.get("memory_citation"),
                    }
                )
            continue
        if event_type in {"exec_command_begin", "exec_command_end"}:
            append_item(_command_execution_item_from_event(payload))
            continue
        if event_type in {"patch_apply_begin", "patch_apply_end"}:
            append_item(_file_change_item_from_event(payload))
            continue
        if event_type in {"web_search_begin", "web_search_end"}:
            append_item(_web_search_item_from_event(payload))
            continue
        if event_type == "plan_update":
            append_item(_plan_item_from_event(payload, item_id=f"event_{index}"))
            continue
        if event_type == "view_image_tool_call":
            path = _optional_string(payload.get("path"))
            if path:
                append_item({"type": "imageView", "id": str(payload.get("call_id") or f"event_{index}"), "path": path})
            continue
        if event_type in {"context_compacted", "context_compaction"}:
            append_item({"type": "contextCompaction", "id": str(payload.get("call_id") or f"event_{index}")})
            continue
        if event_type in {"task_complete", "turn_complete"}:
            completed_at = _optional_int(payload.get("completed_at")) or completed_at
            finish_turn(status="completed")
            continue

    if turn_open and items:
        finish_turn(status="completed")
    return turns


def _plan_item_from_event(payload: dict[str, Any], *, item_id: str) -> dict[str, Any] | None:
    lines: list[str] = []
    explanation = payload.get("explanation")
    if isinstance(explanation, str) and explanation:
        lines.append(explanation)
    plan = payload.get("plan")
    if isinstance(plan, list):
        for entry in plan:
            if not isinstance(entry, dict):
                continue
            step = str(entry.get("step") or "").strip()
            status = str(entry.get("status") or "").strip()
            if step:
                lines.append(f"- [{status}] {step}" if status else f"- {step}")
    text = "\n".join(lines).strip()
    if not text:
        return None
    return {"type": "plan", "id": item_id, "text": text}


def _web_search_item_from_event(payload: dict[str, Any]) -> dict[str, Any] | None:
    call_id = str(payload.get("call_id") or "")
    if not call_id:
        return None
    action = payload.get("action")
    if not isinstance(action, dict):
        action = None
    query = payload.get("query")
    if not isinstance(query, str):
        query = action.get("query") if isinstance(action, dict) else ""
    return {
        "type": "webSearch",
        "id": call_id,
        "query": query if isinstance(query, str) else "",
        "action": action,
    }


def _file_change_item_from_event(payload: dict[str, Any]) -> dict[str, Any] | None:
    call_id = str(payload.get("call_id") or "")
    if not call_id:
        return None
    changes = _file_update_changes(payload.get("changes"))
    if not changes:
        return None
    status = str(payload.get("status") or ("completed" if payload.get("type") == "patch_apply_end" else "inProgress"))
    success = payload.get("success")
    if isinstance(success, bool):
        status = "completed" if success else "failed"
    return {
        "type": "fileChange",
        "id": call_id,
        "changes": changes,
        "status": status,
    }


def _file_update_changes(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [change for change in (_normalize_file_update_change(item) for item in value) if change is not None]
    if isinstance(value, dict):
        changes: list[dict[str, Any]] = []
        for path, change in value.items():
            if not isinstance(path, str) or not isinstance(change, dict):
                continue
            changes.append(_file_update_change_from_patch_change(path, change))
        return [change for change in changes if change is not None]
    return []


def _normalize_file_update_change(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    path = item.get("path")
    if not isinstance(path, str) or not path:
        return None
    kind = item.get("kind")
    if not isinstance(kind, dict):
        kind = _patch_change_kind(item.get("type"), item.get("move_path"))
    if kind is None:
        return None
    return {"path": path, "kind": kind, "diff": str(item.get("diff") or item.get("content") or item.get("unified_diff") or "")}


def _file_update_change_from_patch_change(path: str, change: dict[str, Any]) -> dict[str, Any] | None:
    change_type = change.get("type")
    kind = _patch_change_kind(change_type, change.get("move_path"))
    if kind is None:
        return None
    diff = str(change.get("unified_diff") or change.get("content") or change.get("diff") or "")
    return {"path": path, "kind": kind, "diff": diff}


def _patch_change_kind(change_type: Any, move_path: Any = None) -> dict[str, Any] | None:
    if change_type == "add":
        return {"type": "add"}
    if change_type == "delete":
        return {"type": "delete"}
    if change_type == "update":
        return {"type": "update", "move_path": move_path if isinstance(move_path, str) else None}
    return None


def _command_execution_item_from_event(payload: dict[str, Any]) -> dict[str, Any] | None:
    call_id = str(payload.get("call_id") or "")
    if not call_id:
        return None
    command = _command_string(payload.get("command"))
    if not command:
        return None
    status = str(payload.get("status") or ("completed" if payload.get("type") == "exec_command_end" else "inProgress"))
    return _command_execution_item(
        call_id,
        command=command,
        cwd=str(payload.get("cwd") or Path.cwd()),
        process_id=_optional_string(payload.get("process_id")),
        source=str(payload.get("source") or "agent"),
        status=status,
        command_actions=_command_actions_from_payload(payload.get("parsed_cmd"), command),
        aggregated_output=_optional_string(payload.get("aggregated_output")),
        exit_code=_optional_int(payload.get("exit_code")),
        duration_ms=_duration_ms_from_event_payload(payload),
    )


def _command_actions_from_payload(parsed_cmd: Any, command: str) -> list[dict[str, Any]]:
    if not isinstance(parsed_cmd, list) or not parsed_cmd:
        return []
    actions: list[dict[str, Any]] = []
    for item in parsed_cmd:
        if not isinstance(item, dict):
            continue
        action_type = str(item.get("type") or "unknown")
        action_command = str(item.get("cmd") or item.get("command") or command)
        if action_type == "unknown":
            actions.append({"type": "unknown", "command": action_command})
    return actions


def _duration_ms_from_event_payload(payload: dict[str, Any]) -> int | None:
    direct = _optional_int(payload.get("duration_ms"))
    if direct is not None:
        return direct
    duration = payload.get("duration")
    if isinstance(duration, dict):
        secs = _optional_int(duration.get("secs")) or 0
        nanos = _optional_int(duration.get("nanos")) or 0
        return max(0, secs * 1000 + nanos // 1_000_000)
    return None


def _json_tool_arguments(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _command_from_tool_arguments(arguments: Any) -> tuple[str, str | None]:
    if not isinstance(arguments, dict):
        return str(arguments or ""), None
    command = arguments.get("cmd")
    if command is None:
        command = arguments.get("command")
    cwd = arguments.get("workdir")
    if cwd is None:
        cwd = arguments.get("cwd")
    return _command_string(command), str(cwd) if isinstance(cwd, str) and cwd else None


def _command_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        if len(value) == 1:
            return str(value[0])
        return shlex.join(str(part) for part in value)
    return ""


def _command_status_from_response_item(item: dict[str, Any]) -> str:
    status = str(item.get("status") or "").lower()
    if status in {"failed", "incomplete", "error"}:
        return "failed"
    if status in {"in_progress", "inprogress", "running"}:
        return "inProgress"
    if status == "declined":
        return "declined"
    return "completed"


def _last_user_message_index(history: list[dict[str, Any]]) -> int | None:
    for index in range(len(history) - 1, -1, -1):
        item = history[index]
        if (
            isinstance(item, dict)
            and item.get("type") == "message"
            and item.get("role") == "user"
            and not _is_remote_contextual_user_message(item)
        ):
            return index
    return None


def _is_remote_contextual_user_message(item: dict[str, Any]) -> bool:
    if item.get("type") != "message" or item.get("role") != "user":
        return False
    content = item.get("content")
    if not isinstance(content, list) or not content:
        return False
    saw_context = False
    for part in content:
        if not isinstance(part, dict):
            return False
        text = part.get("text")
        if not isinstance(text, str):
            return False
        stripped = text.strip()
        if (
            stripped.startswith("# AGENTS.md instructions for ")
            or (stripped.startswith("<environment_context>") and stripped.endswith("</environment_context>"))
            or (stripped.startswith("<turn_aborted>") and stripped.endswith("</turn_aborted>"))
            or (stripped.startswith("<hook_context>") and stripped.endswith("</hook_context>"))
            or (stripped.startswith("<subagent_notification>") and stripped.endswith("</subagent_notification>"))
            or (stripped.startswith("<goal_context>") and stripped.endswith("</goal_context>"))
        ):
            saw_context = True
            continue
        return False
    return saw_context


def _message_text(item: dict[str, Any]) -> str:
    content = item.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                value = part.get("text") or part.get("content")
                if isinstance(value, str):
                    parts.append(value)
        return "".join(parts)
    return ""


def _input_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                item_type = item.get("type")
                if item_type in {"text", "input_text"} and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif item_type == "userMessage":
                    parts.append(_input_text(item.get("content")))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        if isinstance(value.get("text"), str):
            return value["text"]
        return _input_text(value.get("content"))
    return ""


def _preview_from_history(history: list[dict[str, Any]]) -> str:
    for item in history:
        if (
            isinstance(item, dict)
            and item.get("type") == "message"
            and item.get("role") == "user"
            and not _is_remote_contextual_user_message(item)
        ):
            text = _message_text(item).strip()
            if text:
                return text[:160]
    return ""


def _rollout_paths(codex_home: Path) -> list[Path]:
    sessions = codex_home / "sessions"
    if not sessions.exists():
        return []
    return sorted(sessions.glob("**/rollout-*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)


def _find_rollout_path(codex_home: Path, thread_id: str) -> Path | None:
    for path in _rollout_paths(codex_home):
        if thread_id in path.name:
            return path
        try:
            records = load_rollout_records(path)
            for record in records[:4]:
                payload = record.get("payload")
                meta = payload.get("meta") if isinstance(payload, dict) else None
                if isinstance(meta, dict) and meta.get("id") == thread_id:
                    return path
        except Exception:
            continue
    return None


def _thread_id_from_rollout_path(path: Path) -> str | None:
    name = path.name
    if not name.startswith("rollout-") or not name.endswith(".jsonl"):
        return None
    stem = name[:-6]
    parts = stem.split("-")
    if len(parts) < 8:
        return None
    return "-".join(parts[-5:])


def _cwd_filter(value: Any) -> set[str]:
    if isinstance(value, str) and value:
        return {str(Path(value).expanduser().resolve())}
    if isinstance(value, list):
        return {str(Path(item).expanduser().resolve()) for item in value if isinstance(item, str) and item}
    return set()


def _timestamp_to_seconds(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(time.mktime(time.strptime(value.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z")))
    except Exception:
        try:
            return int(time.mktime(time.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")))
        except Exception:
            return None


def _seconds_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))


def _token_usage_payload(usage: dict[str, Any]) -> dict[str, Any]:
    last = _token_breakdown(usage.get("last") if isinstance(usage.get("last"), dict) else usage)
    total_source = usage.get("total") if isinstance(usage.get("total"), dict) else usage
    return {
        "total": _token_breakdown(total_source),
        "last": last,
        "modelContextWindow": usage.get("model_context_window") or usage.get("modelContextWindow"),
    }


def _token_breakdown(usage: Any) -> dict[str, int]:
    usage = usage if isinstance(usage, dict) else {}
    input_tokens = _int_value(usage.get("input_tokens") or usage.get("inputTokens"))
    output_tokens = _int_value(usage.get("output_tokens") or usage.get("outputTokens"))
    cached = _int_value(usage.get("cached_input_tokens") or usage.get("cachedInputTokens"))
    reasoning = _int_value(usage.get("reasoning_output_tokens") or usage.get("reasoningOutputTokens"))
    total = _int_value(usage.get("total_tokens") or usage.get("totalTokens")) or input_tokens + output_tokens
    return {
        "totalTokens": total,
        "inputTokens": input_tokens,
        "cachedInputTokens": cached,
        "outputTokens": output_tokens,
        "reasoningOutputTokens": reasoning,
    }


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _enrollment_key(websocket_url: str, account_id: str, app_server_client_name: str | None) -> str:
    suffix = app_server_client_name or ""
    return f"{websocket_url}\n{account_id}\n{suffix}"


def _remote_control_start_human_lines(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
) -> list[str]:
    _ensure_remote_control_startable(status)
    if status.status == "connected":
        lines = [f"This machine is available for remote control as {status.server_name}."]
    else:
        lines = [f"Remote control is enabled on {status.server_name} and still connecting."]
    if mode == "foreground":
        lines.append("Press Ctrl-C to stop.")
    return lines


def _ensure_remote_control_startable(status: RemoteControlReadyStatus) -> None:
    if status.status in {"connected", "connecting"}:
        return
    if status.status == "errored":
        raise RemoteControlError(
            f"Remote control is enabled on {status.server_name} but the connection is errored."
        )
    raise RemoteControlError(f"Remote control is disabled on {status.server_name}.")


def _ensure_trailing_path_slash(parsed: ParseResult) -> ParseResult:
    path = parsed.path or "/"
    if not path.endswith("/"):
        path = f"{path}/"
    return parsed._replace(path=path)


def _is_allowed_chatgpt_host(host: str | None) -> bool:
    if not host:
        return False
    host = host.lower()
    return (
        host == "chatgpt.com"
        or host == "chatgpt-staging.com"
        or host.endswith(".chatgpt.com")
        or host.endswith(".chatgpt-staging.com")
    )


def _is_localhost(host: str | None) -> bool:
    if not host:
        return False
    host = host.lower()
    if host == "localhost":
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _invalid_remote_control_url_message(remote_control_url: str) -> str:
    return (
        f"invalid remote control URL `{remote_control_url}`; expected HTTPS URL for "
        "chatgpt.com or chatgpt-staging.com, or HTTP/HTTPS URL for localhost"
    )


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: Any) -> int | None:
    return value if isinstance(value, int) else None


__all__ = [
    "ClientEnvelope",
    "DEFAULT_REMOTE_CONTROL_BASE_URL",
    "EnrollRemoteServerRequest",
    "REMOTE_CONTROL_ACCOUNT_ID_HEADER",
    "REMOTE_CONTROL_INSTALLATION_ID_HEADER",
    "REMOTE_CONTROL_PROTOCOL_VERSION",
    "REMOTE_CONTROL_SUBSCRIBE_CURSOR_HEADER",
    "RemoteClientEvent",
    "RemoteControlError",
    "RemoteControlAuth",
    "RemoteControlConfig",
    "RemoteControlEnrollment",
    "RemoteControlReadyStatus",
    "RemoteControlStartJsonOutput",
    "RemoteControlTarget",
    "RemoteServerEvent",
    "ServerEnvelope",
    "build_enroll_request",
    "normalize_remote_control_url",
    "remote_control_official_args",
    "remote_control_start_human_lines",
    "remote_control_start_json_output",
    "remote_control_stop_human_message",
    "run_native_remote_control",
]
