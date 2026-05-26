from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from ..types import CodexConfig
from .constants import PYTHON_REMOTE_CONTROL_VERSION, REMOTE_CONTROL_SERVER_NAME_ENV
from .display import remote_control_start_human_lines, remote_control_start_json_output, remote_control_stop_human_message
from .local import app_server_control_socket_available, app_server_control_socket_path
from .pid import _clear_pid_file, _process_is_running, _read_pid_file, _remote_control_log_path, _write_pid_file
from .protocol import RemoteClientEvent, RemoteServerEvent
from .types import RemoteControlConfig, RemoteControlError, RemoteControlReadyStatus
from .utils import _codex_module_command, _optional_int

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
    from .service import RemoteControlService

    service = RemoteControlService(config)
    return service.run_foreground()


def ensure_remote_control_daemon(
    config: RemoteControlConfig,
    *,
    timeout_seconds: float = 5.0,
) -> bool:
    _start_remote_control_daemon(config, quiet=True)
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while time.monotonic() < deadline:
        if app_server_control_socket_available(config.codex_home):
            return True
        time.sleep(0.05)
    return app_server_control_socket_available(config.codex_home)



def _start_remote_control_daemon(config: RemoteControlConfig, *, quiet: bool = False) -> int:
    config.codex_home.mkdir(parents=True, exist_ok=True)
    pid = _read_pid_file(config)
    if pid is not None and _process_is_running(pid):
        if quiet:
            return 0
        return _print_daemon_start_status(config, pid=pid, log_path=_remote_control_log_path(config))

    log_path = _remote_control_log_path(config)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["CODEX_PY_HOME"] = str(config.codex_home)
    env[REMOTE_CONTROL_SERVER_NAME_ENV] = config.server_name
    if config.auth_codex_home is not None:
        env["CODEX_AUTH_HOME"] = str(config.auth_codex_home)
    command = _codex_module_command("remote-control")
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
    if quiet:
        return 0
    return _print_daemon_start_status(config, pid=process.pid, log_path=log_path)


def _print_daemon_start_status(config: RemoteControlConfig, *, pid: int, log_path: Path) -> int:
    status = _read_daemon_ready_status(config) or RemoteControlReadyStatus(
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


def _read_daemon_ready_status(config: RemoteControlConfig, *, timeout_seconds: float = 2.0) -> RemoteControlReadyStatus | None:
    if not hasattr(socket, "AF_UNIX"):
        return None
    socket_path = app_server_control_socket_path(config.codex_home)
    if not socket_path.exists():
        return None
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_id = f"daemon-status-{uuid.uuid4()}"
    stream_id = f"stream-{uuid.uuid4()}"
    next_seq_id = 1

    def send_event(event: dict[str, Any]) -> None:
        nonlocal next_seq_id
        payload = {
            "client_id": client_id,
            "stream_id": stream_id,
            "seq_id": next_seq_id,
            **event,
        }
        next_seq_id += 1
        client.sendall(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")

    def send_message(request_id: int, method: str, params: dict[str, Any] | None = None) -> None:
        send_event(
            {
                "type": RemoteClientEvent.CLIENT_MESSAGE.value,
                "message": {"id": request_id, "method": method, "params": params or {}},
            }
        )

    def ack_server_payload(payload: dict[str, Any]) -> None:
        seq_id = _optional_int(payload.get("seq_id"))
        if seq_id is None:
            return
        ack: dict[str, Any] = {"type": RemoteClientEvent.ACK.value, "seq_id": seq_id}
        segment_id = _optional_int(payload.get("segment_id"))
        if segment_id is not None:
            ack["segment_id"] = segment_id
        try:
            send_event(ack)
        except OSError:
            pass

    def read_response(reader: Any, request_id: int) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            client.settimeout(max(0.05, deadline - time.monotonic()))
            try:
                raw = reader.readline()
            except (OSError, TimeoutError):
                return None
            if not raw:
                return None
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            ack_server_payload(payload)
            if payload.get("type") != RemoteServerEvent.SERVER_MESSAGE.value:
                continue
            message = payload.get("message")
            if not isinstance(message, dict) or message.get("id") != request_id:
                continue
            result = message.get("result")
            return result if isinstance(result, dict) else {}
        return None

    try:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        reader = client.makefile("rb")
        send_message(
            1,
            "initialize",
            {
                "clientInfo": {"name": "codex_app_server_daemon", "version": PYTHON_REMOTE_CONTROL_VERSION},
                "capabilities": {
                    "experimentalApi": True,
                    "optOutNotificationMethods": ["thread/started"],
                },
            },
        )
        if read_response(reader, 1) is None:
            return None
        send_message(2, "remoteControl/status/read", {})
        result = read_response(reader, 2)
        if result is None:
            return None
        raw_status = result.get("status")
        status = raw_status if raw_status in {"disabled", "connecting", "connected", "errored"} else "connecting"
        server_name = result.get("serverName")
        environment_id = result.get("environmentId")
        return RemoteControlReadyStatus(
            status=status,  # type: ignore[arg-type]
            server_name=server_name if isinstance(server_name, str) and server_name else config.server_name,
            environment_id=environment_id if isinstance(environment_id, str) and environment_id else None,
            timed_out=False,
        )
    except Exception:
        return None
    finally:
        try:
            send_event({"type": RemoteClientEvent.CLIENT_CLOSED.value})
        except Exception:
            pass
        try:
            client.close()
        except OSError:
            pass


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
