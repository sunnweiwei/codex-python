from __future__ import annotations

import json
import os
import socket
import threading
from pathlib import Path
from typing import TYPE_CHECKING

from .constants import (
    APP_SERVER_CONTROL_SOCKET_DIR_NAME,
    APP_SERVER_CONTROL_SOCKET_FILE_NAME,
    APP_SERVER_STARTUP_LOCK_FILE_NAME,
)
from .protocol import ClientEnvelope
from .types import RemoteControlError
from .utils import _remote_log

if TYPE_CHECKING:
    from .service import RemoteControlService


def app_server_control_socket_path(codex_home: Path) -> Path:
    return codex_home / APP_SERVER_CONTROL_SOCKET_DIR_NAME / APP_SERVER_CONTROL_SOCKET_FILE_NAME


def app_server_startup_lock_path(codex_home: Path) -> Path:
    return codex_home / APP_SERVER_CONTROL_SOCKET_DIR_NAME / APP_SERVER_STARTUP_LOCK_FILE_NAME


def app_server_control_socket_available(codex_home: Path, *, timeout_seconds: float = 0.1) -> bool:
    socket_path = app_server_control_socket_path(codex_home)
    if not socket_path.exists():
        return False
    if not hasattr(socket, "AF_UNIX"):
        return False
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.settimeout(timeout_seconds)
        client.connect(str(socket_path))
        return True
    except OSError:
        return False
    finally:
        try:
            client.close()
        except OSError:
            pass


class _LocalControlConnection:
    def __init__(self, service: "RemoteControlService", conn: socket.socket, address: str):
        self.service = service
        self.conn = conn
        self.address = address
        self._send_lock = threading.Lock()
        self._closed = threading.Event()
        self._client_streams: set[tuple[str, str]] = set()

    def send(self, payload: str) -> None:
        if self._closed.is_set():
            raise OSError("local app-server control connection is closed")
        data = payload.encode("utf-8") + b"\n"
        with self._send_lock:
            self.conn.sendall(data)

    def run(self) -> None:
        try:
            reader = self.conn.makefile("rb")
            for raw_line in reader:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    envelope = ClientEnvelope.from_wire(payload)
                except Exception as exc:
                    _remote_log("local_control_invalid_message", error=str(exc))
                    continue
                stream_id = self.service._handle_client_envelope(self, envelope)
                if stream_id is not None:
                    self._client_streams.add((envelope.client_id, stream_id))
        finally:
            self.close()

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self.conn.close()
        except OSError:
            pass
        for client_id, stream_id in list(self._client_streams):
            self.service._close_client_transport(client_id, stream_id)
            self.service._server.close_client(client_id, stream_id)


class _LocalControlSocketServer:
    def __init__(self, service: "RemoteControlService", socket_path: Path):
        self.service = service
        self.socket_path = socket_path
        self._stop = threading.Event()
        self._server_socket: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._connections: set[_LocalControlConnection] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        if not hasattr(socket, "AF_UNIX"):
            return
        self.socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._remove_stale_socket_if_needed()
        server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            server_socket.bind(str(self.socket_path))
            try:
                os.chmod(self.socket_path, 0o600)
            except OSError:
                pass
            server_socket.listen(16)
            server_socket.settimeout(0.25)
        except Exception:
            server_socket.close()
            raise
        self._server_socket = server_socket
        self._thread = threading.Thread(target=self._run, name="codex-app-server-control", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        server_socket = self._server_socket
        if server_socket is not None:
            try:
                server_socket.close()
            except OSError:
                pass
        with self._lock:
            connections = list(self._connections)
        for connection in connections:
            connection.close()
        try:
            if self.socket_path.exists():
                self.socket_path.unlink()
        except OSError:
            pass

    def _run(self) -> None:
        server_socket = self._server_socket
        if server_socket is None:
            return
        while not self._stop.is_set():
            try:
                conn, _addr = server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            connection = _LocalControlConnection(self.service, conn, str(self.socket_path))
            with self._lock:
                self._connections.add(connection)
            thread = threading.Thread(
                target=self._run_connection,
                args=(connection,),
                name="codex-app-server-control-client",
                daemon=True,
            )
            thread.start()

    def _run_connection(self, connection: _LocalControlConnection) -> None:
        try:
            connection.run()
        finally:
            with self._lock:
                self._connections.discard(connection)

    def _remove_stale_socket_if_needed(self) -> None:
        if not self.socket_path.exists():
            return
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.1)
            probe.connect(str(self.socket_path))
            raise RemoteControlError(
                f"app-server control socket is already in use at {self.socket_path}"
            )
        except (ConnectionRefusedError, FileNotFoundError, socket.timeout):
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
        except OSError:
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
        finally:
            try:
                probe.close()
            except OSError:
                pass
