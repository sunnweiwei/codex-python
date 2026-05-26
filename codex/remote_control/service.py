from __future__ import annotations

import json
import os
import sys
import threading
import uuid
from typing import Any

from .app_helpers import _initialize_client_name, _opt_out_notification_methods_from_initialize_params
from .app_server import _RemoteAppServer
from .constants import (
    REMOTE_CONTROL_RECONNECT_SECONDS,
    REMOTE_CONTROL_WEBSOCKET_CLIENT_PING_TIMEOUT_SECONDS,
    REMOTE_CONTROL_WEBSOCKET_PING_INTERVAL_SECONDS,
)
from .display import remote_control_start_human_lines, remote_control_start_json_output
from .local import _LocalControlSocketServer, app_server_control_socket_path
from .pid import _clear_pid_file, _write_pid_file
from .protocol import (
    ClientEnvelope,
    RemoteClientEvent,
    RemoteServerEvent,
    ServerEnvelope,
    _ClientSegmentReassembler,
    _OutboundBuffer,
    _split_server_envelope_for_transport,
    _remote_control_message_starts_connection,
)
from .transport import _RemoteControlPersistentState, _load_remote_control_auth, _websocket_headers, enroll_remote_control_server, normalize_remote_control_url
from . import trace
from .types import RemoteControlConnectionStatus, RemoteControlConfig, RemoteControlReadyStatus, RemoteControlUnavailable
from .utils import _effective_app_server_client_name, _optional_int, _remote_log


_REMOTE_CONTROL_IDENTITY_ENV = (
    "PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_NAME",
    "PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_VERSION",
    "PY_CODEX_REMOTE_CONTROL_ALLOW_DESKTOP_COMPAT",
    "PY_CODEX_REMOTE_CONTROL_USER_AGENT",
)


def _drop_remote_control_identity_env() -> None:
    for name in _REMOTE_CONTROL_IDENTITY_ENV:
        os.environ.pop(name, None)


class RemoteControlService:
    def __init__(self, config: RemoteControlConfig):
        self.config = config
        _drop_remote_control_identity_env()
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
        self._subscribe_cursor: str | None = None
        self._last_completed_client_chunk_seq_by_stream: dict[tuple[str, str | None], int] = {}
        self._client_streams: set[tuple[str, str]] = set()
        self._legacy_stream_ids: dict[str, str] = {}
        self._stream_connections: dict[tuple[str, str], Any] = {}
        self._notification_opt_outs: dict[tuple[str, str], set[str]] = {}
        self._local_control = _LocalControlSocketServer(self, app_server_control_socket_path(config.codex_home))

    def run_foreground(self) -> int:
        self.config.codex_home.mkdir(parents=True, exist_ok=True)
        _write_pid_file(self.config)
        self._local_control.start()
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
                    if not self.config.quiet:
                        print(f"Remote control connection failed: {exc}", file=sys.stderr, flush=True)
                    if not self._stop.wait(REMOTE_CONTROL_RECONNECT_SECONDS):
                        continue
            return 0
        finally:
            self._local_control.stop()
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
        pruned = self.state.prune_enrollments(
            self.target.websocket_url,
            auth.account_id,
            app_server_client_name,
            server_name=self.config.server_name,
        )
        if pruned:
            _remote_log(
                "pruned_stale_enrollments",
                count=pruned,
                required_server_name=self.config.server_name,
                app_server_client_name=app_server_client_name,
            )
        enrollment = self.state.enrollment(
            self.target.websocket_url,
            auth.account_id,
            app_server_client_name,
        )
        if enrollment is not None and enrollment.server_name != self.config.server_name:
            _remote_log(
                "discard_stale_enrollment",
                cached_server_name=enrollment.server_name,
                required_server_name=self.config.server_name,
                environment_id=enrollment.environment_id,
            )
            enrollment = None
        if enrollment is None:
            enrollment = enroll_remote_control_server(
                self.target,
                auth,
                installation_id=self.installation_id,
                server_name=self.config.server_name,
                app_server_client_name=app_server_client_name,
                app_server_client_version=self.config.app_server_client_version,
                allow_desktop_compat_identity=self.config.allow_desktop_compat_identity,
                user_agent_override=self.config.user_agent_override,
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
            subscribe_cursor=self._subscribe_cursor,
        )
        trace.append_event(
            {
                "event": "remote_control_websocket_request",
                "url": self.target.websocket_url,
                "server_id": enrollment.server_id,
                "server_name": enrollment.server_name,
                "environment_id": enrollment.environment_id,
                "account_id": auth.account_id,
                "installation_id": self.installation_id,
                "subscribe_cursor": self._subscribe_cursor,
                "headers": trace.headers_dict(headers),
            }
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
            ping_interval=REMOTE_CONTROL_WEBSOCKET_PING_INTERVAL_SECONDS,
            ping_timeout=REMOTE_CONTROL_WEBSOCKET_CLIENT_PING_TIMEOUT_SECONDS,
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
        elif not self.config.quiet:
            for line in remote_control_start_human_lines(ready, mode="foreground"):
                print(line, flush=True)

    def _on_error(self, error: Any) -> None:
        self.status = "errored"
        _remote_log("websocket_error", error=str(error) if error else "")
        if error and not self._stop.is_set() and not self.config.quiet:
            print(f"Remote control websocket error: {error}", file=sys.stderr, flush=True)

    def _on_close(self, code: Any, reason: Any) -> None:
        _remote_log("websocket_close", code=code, reason=reason)
        if not self._stop.is_set():
            self.status = "connecting"

    def _on_message(self, ws: Any, raw_message: str) -> None:
        trace.append_event(
            {
                "event": "remote_control_websocket_client_recv_raw",
                "wire_size_bytes": len(raw_message.encode("utf-8")),
                "payload": trace.payload_json(raw_message),
            }
        )
        try:
            payload = json.loads(raw_message)
            envelope = ClientEnvelope.from_wire(payload)
        except Exception as exc:
            print(f"Dropping invalid remote-control message: {exc}", file=sys.stderr, flush=True)
            return
        if envelope.cursor:
            self._subscribe_cursor = envelope.cursor
        stream_id = self._stream_id_for_envelope(envelope)
        if stream_id is None:
            return
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
        trace.append_event(
            {
                "event": "remote_control_websocket_client_forward",
                "payload": {**envelope.to_wire(), "stream_id": stream_id},
            }
        )
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
        if message.get("method") == "initialize":
            self._client_streams.add((envelope.client_id, stream_id))
            self._remember_legacy_stream_id(envelope, stream_id)
            self._stream_connections[(envelope.client_id, stream_id)] = ws
            self._notification_opt_outs[(envelope.client_id, stream_id)] = (
                _opt_out_notification_methods_from_initialize_params(message.get("params"))
            )
            _remote_log(
                "client_initialize",
                client_id=envelope.client_id,
                stream_id=stream_id,
                client_name=_initialize_client_name(message.get("params")),
                opt_out_notification_methods=sorted(self._notification_opt_outs[(envelope.client_id, stream_id)]),
            )
        self._server.handle_message(ws, envelope.client_id, stream_id, message)

    def _handle_client_envelope(self, ws: Any, envelope: ClientEnvelope) -> str | None:
        stream_id = self._stream_id_for_envelope(envelope)
        if stream_id is None:
            return None
        event = envelope.event
        event_type = event.get("type")
        message = event.get("message")
        method = message.get("method") if isinstance(message, dict) else None
        _remote_log(
            "local_client_event",
            event_type=event_type,
            method=method,
            client_id=envelope.client_id,
            stream_id=stream_id,
            seq_id=envelope.seq_id,
        )
        if event_type == RemoteClientEvent.ACK.value:
            if envelope.seq_id is not None:
                self._outbound_buffer.ack(
                    envelope.client_id,
                    stream_id,
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
            self._close_client_transport(envelope.client_id, stream_id)
            self._server.close_client(envelope.client_id, stream_id)
            return
        if event_type != RemoteClientEvent.CLIENT_MESSAGE.value or not isinstance(message, dict):
            return stream_id
        if message.get("method") == "initialize":
            self._client_streams.add((envelope.client_id, stream_id))
            self._remember_legacy_stream_id(envelope, stream_id)
            self._stream_connections[(envelope.client_id, stream_id)] = ws
            self._notification_opt_outs[(envelope.client_id, stream_id)] = (
                _opt_out_notification_methods_from_initialize_params(message.get("params"))
            )
            _remote_log(
                "local_client_initialize",
                client_id=envelope.client_id,
                stream_id=stream_id,
                client_name=_initialize_client_name(message.get("params")),
                opt_out_notification_methods=sorted(self._notification_opt_outs[(envelope.client_id, stream_id)]),
            )
        self._server.handle_message(ws, envelope.client_id, stream_id, message)
        return stream_id

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
        if self._notification_opted_out(client_id, stream_id, method):
            _remote_log(
                "notification_opted_out",
                method=method,
                client_id=client_id,
                stream_id=stream_id,
            )
            return
        self.send_message(ws, client_id, stream_id, {"method": method, "params": params})

    def initialized_client_refs(self) -> list[tuple[Any, str, str]]:
        refs: list[tuple[Any, str, str]] = []
        for client_id, stream_id in sorted(self._client_streams):
            ws = self._stream_connections.get((client_id, stream_id))
            if ws is not None:
                refs.append((ws, client_id, stream_id))
        return refs

    def _notification_opted_out(self, client_id: str, stream_id: str, method: str) -> bool:
        return method in self._notification_opt_outs.get((client_id, stream_id), set())

    def _send_event(self, ws: Any, client_id: str, stream_id: str, event: dict[str, Any]) -> None:
        seq_id = self._next_seq_id(client_id, stream_id)
        envelope = ServerEnvelope(client_id=client_id, stream_id=stream_id, seq_id=seq_id, event=event)
        for outbound in _split_server_envelope_for_transport(envelope):
            self._outbound_buffer.insert(outbound)
            self._send_wire_envelope(ws, outbound)

    def _send_wire_envelope(self, ws: Any, envelope: ServerEnvelope) -> None:
        payload = json.dumps(envelope.to_wire(), ensure_ascii=False, separators=(",", ":"))
        trace.append_event(
            {
                "event": "remote_control_websocket_server_send",
                "phase": "live",
                "wire_size_bytes": len(payload.encode("utf-8")),
                "payload": trace.payload_json(payload),
            }
        )
        ws.send(payload)

    def _next_seq_id(self, client_id: str, stream_id: str) -> int:
        key = (client_id, stream_id)
        with self._seq_lock:
            seq_id = self._next_seq_by_stream.get(key, 1)
            self._next_seq_by_stream[key] = seq_id + 1
            return seq_id

    def _stream_id_for_envelope(self, envelope: ClientEnvelope) -> str | None:
        if envelope.stream_id:
            return envelope.stream_id
        if _remote_control_message_starts_connection(envelope):
            return self._legacy_stream_ids.pop(envelope.client_id, None) or str(uuid.uuid4())
        if envelope.event.get("type") == RemoteClientEvent.PING.value:
            return self._legacy_stream_ids.get(envelope.client_id) or str(uuid.uuid4())
        return self._legacy_stream_ids.get(envelope.client_id)

    def _remember_legacy_stream_id(self, envelope: ClientEnvelope, stream_id: str) -> None:
        if envelope.stream_id is None and _remote_control_message_starts_connection(envelope):
            self._legacy_stream_ids[envelope.client_id] = stream_id

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
            self._stream_connections = {key: value for key, value in self._stream_connections.items() if key[0] != client_id}
            self._notification_opt_outs = {
                key: value for key, value in self._notification_opt_outs.items() if key[0] != client_id
            }
            self._legacy_stream_ids.pop(client_id, None)
            for key in list(self._last_completed_client_chunk_seq_by_stream):
                if key[0] == client_id:
                    self._last_completed_client_chunk_seq_by_stream.pop(key, None)
            return
        self._segment_reassembler.invalidate_stream(client_id, stream_id)
        self._outbound_buffer.remove_stream(client_id, stream_id)
        self._client_streams.discard((client_id, stream_id))
        if self._legacy_stream_ids.get(client_id) == stream_id:
            self._legacy_stream_ids.pop(client_id, None)
        self._stream_connections.pop((client_id, stream_id), None)
        self._notification_opt_outs.pop((client_id, stream_id), None)
        self._last_completed_client_chunk_seq_by_stream.pop((client_id, stream_id), None)
