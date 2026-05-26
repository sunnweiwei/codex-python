from __future__ import annotations

import datetime as dt
import json
import os
import queue
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import base64

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from codex.remote_control import (
    ClientEnvelope,
    REMOTE_CONTROL_COMPAT_VERSION,
    REMOTE_CONTROL_ACCOUNT_ID_HEADER,
    REMOTE_CONTROL_PROTOCOL_VERSION,
    REMOTE_CONTROL_WEBSOCKET_PING_INTERVAL_SECONDS,
    REMOTE_CONTROL_WEBSOCKET_PONG_TIMEOUT_SECONDS,
    REQUIRED_REMOTE_CONTROL_SERVER_NAME,
    REMOTE_CONTROL_WEBSOCKET_CLIENT_PING_TIMEOUT_SECONDS,
    RemoteControlAuth,
    RemoteControlConfig,
    RemoteControlEnrollment,
    RemoteControlError,
    RemoteControlReadyStatus,
    RemoteControlService,
    ServerEnvelope,
    _ClientSegmentReassembler,
    _OutboundBuffer,
    _RemoteAppServer,
    app_server_control_socket_available,
    app_server_control_socket_path,
    _last_user_message_index,
    _preview_from_history,
    _read_daemon_ready_status,
    _remote_auth_headers,
    _split_server_envelope_for_transport,
    _thread_item_from_response_item,
    _websocket_headers,
    build_enroll_request,
    normalize_remote_control_url,
    remote_control_official_args,
    remote_control_start_human_lines,
    remote_control_start_json_output,
    remote_control_stop_human_message,
    run_native_remote_control,
)
from codex.remote_control import service as remote_control_service_module
from codex.remote_control import trace as remote_control_trace
from codex.remote_control.utils import (
    _codex_module_command,
    _codex_module_name_from as _remote_control_module_name_from,
    _effective_app_server_client_name,
    _remote_control_client_identity,
)
from codex.cli import (
    _DaemonAppServerClient,
    _codex_module_name_from as _cli_module_name_from,
    _codex_module_prog,
    _shared_remote_control_server_name,
)
from codex.core import CodexSession
from codex.types import CodexConfig, CodexEvent


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))


class _FakeStreamingSession:
    def __init__(self, root: Path) -> None:
        self.config = CodexConfig(cwd=root, codex_home=root / "codex-home", skip_git_repo_check=True)
        self.state = type("State", (), {"thread_id": "thread-live", "turn_id": "turn-live"})()
        self.state.config = self.config
        self.tools = type("Tools", (), {"config": self.config})()

    def stream(self, prompt: str) -> list[CodexEvent]:
        command = "python3 -u -c 'print(\"tick 1\"); print(\"tick 2\")'"
        function_call = {
            "type": "function_call",
            "name": "exec_command",
            "call_id": "call_exec",
            "arguments": json.dumps({"cmd": command, "workdir": str(self.config.resolved_cwd())}),
            "status": "in_progress",
        }
        return [
            CodexEvent("turn.started"),
            CodexEvent("item.started", {"item": {"type": "message", "role": "assistant", "id": "msg_1", "content": []}, "item_id": "msg_1"}),
            CodexEvent("item.delta", {"item_id": "msg_1", "delta": "我先运行一个会流式输出的小命令。"}),
            CodexEvent("item.started", {"item": function_call, "item_id": "call_exec"}),
            CodexEvent(
                "item.delta",
                {
                    "item_id": "call_exec",
                    "delta": '{"cmd":"python3 -u -c ..."}',
                    "raw_type": "response.function_call_arguments.delta",
                },
            ),
            CodexEvent("item.completed", {"item": function_call, "item_id": "call_exec"}),
            CodexEvent("tool.started", {"name": "exec_command", "call_id": "call_exec", "arguments": {"cmd": command, "workdir": str(self.config.resolved_cwd())}}),
            CodexEvent("exec_command.output_delta", {"call_id": "call_exec", "delta": "tick 1\n", "stream": "stdout"}),
            CodexEvent("exec_command.output_delta", {"call_id": "call_exec", "delta": "tick 2\n", "stream": "stdout"}),
            CodexEvent(
                "tool.completed",
                {
                    "name": "exec_command",
                    "call_id": "call_exec",
                    "ok": True,
                    "output": "Process running with session 7",
                    "metadata": {"session_id": 7, "aggregated_output": "tick 1\ntick 2\n"},
                },
            ),
            CodexEvent("tool.started", {"name": "write_stdin", "call_id": "call_write", "arguments": {"session_id": 7, "chars": "hello from phone\n"}}),
            CodexEvent(
                "tool.completed",
                {
                    "name": "write_stdin",
                    "call_id": "call_write",
                    "ok": True,
                    "output": "tick 1\ntick 2\n",
                    "metadata": {"event_call_id": "call_exec", "exit_code": 0, "command": command, "aggregated_output": "tick 1\ntick 2\n"},
                },
            ),
            CodexEvent("item.completed", {"item": {"type": "function_call_output", "call_id": "call_exec", "output": "tick 1\ntick 2\n"}}),
            CodexEvent("item.completed", {"item": {"type": "message", "role": "assistant", "id": "msg_1", "content": [{"type": "output_text", "text": "测试完成。"}]}}),
            CodexEvent("turn.completed"),
        ]


class _FakePatchAndQuietCommandSession:
    def __init__(self, root: Path) -> None:
        self.config = CodexConfig(cwd=root, codex_home=root / "codex-home", skip_git_repo_check=True)
        self.state = type("State", (), {"thread_id": "thread-patch", "turn_id": "turn-patch"})()

    def stream(self, prompt: str) -> list[CodexEvent]:
        return [
            CodexEvent("turn.started"),
            CodexEvent(
                "tool.completed",
                {
                    "name": "apply_patch",
                    "call_id": "call_patch",
                    "ok": True,
                    "output": "Success. Updated the following files:\nM demo.py\n",
                    "metadata": {
                        "changes": [
                            {
                                "path": "demo.py",
                                "type": "update",
                                "unified_diff": "--- a/demo.py\n+++ b/demo.py\n@@\n-print('old')\n+print('new')\n",
                                "additions": 1,
                                "deletions": 1,
                            }
                        ]
                    },
                },
            ),
            CodexEvent(
                "tool.completed",
                {
                    "name": "exec_command",
                    "call_id": "call_quiet",
                    "ok": True,
                    "output": "Chunk ID: abc123\nWall time: 0.1000 seconds\nProcess exited with code 0\nOriginal token count: 0\nOutput:\n",
                    "metadata": {
                        "command": "python3 -m py_compile demo.py",
                        "cwd": str(self.config.resolved_cwd()),
                        "chunk_id": "abc123",
                        "wall_time_seconds": 0.1,
                        "exit_code": 0,
                        "original_token_count": 0,
                        "output": "",
                        "aggregated_output": "",
                    },
                },
            ),
            CodexEvent("turn.completed"),
        ]


class CodexRemoteControlTests(unittest.TestCase):
    def _seconds(self, value: str) -> int:
        return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())

    def _write_rollout(
        self,
        codex_home: Path,
        *,
        thread_id: str,
        source: str,
        cwd: Path,
        preview: str,
    ) -> Path:
        rollout_dir = codex_home / "sessions" / "2026" / "05" / "25"
        rollout_dir.mkdir(parents=True, exist_ok=True)
        path = rollout_dir / f"rollout-2026-05-25T00-00-00-{thread_id}.jsonl"
        records = [
            {
                "type": "session_meta",
                "payload": {
                    "id": thread_id,
                    "session_id": thread_id,
                    "cwd": str(cwd),
                    "source": source,
                    "model_provider": "openai",
                    "cli_version": REMOTE_CONTROL_COMPAT_VERSION,
                },
            },
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": preview}],
                },
            },
        ]
        path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")
        return path

    def test_normalize_remote_control_url_matches_official_chatgpt_and_localhost_rules(self) -> None:
        self.assertEqual(
            normalize_remote_control_url("https://chatgpt.com/backend-api").websocket_url,
            "wss://chatgpt.com/backend-api/wham/remote/control/server",
        )
        self.assertEqual(
            normalize_remote_control_url("https://chatgpt.com/backend-api").enroll_url,
            "https://chatgpt.com/backend-api/wham/remote/control/server/enroll",
        )
        self.assertEqual(
            normalize_remote_control_url("https://api.chatgpt-staging.com/backend-api").websocket_url,
            "wss://api.chatgpt-staging.com/backend-api/wham/remote/control/server",
        )
        self.assertEqual(
            normalize_remote_control_url("http://127.0.0.1:8080/backend-api").websocket_url,
            "ws://127.0.0.1:8080/backend-api/wham/remote/control/server",
        )
        self.assertEqual(
            normalize_remote_control_url("https://localhost:8443/backend-api").websocket_url,
            "wss://localhost:8443/backend-api/wham/remote/control/server",
        )

    def test_normalize_remote_control_url_rejects_non_official_hosts(self) -> None:
        for url in [
            "http://chatgpt.com/backend-api",
            "https://example.com/backend-api",
            "ftp://chatgpt.com/backend-api",
        ]:
            with self.subTest(url=url):
                with self.assertRaises(RemoteControlError):
                    normalize_remote_control_url(url)

    def test_remote_control_start_output_matches_official_human_and_json_shape(self) -> None:
        connected = RemoteControlReadyStatus(
            status="connected",
            server_name="owen-mbp",
            environment_id="env_test",
            timed_out=False,
        )
        self.assertEqual(
            remote_control_start_human_lines(connected, mode="foreground"),
            [
                "This machine is available for remote control as owen-mbp.",
                "Press Ctrl-C to stop.",
            ],
        )
        connecting = RemoteControlReadyStatus(status="connecting", server_name="owen-mbp", timed_out=True)
        self.assertEqual(
            remote_control_start_human_lines(connecting, mode="daemon"),
            ["Remote control is enabled on owen-mbp and still connecting."],
        )
        payload = json.loads(remote_control_start_json_output(connected, mode="foreground").to_json())
        self.assertEqual(
            payload,
            {
                "mode": "foreground",
                "status": "connected",
                "serverName": "owen-mbp",
                "environmentId": "env_test",
                "timedOut": False,
            },
        )

    def test_remote_control_start_rejects_disabled_and_errored_statuses(self) -> None:
        for status in ["disabled", "errored"]:
            with self.subTest(status=status):
                with self.assertRaises(RemoteControlError):
                    remote_control_start_human_lines(
                        RemoteControlReadyStatus(status=status, server_name="owen-mbp"),  # type: ignore[arg-type]
                        mode="foreground",
                    )

    def test_remote_control_envelopes_use_official_snake_case_wire_shape(self) -> None:
        incoming = ClientEnvelope.from_wire(
            {
                "type": "client_message",
                "client_id": "client-a",
                "stream_id": "stream-a",
                "seq_id": 7,
                "cursor": "cursor-a",
                "message": {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            }
        )
        self.assertEqual(incoming.client_id, "client-a")
        self.assertEqual(incoming.event["type"], "client_message")
        self.assertEqual(incoming.to_wire()["message"]["method"], "initialize")

        outgoing = ServerEnvelope(
            client_id="client-a",
            stream_id="stream-a",
            seq_id=1,
            event={"type": "pong", "status": "active"},
        )
        self.assertEqual(
            outgoing.to_wire(),
            {
                "client_id": "client-a",
                "stream_id": "stream-a",
                "seq_id": 1,
                "type": "pong",
                "status": "active",
            },
        )

    def test_legacy_stream_ids_are_in_memory_only_and_reset_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = RemoteControlConfig(codex_home=codex_home, cwd=root)

            service = RemoteControlService(config)
            first = _FakeWebSocket()
            service._handle_client_envelope(
                first,
                ClientEnvelope(
                    client_id="legacy-client",
                    stream_id=None,
                    seq_id=1,
                    event={
                        "type": "client_message",
                        "message": {"id": 1, "method": "initialize", "params": {}},
                    },
                ),
            )
            first_stream = first.sent[0]["stream_id"]
            self.assertIsInstance(first_stream, str)

            service._handle_client_envelope(
                first,
                ClientEnvelope(
                    client_id="legacy-client",
                    stream_id=None,
                    seq_id=2,
                    event={
                        "type": "client_message",
                        "message": {"id": 2, "method": "config/read", "params": {}},
                    },
                ),
            )
            self.assertEqual(first.sent[-1]["stream_id"], first_stream)

            state_path = codex_home / "remote-control.json"
            if state_path.exists():
                self.assertNotIn("legacy_stream_ids", json.loads(state_path.read_text(encoding="utf-8")))

            restarted = RemoteControlService(config)
            second = _FakeWebSocket()
            restarted._handle_client_envelope(
                second,
                ClientEnvelope(
                    client_id="legacy-client",
                    stream_id=None,
                    seq_id=1,
                    event={
                        "type": "client_message",
                        "message": {"id": 1, "method": "initialize", "params": {}},
                    },
                ),
            )
            self.assertIsInstance(second.sent[0]["stream_id"], str)
            self.assertNotEqual(second.sent[0]["stream_id"], first_stream)

    def test_remote_control_advertises_mobile_supported_app_server_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "client-a",
                "stream-a",
                "initialize",
                {"clientInfo": {"name": "test", "version": "1"}},
                request_id=1,
            )
        self.assertEqual(REMOTE_CONTROL_COMPAT_VERSION, "0.133.0")
        self.assertIn(f"/{REMOTE_CONTROL_COMPAT_VERSION}", response["userAgent"])
        self.assertTrue(response["userAgent"].startswith("Codex Desktop/"))
        self.assertTrue(response["userAgent"].endswith("(test; 1)"))

    def test_remote_control_backend_initialize_does_not_override_originator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "client-a",
                "stream-a",
                "initialize",
                {"clientInfo": {"name": "codex-backend", "version": "1"}},
                request_id=1,
            )
        self.assertEqual(response["userAgent"], config.user_agent_override)

    def test_remote_control_default_identity_matches_successful_mobile_trace_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "client-a",
                "stream-a",
                "initialize",
                {"clientInfo": {"name": "codex-backend", "version": "1"}},
                request_id=1,
            )
        self.assertEqual(config.app_server_client_name, "Codex Desktop")
        self.assertEqual(config.app_server_client_version, "dumb")
        self.assertTrue(config.allow_desktop_compat_identity)
        self.assertEqual(response["userAgent"], config.user_agent_override)
        self.assertTrue(response["userAgent"].startswith("Codex Desktop/0.133.0 "))
        self.assertTrue(response["userAgent"].endswith(" dumb"))

    def test_remote_control_known_good_desktop_identity_is_locked(self) -> None:
        records = _load_success_trace_fixture()
        backend_user_agent = _server_result_user_agent(
            records,
            client_id="backend_srv_success",
            response_id="__slingshot_backend_initialize__",
        )
        self.assertEqual(REQUIRED_REMOTE_CONTROL_SERVER_NAME, socket.gethostname().strip())

        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            self.assertEqual(config.app_server_client_name, "Codex Desktop")
            self.assertEqual(config.app_server_client_version, "dumb")
            self.assertIsNone(_effective_app_server_client_name(config))
            self.assertEqual(config.user_agent_override, backend_user_agent)

            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "backend_srv_success",
                "stream-backend",
                "initialize",
                {"clientInfo": {"name": "codex-backend", "version": "unknown"}},
                request_id="__slingshot_backend_initialize__",
            )
        self.assertEqual(response["userAgent"], backend_user_agent)

    def test_remote_control_known_good_websocket_headers_are_locked(self) -> None:
        records = _load_success_trace_fixture()
        request = _first_trace_event(records, "remote_control_websocket_request")
        headers = request["headers"]
        expected_name_header = base64.b64encode(REQUIRED_REMOTE_CONTROL_SERVER_NAME.encode("utf-8")).decode("ascii")

        auth = RemoteControlAuth(access_token="access-token", account_id="account-success")
        enrollment = RemoteControlEnrollment(
            account_id="account-success",
            environment_id="env_success",
            server_id="srv_success",
            server_name=REQUIRED_REMOTE_CONTROL_SERVER_NAME,
        )
        generated_headers = dict(
            header.split(": ", 1)
            for header in _websocket_headers(
                auth,
                enrollment,
                installation_id="install-success",
                subscribe_cursor=None,
            )
        )

        self.assertEqual(request["url"], "wss://chatgpt.com/backend-api/wham/remote/control/server")
        self.assertEqual(request["server_name"], REQUIRED_REMOTE_CONTROL_SERVER_NAME)
        self.assertEqual(headers["x-codex-name"], expected_name_header)
        self.assertEqual(headers["x-codex-protocol-version"], REMOTE_CONTROL_PROTOCOL_VERSION)
        self.assertEqual(generated_headers["x-codex-name"], expected_name_header)
        self.assertEqual(generated_headers["x-codex-protocol-version"], REMOTE_CONTROL_PROTOCOL_VERSION)
        self.assertEqual(generated_headers[REMOTE_CONTROL_ACCOUNT_ID_HEADER], "account-success")

    def test_remote_control_daemon_uses_current_package_module_not_hardcoded_agents_path(self) -> None:
        self.assertEqual(_remote_control_module_name_from("codex.remote_control.utils"), "codex")
        self.assertEqual(_remote_control_module_name_from("codex.remote_control.utils"), "codex")
        self.assertEqual(_cli_module_name_from("codex.cli"), "codex")
        self.assertEqual(_cli_module_name_from("codex.cli"), "codex")
        self.assertEqual(_codex_module_command("remote-control"), [sys.executable, "-m", "codex", "remote-control"])
        self.assertEqual(_codex_module_prog("remote-control"), "python -m codex remote-control")

    def test_remote_control_known_good_ios_bootstrap_trace_is_locked(self) -> None:
        records = _load_success_trace_fixture()
        mobile_messages = _client_messages(records, "cli_success_ios")
        methods = [message["method"] for message in mobile_messages]
        self.assertEqual(methods, ["initialize", "thread/list", "process/spawn", "plugin/list", "skills/list"])

        initialize = mobile_messages[0]
        client_info = initialize["params"]["clientInfo"]
        self.assertEqual(client_info, {"version": "1.2026.132", "name": "codex_chatgpt_ios_remote"})

        mobile_user_agent = _server_result_user_agent(records, client_id="cli_success_ios", response_id="ios-initialize")
        self.assertEqual(
            mobile_user_agent,
            "Codex Desktop/0.133.0 (Mac OS 26.5.0; arm64) dumb (codex_chatgpt_ios_remote; 1.2026.132)",
        )

        status = _server_notification(records, "cli_success_ios", "remoteControl/status/changed")
        self.assertEqual(
            status["params"],
            {
                "status": "connected",
                "serverName": REQUIRED_REMOTE_CONTROL_SERVER_NAME,
                "installationId": "install-success",
                "environmentId": "env_success",
            },
        )

        thread_list = mobile_messages[1]
        self.assertEqual(
            thread_list["params"],
            {"limit": 25, "sortKey": "updated_at", "sourceKinds": [], "sortDirection": "desc"},
        )

        process_spawn = mobile_messages[2]["params"]
        self.assertEqual(process_spawn["command"][:2], ["/bin/bash", "-lc"])
        self.assertEqual(process_spawn["outputBytesCap"], 1_000_000)
        self.assertFalse(process_spawn["streamStdoutStderr"])
        self.assertFalse(process_spawn["streamStdin"])
        self.assertFalse(process_spawn["tty"])

        self.assertEqual(mobile_messages[3]["params"]["cwds"], [str(Path.cwd().resolve())])
        self.assertEqual(mobile_messages[4]["params"]["cwds"], [str(Path.cwd().resolve())])

    def test_remote_control_desktop_client_identity_can_be_explicitly_disabled(self) -> None:
        previous_originator = os.environ.get("CODEX_INTERNAL_ORIGINATOR_OVERRIDE")
        try:
            os.environ.pop("CODEX_INTERNAL_ORIGINATOR_OVERRIDE", None)
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(
                    codex_home=Path(tmp),
                    cwd=Path(tmp),
                    app_server_client_name="Codex Desktop",
                    app_server_client_version="0.133.0",
                    allow_desktop_compat_identity=False,
                    user_agent_override=None,
                )
                service = type("Service", (), {"config": config})()
                response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                    _FakeWebSocket(),
                    "client-a",
                    "stream-a",
                    "initialize",
                    {"clientInfo": {"name": "codex-backend", "version": "1"}},
                    request_id=1,
                )
        finally:
            if previous_originator is None:
                os.environ.pop("CODEX_INTERNAL_ORIGINATOR_OVERRIDE", None)
            else:
                os.environ["CODEX_INTERNAL_ORIGINATOR_OVERRIDE"] = previous_originator
        self.assertTrue(response["userAgent"].startswith("codex_cli_rs/"))

        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(
                codex_home=Path(tmp),
                cwd=Path(tmp),
                app_server_client_name="Codex Desktop",
                app_server_client_version="0.133.0",
                allow_desktop_compat_identity=True,
                user_agent_override=None,
            )
            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "client-a",
                "stream-a",
                "initialize",
                {"clientInfo": {"name": "codex-backend", "version": "1"}},
                request_id=1,
            )
        self.assertTrue(response["userAgent"].startswith("Codex Desktop/"))
        self.assertTrue(response["userAgent"].endswith("unknown (0.133.0)"))

    def test_remote_control_user_agent_can_match_official_trace_exactly(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_USER_AGENT")
        expected = "Codex Desktop/0.133.0 (Mac OS 26.5.0; arm64) dumb"
        try:
            os.environ["PY_CODEX_REMOTE_CONTROL_USER_AGENT"] = expected
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(
                    codex_home=Path(tmp),
                    cwd=Path(tmp),
                    app_server_client_name="Codex Desktop",
                    app_server_client_version="dumb",
                    allow_desktop_compat_identity=True,
                )
                service = type("Service", (), {"config": config})()
                response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                    _FakeWebSocket(),
                    "client-a",
                    "stream-a",
                    "initialize",
                    {"clientInfo": {"name": "codex-backend", "version": "unknown"}},
                    request_id=1,
                )
            self.assertEqual(response["userAgent"], expected)
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_USER_AGENT", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_USER_AGENT"] = previous

    def test_remote_control_mobile_initialize_appends_official_client_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base_user_agent = "Codex Desktop/0.133.0 (Mac OS 26.5.0; arm64) dumb"
            config = RemoteControlConfig(
                codex_home=Path(tmp),
                cwd=Path(tmp),
                user_agent_override=base_user_agent,
            )
            service = type("Service", (), {"config": config})()
            response = _RemoteAppServer(service)._dispatch(  # type: ignore[arg-type]
                _FakeWebSocket(),
                "client-a",
                "stream-a",
                "initialize",
                {"clientInfo": {"name": "codex_chatgpt_ios_remote", "version": "1.2026.132"}},
                request_id=1,
            )
        self.assertEqual(
            response["userAgent"],
            f"{base_user_agent} (codex_chatgpt_ios_remote; 1.2026.132)",
        )

    def test_remote_control_service_drops_identity_env_for_child_processes(self) -> None:
        previous = {name: os.environ.get(name) for name in (
            "PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_NAME",
            "PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_VERSION",
            "PY_CODEX_REMOTE_CONTROL_ALLOW_DESKTOP_COMPAT",
            "PY_CODEX_REMOTE_CONTROL_USER_AGENT",
        )}
        try:
            for name in previous:
                os.environ[name] = f"value-for-{name}"
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
                self.assertEqual(config.user_agent_override, "value-for-PY_CODEX_REMOTE_CONTROL_USER_AGENT")
                RemoteControlService(config)
                for name in previous:
                    self.assertNotIn(name, os.environ)
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def test_client_message_chunks_are_reassembled_to_official_message_shape(self) -> None:
        message = {"jsonrpc": "2.0", "id": 9, "method": "initialize", "params": {}}
        raw = json.dumps(message, separators=(",", ":")).encode("utf-8")
        midpoint = len(raw) // 2
        first = ClientEnvelope.from_wire(
            {
                "type": "client_message_chunk",
                "client_id": "client-a",
                "stream_id": "stream-a",
                "seq_id": 42,
                "segment_id": 0,
                "segment_count": 2,
                "message_size_bytes": len(raw),
                "message_chunk_base64": base64.b64encode(raw[:midpoint]).decode("ascii"),
            }
        )
        second = ClientEnvelope.from_wire(
            {
                **first.to_wire(),
                "segment_id": 1,
                "message_chunk_base64": base64.b64encode(raw[midpoint:]).decode("ascii"),
            }
        )
        reassembler = _ClientSegmentReassembler()
        self.assertIsNone(reassembler.observe(first))
        completed = reassembler.observe(second)
        self.assertIsNotNone(completed)
        self.assertEqual(completed.event["type"], "client_message")  # type: ignore[union-attr]
        self.assertEqual(completed.event["message"], message)  # type: ignore[union-attr]

    def test_large_server_messages_are_split_and_ack_buffer_matches_wire_cursor(self) -> None:
        envelope = ServerEnvelope(
            client_id="client-a",
            stream_id="stream-a",
            seq_id=4,
            event={"type": "server_message", "message": {"id": 1, "result": {"text": "x" * 250_000}}},
        )
        chunks = _split_server_envelope_for_transport(envelope)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk.event["type"] == "server_message_chunk" for chunk in chunks))
        self.assertEqual({chunk.seq_id for chunk in chunks}, {4})

        buffer = _OutboundBuffer()
        for chunk in chunks:
            buffer.insert(chunk)
        buffer.ack("client-a", "stream-a", 4, 0)
        remaining = buffer.server_envelopes()
        self.assertEqual(len(remaining), len(chunks) - 1)
        self.assertTrue(all(int(chunk.event["segment_id"]) > 0 for chunk in remaining))
        buffer.ack("client-a", "stream-a", 4, None)
        self.assertEqual(buffer.server_envelopes(), [])

    def test_compat_arg_builder_keeps_official_cli_shape_without_runtime_delegation(self) -> None:
        self.assertEqual(remote_control_official_args(None), ["remote-control"])
        self.assertEqual(remote_control_official_args("start", json_output=True), ["remote-control", "--json", "start"])
        self.assertEqual(remote_control_official_args("stop"), ["remote-control", "stop"])

    def test_native_stop_is_python_only_and_login_free(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = StringIO()
            config = CodexConfig(codex_home=tmp, skip_git_repo_check=True)
            with redirect_stdout(output):
                self.assertEqual(run_native_remote_control("stop", json_output=True, codex_config=config), 0)
            self.assertEqual(json.loads(output.getvalue()), {"status": "notRunning"})

    def test_local_control_socket_multiplexes_loaded_threads_for_multiple_clients(self) -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = RemoteControlConfig(
                codex_home=codex_home,
                cwd=root,
                codex_config=CodexConfig(cwd=root, codex_home=codex_home, skip_git_repo_check=True),
            )
            service = RemoteControlService(config)
            try:
                service._local_control.start()
            except PermissionError as exc:
                self.skipTest(f"Unix socket bind is not available in this sandbox: {exc}")
            try:
                self.assertTrue(app_server_control_socket_available(codex_home))
                socket_path = app_server_control_socket_path(codex_home)

                first = _DaemonAppServerClient(socket_path)
                first.connect()
                session = CodexSession(CodexConfig(cwd=root, codex_home=codex_home, skip_git_repo_check=True))
                start_response = first.start_thread(session)
                thread_id = start_response["thread"]["id"]

                second = _DaemonAppServerClient(socket_path)
                second.connect()
                loaded = second.request("thread/loaded/list", {}, timeout=2)
                listed = second.request("thread/list", {"limit": 20}, timeout=2)
                self.assertIn(thread_id, loaded["data"])
                self.assertIn(thread_id, [row["id"] for row in listed["data"]])

                first.close()
                second.close()
            finally:
                service._local_control.stop()

    def test_daemon_status_reads_real_running_service_over_local_socket(self) -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = RemoteControlConfig(
                codex_home=codex_home,
                cwd=root,
            )
            service = RemoteControlService(config)
            service.status = "connected"
            service.environment_id = "env-real"
            try:
                service._local_control.start()
            except PermissionError as exc:
                self.skipTest(f"Unix socket bind is not available in this sandbox: {exc}")
            try:
                shell_config = RemoteControlConfig(
                    codex_home=codex_home,
                    cwd=root,
                )
                status = _read_daemon_ready_status(shell_config)
                self.assertEqual(
                    status,
                    RemoteControlReadyStatus(
                        status="connected",
                        server_name=REQUIRED_REMOTE_CONTROL_SERVER_NAME,
                        environment_id="env-real",
                        timed_out=False,
                    ),
                )
            finally:
                service._local_control.stop()

    def test_remote_control_initialize_does_not_replay_loaded_threads(self) -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(
                codex_home=root / "codex-home",
                cwd=root,
                codex_config=CodexConfig(cwd=root, codex_home=root / "codex-home", skip_git_repo_check=True),
            )
            service = RemoteControlService(config)
            first = _FakeWebSocket()
            service._handle_client_envelope(
                first,
                ClientEnvelope(
                    client_id="client-a",
                    stream_id="stream-a",
                    seq_id=1,
                    event={
                        "type": "client_message",
                        "message": {
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "clientInfo": {"name": "codex-tui", "version": "test"},
                                "capabilities": {"experimentalApi": True},
                            },
                        },
                    },
                ),
            )
            service._handle_client_envelope(
                first,
                ClientEnvelope(
                    client_id="client-a",
                    stream_id="stream-a",
                    seq_id=2,
                    event={"type": "client_message", "message": {"id": 2, "method": "thread/start", "params": {}}},
                ),
            )

            second = _FakeWebSocket()
            service._handle_client_envelope(
                second,
                ClientEnvelope(
                    client_id="client-b",
                    stream_id="stream-b",
                    seq_id=1,
                    event={
                        "type": "client_message",
                        "message": {
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "clientInfo": {"name": "codex-tui", "version": "test"},
                                "capabilities": {"experimentalApi": True},
                            },
                        },
                    },
                ),
            )

        second_messages = [
            payload.get("message")
            for payload in second.sent
            if payload.get("type") == "server_message" and isinstance(payload.get("message"), dict)
        ]
        self.assertNotIn("thread/started", [message.get("method") for message in second_messages])

    def test_remote_control_initialize_opt_out_filters_thread_started_notifications(self) -> None:
        with tempfile.TemporaryDirectory(dir="/private/tmp") as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(
                codex_home=root / "codex-home",
                cwd=root,
                codex_config=CodexConfig(cwd=root, codex_home=root / "codex-home", skip_git_repo_check=True),
            )
            service = RemoteControlService(config)
            ws = _FakeWebSocket()
            service._handle_client_envelope(
                ws,
                ClientEnvelope(
                    client_id="client-opt-out",
                    stream_id="stream-opt-out",
                    seq_id=1,
                    event={
                        "type": "client_message",
                        "message": {
                            "id": 1,
                            "method": "initialize",
                            "params": {
                                "clientInfo": {"name": "codex-tui", "version": "test"},
                                "capabilities": {
                                    "experimentalApi": True,
                                    "optOutNotificationMethods": ["thread/started"],
                                },
                            },
                        },
                    },
                ),
            )
            service._handle_client_envelope(
                ws,
                ClientEnvelope(
                    client_id="client-opt-out",
                    stream_id="stream-opt-out",
                    seq_id=2,
                    event={"type": "client_message", "message": {"id": 2, "method": "thread/start", "params": {}}},
                ),
            )

        messages = [
            payload.get("message")
            for payload in ws.sent
            if payload.get("type") == "server_message" and isinstance(payload.get("message"), dict)
        ]
        self.assertNotIn("thread/started", [message.get("method") for message in messages])

    def test_remote_app_server_answers_common_mobile_bootstrap_requests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "initialize", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "config/read", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 3, "method": "model/list", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 4, "method": "account/read", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 5, "method": "thread/start", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 6, "method": "plugin/list", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 7, "method": "mcpServerStatus/list", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 8, "method": "configRequirements/read", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 9, "method": "plugin/read", "params": {"pluginId": "demo"}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 10, "method": "marketplace/upgrade", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 11, "method": "skills/config/write", "params": {"enabled": True}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 12, "method": "config/value/write", "params": {"keyPath": "model", "value": "gpt-5.5", "mergeStrategy": "replace"}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 13, "method": "collaborationMode/list", "params": {}})

        responses = [message for message in ws.sent if "id" in message]
        self.assertEqual([message["id"] for message in responses], list(range(1, 14)))
        initialize_notifications = [
            message
            for message in ws.sent
            if message.get("method") == "remoteControl/status/changed"
        ]
        self.assertEqual(
            initialize_notifications[0],
            {
                "method": "remoteControl/status/changed",
                "params": {
                    "status": "connected",
                    "serverName": config.server_name,
                    "installationId": "install-test",
                    "environmentId": "env-test",
                },
            },
        )
        self.assertIn("codexHome", responses[0]["result"])  # type: ignore[operator]
        self.assertIn("config", responses[1]["result"])  # type: ignore[operator]
        self.assertIn("data", responses[2]["result"])  # type: ignore[operator]
        self.assertIn("account", responses[3]["result"])  # type: ignore[operator]
        self.assertEqual(responses[4]["result"]["sandbox"]["type"], "workspaceWrite")  # type: ignore[index]
        self.assertIn("marketplaces", responses[5]["result"])  # type: ignore[operator]
        self.assertIn("data", responses[6]["result"])  # type: ignore[operator]
        self.assertIn("requirements", responses[7]["result"])  # type: ignore[operator]
        self.assertEqual(responses[8]["result"]["plugin"]["summary"]["id"], "demo")  # type: ignore[index]
        self.assertEqual(responses[9]["result"]["errors"], [])  # type: ignore[index]
        self.assertTrue(responses[10]["result"]["effectiveEnabled"])  # type: ignore[index]
        self.assertEqual(responses[11]["result"], {})
        self.assertEqual(
            responses[12]["result"],
            {
                "data": [
                    {"name": "Plan", "mode": "plan", "model": None, "reasoning_effort": "medium"},
                    {"name": "Default", "mode": "default", "model": None, "reasoning_effort": None},
                ]
            },
        )

    def test_remote_app_server_handles_thread_and_filesystem_utility_methods(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/start", "params": {}})
            start_response = next(message for message in ws.sent if message.get("id") == 1)
            thread_id = start_response["result"]["thread"]["id"]  # type: ignore[index]
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/name/set", "params": {"threadId": thread_id, "name": "Remote test"}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 3, "method": "thread/read", "params": {"threadId": thread_id, "includeTurns": False}},
            )
            file_path = root / "notes.txt"
            encoded = base64.b64encode("hello remote".encode("utf-8")).decode("ascii")
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 4, "method": "fs/writeFile", "params": {"path": str(file_path), "dataBase64": encoded}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 5, "method": "fs/readFile", "params": {"path": str(file_path)}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 6, "method": "fs/getMetadata", "params": {"path": str(file_path)}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 7, "method": "fs/readDirectory", "params": {"path": str(root)}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 8, "method": "fuzzyFileSearch", "params": {"query": "note", "roots": [str(root)], "cancellationToken": None}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 9, "method": "getAuthStatus", "params": {"includeToken": False}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 10,
                    "method": "command/exec",
                    "params": {"command": [sys.executable, "-c", "print('remote ok')"], "cwd": str(root), "timeoutMs": 5000},
                },
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 11,
                    "method": "thread/metadata/update",
                    "params": {"threadId": thread_id, "gitInfo": {"sha": "abc123", "branch": "main", "originUrl": None}},
                },
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 12,
                    "method": "thread/shellCommand",
                    "params": {"threadId": thread_id, "command": f"{sys.executable} -c \"print('shell ok')\""},
                },
            )
            _wait_for_sent_method(ws, "item/completed")
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 13, "method": "thread/rollback", "params": {"threadId": thread_id, "numTurns": 1}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(responses[2], {})
        self.assertEqual(responses[3]["thread"]["name"], "Remote test")  # type: ignore[index]
        self.assertEqual(base64.b64decode(responses[5]["dataBase64"]).decode("utf-8"), "hello remote")  # type: ignore[index]
        self.assertTrue(responses[6]["isFile"])  # type: ignore[index]
        self.assertTrue(any(entry["fileName"] == "notes.txt" for entry in responses[7]["entries"]))  # type: ignore[index]
        self.assertTrue(any(item["path"] == "notes.txt" for item in responses[8]["files"]))  # type: ignore[index]
        self.assertTrue(responses[9]["requiresOpenaiAuth"])  # type: ignore[index]
        self.assertEqual(responses[10]["exitCode"], 0)  # type: ignore[index]
        self.assertEqual(responses[10]["stdout"], "remote ok\n")  # type: ignore[index]
        self.assertEqual(responses[11]["thread"]["gitInfo"]["sha"], "abc123")  # type: ignore[index]
        self.assertEqual(responses[11]["thread"]["gitInfo"]["branch"], "main")  # type: ignore[index]
        self.assertEqual(responses[12], {})
        self.assertIn("thread", responses[13])
        self.assertTrue(any(message.get("method") == "item/completed" for message in ws.sent))

    def test_remote_thread_items_hide_contextual_user_messages_from_mobile_ui(self) -> None:
        context_item = {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "<environment_context>\n  <cwd>/tmp/demo</cwd>\n</environment_context>",
                }
            ],
        }
        real_user_item = {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}],
        }

        self.assertIsNone(_thread_item_from_response_item(context_item))
        self.assertEqual(_preview_from_history([context_item, real_user_item]), "hello")
        self.assertEqual(_last_user_message_index([context_item, real_user_item]), 1)

    def test_remote_thread_resume_replays_exec_command_as_command_execution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            thread_id = "31c927b9-67e8-469f-9b79-69bdab15445c"
            rollout_dir = codex_home / "sessions" / "2026" / "05" / "25"
            rollout_dir.mkdir(parents=True)
            rollout_path = rollout_dir / f"rollout-2026-05-25T07-13-11-{thread_id}.jsonl"
            command = "python3 - <<'PY'\nprint(sum(i*i for i in range(1, 6)))\nPY"
            records = [
                {
                    "type": "session_meta",
                    "payload": {
                        "id": thread_id,
                        "session_id": thread_id,
                        "cwd": str(root),
                        "source": "cli",
                        "model_provider": "openai",
                    },
                },
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-a", "started_at": 1779718391}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message",
                        "message": "<environment_context>\n  <cwd>/tmp/demo</cwd>\n</environment_context>",
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "请运行一个很小的 Python 例子。"},
                },
                {
                    "type": "response_item",
                    "payload": {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "call_exec",
                        "arguments": json.dumps({"cmd": command, "workdir": str(root)}),
                        "status": "completed",
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "exec_command_begin",
                        "call_id": "call_exec",
                        "turn_id": "turn-a",
                        "command": [command],
                        "cwd": str(root),
                        "parsed_cmd": [{"type": "unknown", "cmd": command}],
                        "source": "agent",
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "exec_command_end",
                        "call_id": "call_exec",
                        "turn_id": "turn-a",
                        "command": [command],
                        "cwd": str(root),
                        "parsed_cmd": [{"type": "unknown", "cmd": command}],
                        "source": "agent",
                        "aggregated_output": "55\n",
                        "exit_code": 0,
                        "duration": {"secs": 0, "nanos": 135000000},
                        "status": "completed",
                    },
                },
                {
                    "type": "response_item",
                    "payload": {"type": "function_call_output", "call_id": "call_exec", "output": "55\n"},
                },
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "运行结果是 `55`。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-a", "completed_at": 1779718395}},
            ]
            rollout_path.write_text(
                "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
                encoding="utf-8",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path)}},
            )

        response = next(message for message in ws.sent if message.get("id") == 1)
        turn = response["result"]["thread"]["turns"][0]  # type: ignore[index]
        self.assertEqual(turn["status"], "completed")
        items = turn["items"]
        self.assertEqual([item["type"] for item in items], ["userMessage", "commandExecution", "agentMessage"])
        self.assertEqual(items[0]["content"][0]["text_elements"], [])
        command_item = items[1]
        self.assertEqual(command_item["command"], command)
        self.assertEqual(command_item["source"], "agent")
        self.assertEqual(command_item["status"], "completed")
        self.assertEqual(command_item["aggregatedOutput"], "55\n")
        self.assertEqual(command_item["exitCode"], 0)
        self.assertNotIn("dynamicToolCall", [item["type"] for item in items])

    def test_remote_thread_resume_replays_multi_turn_tool_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            thread_id = "thread-tools"
            rollout_dir = codex_home / "sessions" / "2026" / "05" / "25"
            rollout_dir.mkdir(parents=True)
            rollout_path = rollout_dir / f"rollout-2026-05-25T00-00-00-{thread_id}.jsonl"
            records = [
                {
                    "type": "session_meta",
                    "payload": {
                        "id": thread_id,
                        "session_id": thread_id,
                        "cwd": str(root),
                        "source": "cli",
                        "model_provider": "openai",
                    },
                },
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 1779718400}},
                {"type": "event_msg", "payload": {"type": "user_message", "message": "做一个小计划，然后列目录。"}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "plan_update",
                        "explanation": "我先列一个短计划。",
                        "plan": [
                            {"step": "列出目录", "status": "in_progress"},
                            {"step": "总结结果", "status": "pending"},
                        ],
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "exec_command_end",
                        "call_id": "call_ls",
                        "turn_id": "turn-1",
                        "command": ["ls -1"],
                        "cwd": str(root),
                        "parsed_cmd": [{"type": "list_files", "cmd": "ls -1", "path": None}],
                        "source": "agent",
                        "aggregated_output": "alpha.txt\nbeta.py\n",
                        "exit_code": 0,
                        "duration": {"secs": 0, "nanos": 50000000},
                        "status": "completed",
                    },
                },
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "目录里有 `alpha.txt` 和 `beta.py`。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-1", "completed_at": 1779718402}},
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": 1779718410}},
                {"type": "event_msg", "payload": {"type": "user_message", "message": "再模拟搜索和编辑展示。"}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "web_search_end",
                        "call_id": "search-1",
                        "query": "OpenAI Codex",
                        "action": {"type": "search", "query": "OpenAI Codex", "queries": None},
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "patch_apply_end",
                        "call_id": "patch-1",
                        "turn_id": "turn-2",
                        "success": True,
                        "status": "completed",
                        "changes": {
                            "notes/demo.txt": {
                                "type": "add",
                                "content": "hello from remote history\n",
                            }
                        },
                    },
                },
                {"type": "event_msg", "payload": {"type": "view_image_tool_call", "call_id": "image-1", "path": str(root / "plot.png")}},
                {"type": "event_msg", "payload": {"type": "context_compacted", "call_id": "compact-1"}},
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "搜索、文件修改和图片查看条目都已生成。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-2", "completed_at": 1779718413}},
            ]
            rollout_path.write_text(
                "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
                encoding="utf-8",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path)}},
            )

        response = next(message for message in ws.sent if message.get("id") == 1)
        turns = response["result"]["thread"]["turns"]  # type: ignore[index]
        self.assertEqual([turn["id"] for turn in turns], ["turn-1", "turn-2"])
        self.assertEqual(
            [item["type"] for item in turns[0]["items"]],
            ["userMessage", "plan", "commandExecution", "agentMessage"],
        )
        self.assertEqual(
            [item["type"] for item in turns[1]["items"]],
            ["userMessage", "webSearch", "fileChange", "imageView", "contextCompaction", "agentMessage"],
        )
        self.assertEqual(turns[1]["items"][2]["changes"][0]["kind"], {"type": "add"})

    def test_remote_thread_turns_list_items_view_matches_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            thread_id = "thread-items-view"
            rollout_dir = codex_home / "sessions" / "2026" / "05" / "25"
            rollout_dir.mkdir(parents=True)
            rollout_path = rollout_dir / f"rollout-2026-05-25T00-00-00-{thread_id}.jsonl"
            records = [
                {"type": "session_meta", "payload": {"id": thread_id, "session_id": thread_id, "cwd": str(root), "source": "cli"}},
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 1779718400}},
                {"type": "event_msg", "payload": {"type": "user_message", "message": "先列目录。"}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "exec_command_end",
                        "call_id": "call_ls",
                        "command": ["ls -1"],
                        "aggregated_output": "alpha.txt\n",
                        "exit_code": 0,
                    },
                },
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "目录看完了。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-1", "completed_at": 1779718401}},
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": 1779718410}},
                {"type": "event_msg", "payload": {"type": "user_message", "message": "再改文件。"}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "patch_apply_end",
                        "call_id": "patch-1",
                        "success": True,
                        "changes": {"notes/demo.txt": {"type": "add", "content": "hello\n"}},
                    },
                },
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "文件改完了。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-2", "completed_at": 1779718411}},
            ]
            rollout_path.write_text(
                "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
                encoding="utf-8",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path)}},
            )

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/turns/list", "params": {"threadId": thread_id, "sortDirection": "asc"}},
            )
            summary_response = next(message for message in ws.sent if message.get("id") == 2)
            summary_turns = summary_response["result"]["data"]  # type: ignore[index]
            self.assertEqual([turn["itemsView"] for turn in summary_turns], ["summary", "summary"])
            self.assertEqual([item["type"] for item in summary_turns[0]["items"]], ["userMessage", "agentMessage"])
            self.assertEqual([item["type"] for item in summary_turns[1]["items"]], ["userMessage", "agentMessage"])

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 3,
                    "method": "thread/turns/list",
                    "params": {"threadId": thread_id, "sortDirection": "asc", "itemsView": "full"},
                },
            )
            full_response = next(message for message in ws.sent if message.get("id") == 3)
            full_turns = full_response["result"]["data"]  # type: ignore[index]
            self.assertEqual(full_turns[0]["itemsView"], "full")
            self.assertEqual([item["type"] for item in full_turns[0]["items"]], ["userMessage", "commandExecution", "agentMessage"])

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 4,
                    "method": "thread/turns/list",
                    "params": {"threadId": thread_id, "sortDirection": "asc", "itemsView": "notLoaded"},
                },
            )
            not_loaded_response = next(message for message in ws.sent if message.get("id") == 4)
            not_loaded_turns = not_loaded_response["result"]["data"]  # type: ignore[index]
            self.assertEqual(not_loaded_turns[0]["itemsView"], "notLoaded")
            self.assertEqual(not_loaded_turns[0]["items"], [])

    def test_remote_thread_turns_list_unloaded_rollout_does_not_resume_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            thread_id = "thread-unloaded-turns"
            rollout_dir = codex_home / "sessions" / "2026" / "05" / "25"
            rollout_dir.mkdir(parents=True)
            rollout_path = rollout_dir / f"rollout-2026-05-25T00-00-00-{thread_id}.jsonl"
            records = [
                {"type": "session_meta", "payload": {"id": thread_id, "session_id": thread_id, "cwd": str(root), "source": "cli"}},
                {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": 1779718400}},
                {"type": "event_msg", "payload": {"type": "user_message", "message": "第一页。"}},
                {"type": "event_msg", "payload": {"type": "agent_message", "message": "第一页回复。"}},
                {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-1", "completed_at": 1779718401}},
            ]
            rollout_path.write_text(
                "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
                encoding="utf-8",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/turns/list", "params": {"threadId": thread_id, "itemsView": "summary"}},
            )
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "thread/list", "params": {}})

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        turns = responses[1]["data"]  # type: ignore[index]
        self.assertEqual([item["type"] for item in turns[0]["items"]], ["userMessage", "agentMessage"])
        self.assertEqual(app_server._sessions, {})  # type: ignore[attr-defined]
        self.assertEqual(responses[2]["data"][0]["cwd"], str(root.resolve()))  # type: ignore[index]

    def test_remote_thread_list_defaults_to_upstream_interactive_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            self._write_rollout(codex_home, thread_id="thread-cli", source="cli", cwd=root, preview="hello from cli")
            self._write_rollout(codex_home, thread_id="thread-exec", source="exec", cwd=root, preview="hello from exec")
            self._write_rollout(
                codex_home,
                thread_id="thread-legacy",
                source="appServer",
                cwd=root,
                preview="hello from legacy remote",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/list", "params": {}})
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/list", "params": {"sourceKinds": ["exec"]}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        default_threads = responses[1]["data"]  # type: ignore[index]
        default_previews = {thread["preview"] for thread in default_threads}
        self.assertIn("hello from cli", default_previews)
        self.assertIn("hello from legacy remote", default_previews)
        self.assertNotIn("hello from exec", default_previews)
        self.assertEqual({thread["source"] for thread in default_threads}, {"cli"})
        self.assertTrue(all(thread["turns"] == [] for thread in default_threads))
        self.assertEqual([thread["preview"] for thread in responses[2]["data"]], ["hello from exec"])  # type: ignore[index]

    def test_remote_thread_list_sort_key_matches_upstream_created_and_updated_at(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            old_created_recently_used = self._write_rollout(
                codex_home,
                thread_id="thread-old-created-recent-used",
                source="cli",
                cwd=root,
                preview="old created recent used",
            )
            old_created_recently_used.write_text(
                old_created_recently_used.read_text(encoding="utf-8").replace(
                    '"model_provider": "openai"',
                    '"model_provider": "openai", "timestamp": "2026-05-21T10:00:00Z"',
                ),
                encoding="utf-8",
            )
            new_created_old_used = self._write_rollout(
                codex_home,
                thread_id="thread-new-created-old-used",
                source="cli",
                cwd=root,
                preview="new created old used",
            )
            new_created_old_used.write_text(
                new_created_old_used.read_text(encoding="utf-8").replace(
                    '"model_provider": "openai"',
                    '"model_provider": "openai", "timestamp": "2026-05-22T10:00:00Z"',
                ),
                encoding="utf-8",
            )
            os.utime(
                old_created_recently_used,
                (self._seconds("2026-05-25T12:00:00Z"), self._seconds("2026-05-25T12:00:00Z")),
            )
            os.utime(
                new_created_old_used,
                (self._seconds("2026-05-23T12:00:00Z"), self._seconds("2026-05-23T12:00:00Z")),
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/list", "params": {}})
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/list", "params": {"sortKey": "updatedAt"}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(
            [thread["preview"] for thread in responses[1]["data"]],  # type: ignore[index]
            ["new created old used", "old created recent used"],
        )
        self.assertEqual(
            [thread["preview"] for thread in responses[2]["data"]],  # type: ignore[index]
            ["old created recent used", "new created old used"],
        )
        self.assertEqual(responses[2]["data"][0]["updatedAt"], self._seconds("2026-05-25T12:00:00Z"))  # type: ignore[index]

    def test_remote_thread_list_loaded_session_uses_rollout_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            rollout_path = self._write_rollout(
                codex_home,
                thread_id="thread-loaded",
                source="cli",
                cwd=root,
                preview="loaded thread",
            )
            rollout_path.write_text(
                rollout_path.read_text(encoding="utf-8").replace(
                    '"model_provider": "openai"',
                    '"model_provider": "openai", "timestamp": "2026-05-21T10:00:00Z"',
                ),
                encoding="utf-8",
            )
            os.utime(rollout_path, (self._seconds("2026-05-25T15:00:00Z"), self._seconds("2026-05-25T15:00:00Z")))
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path)}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "thread/list", "params": {"sortKey": "updatedAt"}})

        response = next(message for message in ws.sent if message.get("id") == 2)["result"]
        loaded = response["data"][0]
        self.assertEqual(loaded["id"], "thread-loaded")
        self.assertEqual(loaded["createdAt"], self._seconds("2026-05-21T10:00:00Z"))
        self.assertEqual(loaded["updatedAt"], self._seconds("2026-05-25T15:00:00Z"))

    def test_remote_resume_preserves_rollout_cwd_like_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daemon_cwd = root / "daemon"
            project_cwd = root / "project"
            daemon_cwd.mkdir()
            project_cwd.mkdir()
            codex_home = root / "codex-home"
            rollout_path = self._write_rollout(
                codex_home,
                thread_id="thread-cwd",
                source="cli",
                cwd=project_cwd,
                preview="project thread",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=daemon_cwd)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path), "excludeTurns": True}},
            )
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "thread/list", "params": {}})

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        resumed = responses[1]["thread"]  # type: ignore[index]
        listed = responses[2]["data"][0]  # type: ignore[index]
        self.assertEqual(resumed["cwd"], str(project_cwd.resolve()))
        self.assertEqual(listed["cwd"], str(project_cwd.resolve()))
        self.assertEqual(app_server._sessions["thread-cwd"].config.resolved_cwd(), project_cwd.resolve())  # type: ignore[index]

    def test_remote_loaded_thread_keeps_normalized_rollout_cwd_stable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daemon_cwd = root / "daemon"
            project_cwd = root / "project"
            daemon_cwd.mkdir()
            project_cwd.mkdir()
            codex_home = root / "codex-home"
            rollout_path = self._write_rollout(
                codex_home,
                thread_id="thread-normalized-cwd",
                source="cli",
                cwd=project_cwd / ".." / "project",
                preview="stable folder",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=daemon_cwd)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/list", "params": {}})
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/resume", "params": {"path": str(rollout_path), "excludeTurns": True}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 3, "method": "thread/read", "params": {"threadId": "thread-normalized-cwd"}},
            )
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 4, "method": "thread/list", "params": {}})

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        expected_cwd = str(project_cwd.resolve())
        self.assertEqual(responses[1]["data"][0]["cwd"], expected_cwd)  # type: ignore[index]
        self.assertEqual(responses[2]["thread"]["cwd"], expected_cwd)  # type: ignore[index]
        self.assertEqual(responses[3]["thread"]["cwd"], expected_cwd)  # type: ignore[index]
        self.assertEqual(responses[4]["data"][0]["cwd"], expected_cwd)  # type: ignore[index]
        self.assertEqual(app_server._sessions["thread-normalized-cwd"].config.resolved_cwd(), project_cwd.resolve())  # type: ignore[index]

    def test_remote_thread_read_defaults_to_metadata_without_loading_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            self._write_rollout(
                codex_home,
                thread_id="thread-read-default",
                source="cli",
                cwd=root,
                preview="read without turns",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/read", "params": {"threadId": "thread-read-default"}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/read", "params": {"threadId": "thread-read-default", "includeTurns": True}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(responses[1]["thread"]["turns"], [])  # type: ignore[index]
        self.assertEqual(responses[2]["thread"]["turns"][0]["items"][0]["type"], "userMessage")  # type: ignore[index]
        self.assertEqual(app_server._sessions, {})  # type: ignore[attr-defined]

    def test_remote_resume_and_fork_honor_exclude_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            rollout_path = self._write_rollout(
                codex_home,
                thread_id="thread-exclude-turns",
                source="cli",
                cwd=root,
                preview="exclude turns",
            )
            config = RemoteControlConfig(codex_home=codex_home, auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 1, "method": "thread/resume", "params": {"path": str(rollout_path), "excludeTurns": True}},
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 2, "method": "thread/fork", "params": {"path": str(rollout_path), "excludeTurns": True}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(responses[1]["thread"]["turns"], [])  # type: ignore[index]
        self.assertEqual(responses[2]["thread"]["turns"], [])  # type: ignore[index]
        thread_started = [message for message in ws.sent if message.get("method") == "thread/started"]
        self.assertEqual(thread_started[-1]["params"]["thread"]["turns"], [])  # type: ignore[index]

    def test_remote_resume_redacts_mobile_payloads_like_upstream(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 1,
                    "method": "initialize",
                    "params": {"clientInfo": {"name": "codex_chatgpt_ios_remote", "version": "test"}},
                },
            )
            thread = {
                "turns": [
                    {
                        "items": [
                            {
                                "type": "mcpToolCall",
                                "arguments": {"secret": "arg"},
                                "result": {"content": [{"type": "text", "text": "secret"}]},
                                "error": {"message": "secret error"},
                            },
                            {"type": "imageGeneration", "result": "base64"},
                            {"type": "agentMessage", "text": "kept"},
                        ]
                    }
                ]
            }

            app_server._redact_thread_payload_for_client(thread, client_id="client-a", stream_id="stream-a")  # type: ignore[attr-defined]

        items = thread["turns"][0]["items"]
        self.assertEqual([item["type"] for item in items], ["mcpToolCall", "agentMessage"])
        self.assertEqual(items[0]["arguments"], "[redacted]")
        self.assertEqual(items[0]["result"]["content"][0]["text"], "[redacted]")
        self.assertEqual(items[0]["error"]["message"], "[redacted]")

    def test_remote_thread_loaded_list_returns_loaded_thread_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/start", "params": {}})
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "thread/start", "params": {}})
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 3, "method": "thread/loaded/list", "params": {"limit": 1}},
            )
            first_page = next(message for message in ws.sent if message.get("id") == 3)["result"]
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": 4, "method": "thread/loaded/list", "params": {"cursor": first_page["nextCursor"], "limit": 1}},
            )

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(len(responses[3]["data"]), 1)  # type: ignore[index]
        self.assertIsInstance(responses[3]["data"][0], str)  # type: ignore[index]
        self.assertIsInstance(responses[3]["nextCursor"], str)
        self.assertEqual(len(responses[4]["data"]), 1)  # type: ignore[index]
        self.assertIsNone(responses[4]["nextCursor"])

    def test_remote_app_server_round_trips_server_requests_to_mobile_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "thread/start", "params": {}})
            start_response = next(message for message in ws.sent if message.get("id") == 1)
            thread_id = start_response["result"]["thread"]["id"]  # type: ignore[index]
            session = app_server._session_by_id(thread_id)  # type: ignore[attr-defined]
            with app_server._lock:  # type: ignore[attr-defined]
                app_server._active_turn_clients[thread_id] = (ws, "client-a", "stream-a")  # type: ignore[attr-defined]

            answers: queue.Queue[object] = queue.Queue(maxsize=1)

            def ask_user() -> None:
                provider = session.config.request_user_input_provider
                assert provider is not None
                answers.put(
                    provider(
                        [
                            {
                                "id": "choice",
                                "header": "Choice",
                                "question": "Pick one",
                                "options": [{"label": "A", "description": "Use A"}],
                            }
                        ]
                    )
                )

            thread = threading.Thread(target=ask_user)
            thread.start()
            request = _wait_for_sent_method(ws, "item/tool/requestUserInput")
            self.assertEqual(request["params"]["threadId"], thread_id)  # type: ignore[index]
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": request["id"], "result": {"answers": {"choice": {"answers": ["A"]}}}},
            )
            self.assertEqual(answers.get(timeout=2), {"answers": {"choice": {"answers": ["A"]}}})
            thread.join(timeout=2)

            approvals: queue.Queue[object] = queue.Queue(maxsize=1)

            def ask_approval() -> None:
                provider = session.config.approval_provider
                assert provider is not None
                approvals.put(provider({"tool": "exec_command", "cmd": "echo hi", "reason": "test"}))

            thread = threading.Thread(target=ask_approval)
            thread.start()
            request = _wait_for_sent_method(ws, "item/commandExecution/requestApproval")
            self.assertEqual(request["params"]["command"], "echo hi")  # type: ignore[index]
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {"id": request["id"], "result": {"decision": "acceptForSession"}},
            )
            self.assertEqual(approvals.get(timeout=2), {"decision": "approved_for_session"})
            thread.join(timeout=2)

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 20,
                    "method": "command/exec",
                    "params": {
                        "command": [sys.executable, "-c", "import sys; print('got:' + sys.stdin.read())"],
                        "processId": "proc-1",
                        "streamStdin": True,
                        "streamStdoutStderr": True,
                        "cwd": str(root),
                    },
                },
            )
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 21,
                    "method": "command/exec/write",
                    "params": {
                        "processId": "proc-1",
                        "deltaBase64": base64.b64encode(b"mobile").decode("ascii"),
                        "closeStdin": True,
                    },
                },
            )
            final_response = _wait_for_response_id(ws, 20)
            self.assertEqual(final_response["result"]["exitCode"], 0)  # type: ignore[index]
            output_notification = _wait_for_sent_method(ws, "command/exec/outputDelta")
            self.assertIn(
                "got:mobile",
                base64.b64decode(output_notification["params"]["deltaBase64"]).decode("utf-8"),  # type: ignore[index]
            )

            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 30,
                    "method": "process/spawn",
                    "params": {
                        "command": [sys.executable, "-c", "import sys; print('proc:' + sys.stdin.read())"],
                        "processHandle": "proc-handle-1",
                        "streamStdin": True,
                        "streamStdoutStderr": True,
                        "cwd": str(root),
                    },
                },
            )
            self.assertEqual(_wait_for_response_id(ws, 30)["result"], {})
            app_server.handle_message(
                ws,
                "client-a",
                "stream-a",
                {
                    "id": 31,
                    "method": "process/writeStdin",
                    "params": {
                        "processHandle": "proc-handle-1",
                        "deltaBase64": base64.b64encode(b"phone").decode("ascii"),
                        "closeStdin": True,
                    },
                },
            )
            process_output = _wait_for_sent_method(ws, "process/outputDelta")
            self.assertIn(
                "proc:phone",
                base64.b64decode(process_output["params"]["deltaBase64"]).decode("utf-8"),  # type: ignore[index]
            )
            process_exit = _wait_for_sent_method(ws, "process/exited")
            self.assertEqual(process_exit["params"]["processHandle"], "proc-handle-1")  # type: ignore[index]
            self.assertEqual(process_exit["params"]["exitCode"], 0)  # type: ignore[index]

    def test_remote_turn_streams_agent_and_command_execution_events_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            session = _FakeStreamingSession(root)

            app_server._run_turn(ws, "client-a", "stream-a", session, "run streaming test")  # type: ignore[arg-type]

        methods = [message.get("method") for message in ws.sent]
        self.assertIn("thread/settings/updated", methods)
        self.assertIn("turn/started", methods)
        self.assertLess(methods.index("thread/settings/updated"), methods.index("turn/started"))
        self.assertIn("item/agentMessage/delta", methods)
        self.assertIn("item/commandExecution/outputDelta", methods)
        self.assertIn("item/commandExecution/terminalInteraction", methods)
        self.assertIn("turn/completed", methods)
        settings = next(message for message in ws.sent if message.get("method") == "thread/settings/updated")
        thread_settings = settings["params"]["threadSettings"]  # type: ignore[index]
        self.assertEqual(thread_settings["cwd"], str(root.resolve()))
        self.assertEqual(thread_settings["approvalPolicy"], "never")
        self.assertEqual(thread_settings["sandboxPolicy"]["type"], "workspaceWrite")
        self.assertEqual(thread_settings["model"], session.config.model)
        agent_deltas = [
            message["params"]["delta"]  # type: ignore[index]
            for message in ws.sent
            if message.get("method") == "item/agentMessage/delta"
        ]
        self.assertEqual(agent_deltas, ["我先运行一个会流式输出的小命令。"])

        command_started = [
            message
            for message in ws.sent
            if message.get("method") == "item/started"
            and message["params"]["item"]["type"] == "commandExecution"  # type: ignore[index]
        ]
        self.assertEqual(len(command_started), 1)
        self.assertEqual(command_started[0]["params"]["item"]["status"], "inProgress")  # type: ignore[index]
        output_deltas = [
            message["params"]["delta"]  # type: ignore[index]
            for message in ws.sent
            if message.get("method") == "item/commandExecution/outputDelta"
        ]
        self.assertEqual(output_deltas, ["tick 1\n", "tick 2\n"])
        interaction = next(message for message in ws.sent if message.get("method") == "item/commandExecution/terminalInteraction")
        self.assertEqual(interaction["params"]["itemId"], "call_exec")  # type: ignore[index]
        self.assertEqual(interaction["params"]["stdin"], "hello from phone\n")  # type: ignore[index]
        completed_commands = [
            message["params"]["item"]  # type: ignore[index]
            for message in ws.sent
            if message.get("method") == "item/completed"
            and message["params"]["item"]["type"] == "commandExecution"  # type: ignore[index]
        ]
        self.assertEqual(len(completed_commands), 1)
        self.assertEqual(completed_commands[0]["aggregatedOutput"], "tick 1\ntick 2\n")
        self.assertEqual(completed_commands[0]["exitCode"], 0)
        completed_types = [
            message["params"]["item"]["type"]  # type: ignore[index]
            for message in ws.sent
            if message.get("method") == "item/completed"
        ]
        self.assertNotIn("dynamicToolCall", completed_types)

    def test_remote_turn_settings_overrides_apply_to_session_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            session = _FakeStreamingSession(root)

            app_server._apply_settings_overrides(
                session,  # type: ignore[arg-type]
                {
                    "cwd": str(root / "child"),
                    "model": "gpt-5.5",
                    "effort": "medium",
                    "approvalPolicy": "on-request",
                    "sandboxPolicy": {
                        "type": "workspaceWrite",
                        "writableRoots": [str(root / "extra")],
                        "networkAccess": False,
                        "excludeTmpdirEnvVar": True,
                        "excludeSlashTmp": True,
                    },
                    "collaborationMode": {
                        "mode": "plan",
                        "settings": {"model": "gpt-5.5", "reasoning_effort": "medium"},
                    },
                },
            )

        self.assertEqual(session.config.resolved_cwd(), (root / "child").resolve())
        self.assertEqual(session.config.model, "gpt-5.5")
        self.assertEqual(session.config.model_reasoning_effort, "medium")
        self.assertEqual(session.config.approval_policy, "on-request")
        self.assertEqual(session.config.sandbox, "workspace-write")
        self.assertEqual(session.config.writable_roots, (str(root / "extra"),))
        self.assertEqual(session.config.collaboration_mode, "Plan")

    def test_remote_turn_maps_apply_patch_to_file_change_and_strips_empty_exec_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            session = _FakePatchAndQuietCommandSession(root)

            app_server._run_turn(ws, "client-a", "stream-a", session, "patch and check")  # type: ignore[arg-type]

        completed_items = [
            message["params"]["item"]  # type: ignore[index]
            for message in ws.sent
            if message.get("method") == "item/completed"
        ]
        file_change = next(item for item in completed_items if item["type"] == "fileChange")
        self.assertEqual(file_change["status"], "completed")
        self.assertEqual(file_change["changes"][0]["path"], "demo.py")
        self.assertEqual(file_change["changes"][0]["kind"], {"type": "update", "movePath": None})
        self.assertIn("print('new')", file_change["changes"][0]["diff"])

        quiet_command = next(item for item in completed_items if item["type"] == "commandExecution")
        self.assertEqual(quiet_command["aggregatedOutput"], "")
        self.assertNotIn("Chunk ID", quiet_command["aggregatedOutput"])

    def test_remote_turn_notifications_are_broadcast_to_thread_subscribers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root),
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(
                        json.dumps(message)
                    ),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            session = _FakeStreamingSession(root)
            ws_a = _FakeWebSocket()
            ws_b = _FakeWebSocket()

            app_server._sessions[session.state.thread_id] = session  # type: ignore[assignment]
            app_server._subscribe_thread(session, ws_a, "client-a", "stream-a")  # type: ignore[arg-type]
            app_server._subscribe_thread(session, ws_b, "client-b", "stream-b")  # type: ignore[arg-type]
            app_server._run_turn(ws_a, "client-a", "stream-a", session, "run streaming test")  # type: ignore[arg-type]

            methods_a = [message.get("method") for message in ws_a.sent]
            methods_b = [message.get("method") for message in ws_b.sent]
            self.assertIn("item/agentMessage/delta", methods_a)
            self.assertIn("item/agentMessage/delta", methods_b)
            self.assertIn("item/commandExecution/outputDelta", methods_a)
            self.assertIn("item/commandExecution/outputDelta", methods_b)

            app_server.handle_message(
                ws_b,
                "client-b",
                "stream-b",
                {
                    "id": 9,
                    "method": "thread/unsubscribe",
                    "params": {"threadId": session.state.thread_id},
                },
            )
            unsubscribe_response = next(message for message in ws_b.sent if message.get("id") == 9)
            self.assertEqual(unsubscribe_response["result"], {"status": "unsubscribed"})
            ws_a.sent.clear()
            ws_b.sent.clear()

            app_server._run_turn(ws_a, "client-a", "stream-a", session, "run streaming test")  # type: ignore[arg-type]

            self.assertIn("turn/started", [message.get("method") for message in ws_a.sent])
            self.assertNotIn("turn/started", [message.get("method") for message in ws_b.sent])

    def test_remote_resume_existing_thread_attaches_without_replacing_live_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_config = CodexConfig(
                cwd=root,
                codex_home=root / "codex-home",
                auth_codex_home=root / "auth-home",
                skip_git_repo_check=True,
                include_web_search_tool=False,
                memory_tool_enabled=True,
                remote_compaction="required",
                sandbox="danger-full-access",
                approval_policy="on-request",
            )
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig.from_codex_config(base_config),
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(
                        json.dumps(message)
                    ),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws_a = _FakeWebSocket()
            ws_b = _FakeWebSocket()

            app_server.handle_message(ws_a, "client-a", "stream-a", {"id": 1, "method": "thread/start", "params": {}})
            start_response = next(message for message in ws_a.sent if message.get("id") == 1)
            thread_id = start_response["result"]["thread"]["id"]  # type: ignore[index]
            live_session = app_server._sessions[thread_id]  # type: ignore[index]
            self.assertFalse(live_session.config.include_web_search_tool)
            self.assertTrue(live_session.config.memory_tool_enabled)
            self.assertEqual(live_session.config.remote_compaction, "required")
            self.assertEqual(live_session.config.sandbox, "danger-full-access")
            self.assertEqual(live_session.config.approval_policy, "on-request")

            app_server.handle_message(
                ws_b,
                "client-b",
                "stream-b",
                {"id": 2, "method": "thread/resume", "params": {"threadId": thread_id}},
            )

            self.assertIs(app_server._sessions[thread_id], live_session)  # type: ignore[index]
            self.assertIn(("client-b", "stream-b"), app_server._thread_subscribers[thread_id])  # type: ignore[index]

    def test_remote_initialize_does_not_replay_loaded_thread_or_subscribe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ws = _FakeWebSocket()
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root),
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(
                        json.dumps(message)
                    ),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=root / "codex-home",
                    auth_codex_home=root / "auth-home",
                    skip_git_repo_check=True,
                )
            )
            app_server._sessions[session.state.thread_id] = session  # type: ignore[index]

            app_server.handle_message(ws, "client-a", "stream-a", {"id": 1, "method": "initialize", "params": {}})

            started = [message for message in ws.sent if message.get("method") == "thread/started"]
            self.assertEqual(started, [])
            self.assertNotIn(session.state.thread_id, app_server._thread_subscribers)  # type: ignore[operator]

    def test_remote_loaded_thread_announcement_targets_initialized_clients(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ws_remote = _FakeWebSocket()
            service = type(
                "Service",
                (),
                {
                    "config": RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root),
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(
                        json.dumps(message)
                    ),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                    "initialized_client_refs": lambda self: [(ws_remote, "client-remote", "stream-remote")],
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=root / "codex-home",
                    auth_codex_home=root / "auth-home",
                    skip_git_repo_check=True,
                )
            )

            app_server._announce_loaded_thread(session)  # type: ignore[arg-type]

            started = [message for message in ws_remote.sent if message.get("method") == "thread/started"]
            self.assertEqual(len(started), 1)
            self.assertEqual(started[0]["params"]["thread"]["id"], session.state.thread_id)  # type: ignore[index]
            self.assertIn(("client-remote", "stream-remote"), app_server._thread_subscribers[session.state.thread_id])  # type: ignore[index]

    def test_remote_app_server_handles_account_login_and_logout_requests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", auth_codex_home=root / "auth-home", cwd=root)
            service = type(
                "Service",
                (),
                {
                    "config": config,
                    "status": "connected",
                    "installation_id": "install-test",
                    "environment_id": "env-test",
                    "send_message": lambda self, ws, client_id, stream_id, message: ws.send(json.dumps(message)),
                    "send_notification": lambda self, ws, client_id, stream_id, method, params: ws.send(
                        json.dumps({"method": method, "params": params})
                    ),
                },
            )()
            app_server = _RemoteAppServer(service)  # type: ignore[arg-type]
            ws = _FakeWebSocket()
            previous_key = os.environ.pop("OPENAI_API_KEY", None)
            try:
                app_server.handle_message(
                    ws,
                    "client-a",
                    "stream-a",
                    {"id": 1, "method": "account/login/start", "params": {"type": "apiKey", "apiKey": "sk-test"}},
                )
                app_server.handle_message(ws, "client-a", "stream-a", {"id": 2, "method": "account/read", "params": {}})
                app_server.handle_message(ws, "client-a", "stream-a", {"id": 3, "method": "account/logout", "params": {}})
                app_server.handle_message(ws, "client-a", "stream-a", {"id": 4, "method": "account/read", "params": {}})
            finally:
                if previous_key is not None:
                    os.environ["OPENAI_API_KEY"] = previous_key

        responses = {message["id"]: message["result"] for message in ws.sent if "id" in message}
        self.assertEqual(responses[1], {"type": "apiKey"})
        self.assertEqual(responses[2]["account"], {"type": "apiKey"})  # type: ignore[index]
        self.assertEqual(responses[3], {})
        self.assertIsNone(responses[4]["account"])  # type: ignore[index]
        self.assertTrue(any(message.get("method") == "account/login/completed" for message in ws.sent))

    def test_login_free_enroll_request_shape_is_serializable(self) -> None:
        request = build_enroll_request(
            name="test-machine",
            installation_id="11111111-1111-4111-8111-111111111111",
            app_server_version="1.0.0",
        )
        self.assertEqual(request.name, "test-machine")
        self.assertEqual(request.installation_id, "11111111-1111-4111-8111-111111111111")
        if sys.platform == "darwin":
            self.assertEqual(request.os, "macos")
        else:
            self.assertNotEqual(request.os, "darwin")
        self.assertNotEqual(request.arch, "arm64")

    def test_remote_control_transport_headers_match_upstream_account_id_shape(self) -> None:
        auth = RemoteControlAuth(access_token="access-token", account_id="account-123")
        enrollment = RemoteControlEnrollment(
            account_id="account-123",
            environment_id="env_123",
            server_id="srv_123",
            server_name="test-machine",
        )

        auth_headers = _remote_auth_headers(auth)
        self.assertEqual(auth_headers, {"Authorization": "Bearer access-token"})

        websocket_headers = _websocket_headers(
            auth,
            enrollment,
            installation_id="install-123",
            subscribe_cursor=None,
        )
        names = [header.split(":", 1)[0].lower() for header in websocket_headers]
        self.assertEqual(names.count(REMOTE_CONTROL_ACCOUNT_ID_HEADER), 1)
        self.assertIn(f"{REMOTE_CONTROL_ACCOUNT_ID_HEADER}: account-123", websocket_headers)

    def test_remote_control_default_identity_honors_upstream_originator_override(self) -> None:
        previous = os.environ.get("CODEX_INTERNAL_ORIGINATOR_OVERRIDE")
        try:
            os.environ["CODEX_INTERNAL_ORIGINATOR_OVERRIDE"] = "Codex Desktop"
            originator, suffix = _remote_control_client_identity(None, None)
        finally:
            if previous is None:
                os.environ.pop("CODEX_INTERNAL_ORIGINATOR_OVERRIDE", None)
            else:
                os.environ["CODEX_INTERNAL_ORIGINATOR_OVERRIDE"] = previous
        self.assertEqual(originator, "Codex Desktop")
        self.assertIsNone(suffix)

    def test_remote_control_discards_cached_enrollment_with_wrong_fixed_name(self) -> None:
        class _NoopWebSocketApp:
            instances: list["_NoopWebSocketApp"] = []

            def __init__(self, *_args: object, **_kwargs: object) -> None:
                self.args = _args
                self.kwargs = _kwargs
                self.run_kwargs: dict[str, object] | None = None
                self.instances.append(self)

            def run_forever(self, **_kwargs: object) -> bool:
                self.run_kwargs = dict(_kwargs)
                return True

        with tempfile.TemporaryDirectory() as tmp:
            _NoopWebSocketApp.instances.clear()
            root = Path(tmp)
            config = RemoteControlConfig(codex_home=root / "codex-home", cwd=root)
            service = RemoteControlService(config)
            stale = RemoteControlEnrollment(
                account_id="account-123",
                environment_id="env-old",
                server_id="srv-old",
                server_name="python-macbookpro-lan",
            )
            service.state.save_enrollment(service.target.websocket_url, "account-123", "Codex Desktop", stale)
            sibling = RemoteControlEnrollment(
                account_id="account-123",
                environment_id="env-sibling",
                server_id="srv-sibling",
                server_name="python-macbookpro-lan",
            )
            service.state.save_enrollment(service.target.websocket_url, "account-123", None, sibling)
            fresh = RemoteControlEnrollment(
                account_id="account-123",
                environment_id="env-new",
                server_id="srv-new",
                server_name=REQUIRED_REMOTE_CONTROL_SERVER_NAME,
            )

            with (
                patch.object(
                    remote_control_service_module,
                    "_load_remote_control_auth",
                    return_value=RemoteControlAuth(access_token="access-token", account_id="account-123"),
                ),
                patch.object(remote_control_service_module, "enroll_remote_control_server", return_value=fresh) as enroll,
                patch.object(remote_control_service_module, "_websocket_headers", return_value=[]) as headers,
                patch("websocket.WebSocketApp", _NoopWebSocketApp),
            ):
                service._connect_once()

            enroll.assert_called_once()
            used_enrollment = headers.call_args.args[1]
            self.assertEqual(used_enrollment.environment_id, "env-new")
            self.assertEqual(used_enrollment.server_id, "srv-new")
            saved = service.state.enrollment(service.target.websocket_url, "account-123", None)
            self.assertIsNotNone(saved)
            assert saved is not None
            self.assertEqual(saved.server_name, REQUIRED_REMOTE_CONTROL_SERVER_NAME)
            self.assertEqual(saved.environment_id, "env-new")
            state = json.loads((config.codex_home / "remote-control.json").read_text(encoding="utf-8"))
            self.assertEqual(list(state["enrollments"].values())[0]["server_id"], "srv-new")
            self.assertEqual(len(state["enrollments"]), 1)
            self.assertEqual(len(_NoopWebSocketApp.instances), 1)
            self.assertEqual(
                _NoopWebSocketApp.instances[0].run_kwargs,
                {
                    "ping_interval": 0,
                    "ping_timeout": None,
                    "http_proxy_host": None,
                    "http_proxy_port": None,
                },
            )

    def test_remote_control_trace_redacts_credentials_but_preserves_token_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "trace.jsonl"
            previous_py = os.environ.get("PY_CODEX_RC_TRACE_FILE")
            previous_rs = os.environ.get("CODEX_RC_TRACE_FILE")
            try:
                os.environ["PY_CODEX_RC_TRACE_FILE"] = str(path)
                os.environ.pop("CODEX_RC_TRACE_FILE", None)
                remote_control_trace.append_event(
                    {
                        "event": "unit",
                        "Authorization": "Bearer secret-token",
                        "access_token": "secret-token",
                        "x-codex-installation-id": "11111111-1111-4111-8111-111111111111",
                        "tokenUsage": {"totalTokens": 12, "inputTokens": 7, "outputTokens": 5},
                    }
                )
            finally:
                if previous_py is None:
                    os.environ.pop("PY_CODEX_RC_TRACE_FILE", None)
                else:
                    os.environ["PY_CODEX_RC_TRACE_FILE"] = previous_py
                if previous_rs is None:
                    os.environ.pop("CODEX_RC_TRACE_FILE", None)
                else:
                    os.environ["CODEX_RC_TRACE_FILE"] = previous_rs

            payload = json.loads(path.read_text(encoding="utf-8").strip())
        self.assertEqual(payload["Authorization"], "<redacted>")
        self.assertEqual(payload["access_token"], "<redacted>")
        self.assertEqual(payload["x-codex-installation-id"], "<redacted>")
        self.assertEqual(payload["tokenUsage"], {"totalTokens": 12, "inputTokens": 7, "outputTokens": 5})

    def test_remote_control_websocket_timing_matches_upstream(self) -> None:
        self.assertEqual(REMOTE_CONTROL_WEBSOCKET_PING_INTERVAL_SECONDS, 10)
        self.assertEqual(REMOTE_CONTROL_WEBSOCKET_PONG_TIMEOUT_SECONDS, 60)
        self.assertEqual(
            REMOTE_CONTROL_WEBSOCKET_CLIENT_PING_TIMEOUT_SECONDS,
            REMOTE_CONTROL_WEBSOCKET_PONG_TIMEOUT_SECONDS,
        )

    def test_remote_control_heartbeat_closes_after_upstream_pong_deadline(self) -> None:
        class _HeartbeatWebSocket:
            def __init__(self) -> None:
                self.closed = threading.Event()
                self.pings = 0

            def send(self, _payload: str, *, opcode: object | None = None) -> None:
                self.pings += 1

            def close(self) -> None:
                self.closed.set()

        with tempfile.TemporaryDirectory() as tmp:
            service = RemoteControlService(
                RemoteControlConfig(codex_home=Path(tmp) / "codex-home", cwd=Path(tmp), quiet=False)
            )
            service.status = "connected"
            ws = _HeartbeatWebSocket()
            stderr = StringIO()
            with (
                patch.object(remote_control_service_module, "REMOTE_CONTROL_WEBSOCKET_PING_INTERVAL_SECONDS", 0.01),
                patch.object(remote_control_service_module, "REMOTE_CONTROL_WEBSOCKET_PONG_TIMEOUT_SECONDS", 0.03),
                redirect_stdout(StringIO()),
                redirect_stderr(stderr),
            ):
                service._start_heartbeat(ws)
                self.assertTrue(ws.closed.wait(1.0))
                service._stop_heartbeat()
            self.assertGreaterEqual(ws.pings, 1)
            self.assertEqual(service.status, "connecting")
            self.assertIn("remote control websocket pong timeout", stderr.getvalue())

    def test_remote_control_server_name_override_is_rejected(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
        try:
            os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = "python-codex-test"
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError):
                    RemoteControlConfig.from_codex_config(
                        CodexConfig(cwd=Path(tmp), codex_home=Path(tmp))
                    )
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = previous

    def test_remote_control_default_server_name_matches_upstream_host_identity(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
        try:
            os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            self.assertEqual(config.server_name, REQUIRED_REMOTE_CONTROL_SERVER_NAME)
            with self.assertRaises(ValueError):
                RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp), server_name="python-macbookpro-lan")
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = previous

    def test_python_remote_control_refuses_official_desktop_state_home(self) -> None:
        official_home = Path.home() / ".codex"
        with self.assertRaises(RemoteControlError):
            RemoteControlConfig(codex_home=official_home, cwd=Path.cwd())

        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(
                codex_home=Path(tmp) / "codex-python",
                auth_codex_home=official_home,
                cwd=Path(tmp),
            )
            self.assertEqual(config.auth_codex_home, official_home.resolve())
            self.assertNotEqual(config.codex_home, official_home.resolve())

    def test_shared_interactive_remote_control_name_is_stable(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
        try:
            os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            self.assertEqual(_shared_remote_control_server_name(), REQUIRED_REMOTE_CONTROL_SERVER_NAME)
            os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = "python-custom-lan"
            with self.assertRaises(RuntimeError):
                _shared_remote_control_server_name()
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = previous

    def test_remote_control_installation_id_can_be_overridden_for_live_diagnostics(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_INSTALLATION_ID")
        try:
            os.environ["PY_CODEX_REMOTE_CONTROL_INSTALLATION_ID"] = "diagnostic-installation-id"
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
                service = RemoteControlService(config)
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_INSTALLATION_ID", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_INSTALLATION_ID"] = previous
        self.assertEqual(service.installation_id, "diagnostic-installation-id")

    def test_remote_control_installation_id_uses_upstream_style_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = remote_control_service_module._RemoteControlPersistentState(root)  # type: ignore[attr-defined]
            installation_id = state.installation_id()
            self.assertTrue((root / "installation_id").is_file())
            self.assertEqual((root / "installation_id").read_text(encoding="utf-8").strip(), installation_id)

            legacy_state = {
                "installation_id": "legacy-json-installation",
                "enrollments": {},
            }
            (root / "remote-control.json").write_text(json.dumps(legacy_state), encoding="utf-8")
            self.assertEqual(state.installation_id(), installation_id)

    def test_stop_human_message_matches_official_status_text(self) -> None:
        self.assertEqual(remote_control_stop_human_message("stopped"), "Remote control stopped.")
        self.assertEqual(remote_control_stop_human_message("notRunning"), "Remote control is not running.")

    def test_cli_help_exposes_remote_control_without_routing_to_chat(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "codex", "--help"],
            text=True,
            capture_output=True,
            cwd=os.getcwd(),
            timeout=30,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("remote-control", completed.stdout)

    def test_cli_remote_control_stop_does_not_require_official_codex_binary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = dict(os.environ)
            env["CODEX_PY_HOME"] = tmp
            completed = subprocess.run(
                [sys.executable, "-m", "codex", "remote-control", "--json", "stop"],
                text=True,
                capture_output=True,
                cwd=os.getcwd(),
                env=env,
                timeout=30,
                check=False,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout), {"status": "notRunning"})

def _load_success_trace_fixture() -> list[dict[str, object]]:
    path = Path(__file__).parent / "fixtures" / "remote_control_success_trace.jsonl"
    host = REQUIRED_REMOTE_CONTROL_SERVER_NAME
    host_b64 = base64.b64encode(host.encode("utf-8")).decode("ascii")
    codex_home = str((Path.home() / ".codex-python").resolve())
    workspace_cwd = str(Path.cwd().resolve())
    user_codex_skills = str((Path.home() / ".codex" / "skills").resolve())
    return [
        _expand_success_trace_placeholders(
            json.loads(line),
            host=host,
            host_b64=host_b64,
            codex_home=codex_home,
            workspace_cwd=workspace_cwd,
            user_codex_skills=user_codex_skills,
        )
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _expand_success_trace_placeholders(
    value: object,
    *,
    host: str,
    host_b64: str,
    codex_home: str,
    workspace_cwd: str,
    user_codex_skills: str,
) -> object:
    if isinstance(value, dict):
        return {
            key: _expand_success_trace_placeholders(
                item,
                host=host,
                host_b64=host_b64,
                codex_home=codex_home,
                workspace_cwd=workspace_cwd,
                user_codex_skills=user_codex_skills,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _expand_success_trace_placeholders(
                item,
                host=host,
                host_b64=host_b64,
                codex_home=codex_home,
                workspace_cwd=workspace_cwd,
                user_codex_skills=user_codex_skills,
            )
            for item in value
        ]
    if value == "__REMOTE_CONTROL_SERVER_NAME__":
        return host
    if value == "__REMOTE_CONTROL_SERVER_NAME_B64__":
        return host_b64
    if value == "__CODEX_HOME__":
        return codex_home
    if value == "__WORKSPACE_CWD__":
        return workspace_cwd
    if value == "__USER_CODEX_SKILLS__":
        return user_codex_skills
    return value


def _first_trace_event(records: list[dict[str, object]], event: str) -> dict[str, object]:
    for record in records:
        if record.get("event") == event:
            return record
    raise AssertionError(f"trace fixture missing event {event}")


def _client_messages(records: list[dict[str, object]], client_id: str) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    for record in records:
        if record.get("event") != "remote_control_websocket_client_recv_raw":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict) or payload.get("client_id") != client_id:
            continue
        message = payload.get("message")
        if isinstance(message, dict):
            messages.append(message)
    return messages


def _server_messages(records: list[dict[str, object]], client_id: str) -> list[dict[str, object]]:
    messages: list[dict[str, object]] = []
    for record in records:
        if record.get("event") != "remote_control_websocket_server_send":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict) or payload.get("client_id") != client_id:
            continue
        message = payload.get("message")
        if isinstance(message, dict):
            messages.append(message)
    return messages


def _server_result_user_agent(
    records: list[dict[str, object]],
    *,
    client_id: str,
    response_id: object,
) -> str:
    for message in _server_messages(records, client_id):
        if message.get("id") != response_id:
            continue
        result = message.get("result")
        if isinstance(result, dict) and isinstance(result.get("userAgent"), str):
            return result["userAgent"]
    raise AssertionError(f"trace fixture missing userAgent response {response_id!r}")


def _server_notification(
    records: list[dict[str, object]],
    client_id: str,
    method: str,
) -> dict[str, object]:
    for message in _server_messages(records, client_id):
        if message.get("method") == method:
            return message
    raise AssertionError(f"trace fixture missing notification {method}")


def _wait_for_sent_method(ws: _FakeWebSocket, method: str) -> dict[str, object]:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        for message in ws.sent:
            if message.get("method") == method:
                return message
        time.sleep(0.01)
    raise AssertionError(f"server request {method} was not sent")


def _wait_for_response_id(ws: _FakeWebSocket, response_id: int) -> dict[str, object]:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        for message in ws.sent:
            if message.get("id") == response_id and "result" in message:
                return message
        time.sleep(0.01)
    raise AssertionError(f"response {response_id} was not sent")


if __name__ == "__main__":
    unittest.main()
