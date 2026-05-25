from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import base64

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from codex.remote_control import (
    ClientEnvelope,
    REMOTE_CONTROL_COMPAT_VERSION,
    RemoteControlConfig,
    RemoteControlError,
    RemoteControlReadyStatus,
    RemoteControlService,
    ServerEnvelope,
    _ClientSegmentReassembler,
    _OutboundBuffer,
    _RemoteAppServer,
    _last_user_message_index,
    _preview_from_history,
    _split_server_envelope_for_transport,
    _thread_item_from_response_item,
    build_enroll_request,
    normalize_remote_control_url,
    remote_control_official_args,
    remote_control_start_human_lines,
    remote_control_start_json_output,
    remote_control_stop_human_message,
    run_native_remote_control,
)
from codex.types import CodexConfig


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))


class CodexRemoteControlTests(unittest.TestCase):
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
        self.assertTrue(response["userAgent"].startswith("test/"))

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
        self.assertTrue(response["userAgent"].startswith("codex_cli_rs/"))

    def test_remote_control_desktop_client_identity_requires_explicit_compat_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(
                codex_home=Path(tmp),
                cwd=Path(tmp),
                app_server_client_name="Codex Desktop",
                app_server_client_version="0.133.0",
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
        self.assertTrue(response["userAgent"].startswith("codex_cli_rs/"))

        with tempfile.TemporaryDirectory() as tmp:
            config = RemoteControlConfig(
                codex_home=Path(tmp),
                cwd=Path(tmp),
                app_server_client_name="Codex Desktop",
                app_server_client_version="0.133.0",
                allow_desktop_compat_identity=True,
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
        self.assertIn("(Codex Desktop; 0.133.0)", response["userAgent"])

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

    def test_remote_control_server_name_can_be_overridden_for_live_diagnostics(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
        try:
            os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = "python-codex-test"
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig.from_codex_config(
                    CodexConfig(cwd=Path(tmp), codex_home=Path(tmp))
                )
            self.assertEqual(config.server_name, "python-codex-test")
        finally:
            if previous is None:
                os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            else:
                os.environ["PY_CODEX_REMOTE_CONTROL_NAME"] = previous

    def test_remote_control_default_server_name_matches_official_hostname(self) -> None:
        previous = os.environ.get("PY_CODEX_REMOTE_CONTROL_NAME")
        try:
            os.environ.pop("PY_CODEX_REMOTE_CONTROL_NAME", None)
            with tempfile.TemporaryDirectory() as tmp:
                config = RemoteControlConfig(codex_home=Path(tmp), cwd=Path(tmp))
            self.assertTrue(config.server_name)
            self.assertFalse(config.server_name.startswith("python-"))
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
