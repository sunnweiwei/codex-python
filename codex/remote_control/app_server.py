from __future__ import annotations

import base64
import json
import os
import queue
import subprocess
import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..auth import (
    auth_json_path,
    complete_device_code_login,
    login_with_api_key,
    request_device_code,
    run_browser_login,
    _write_auth_json,
)
from ..core import CodexSession
from ..types import CodexConfig
from .protocol import _DEFERRED_RESPONSE, _DeferredResponse
from .types import RemoteControlError
from .utils import (
    _now_ms,
    _optional_int,
    _remote_log,
)
from .app_helpers import (
    _RemoteCommandProcess,
    _account_logout,
    _account_rate_limits_response,
    _account_read_response,
    _api_session_source,
    _approval_server_request,
    _auth_mode_from_account,
    _auth_status_response,
    _cap_process_chunk,
    _cancel_remote_process_timeout,
    _close_remote_process_fds,
    _collaboration_mode_list_response,
    _collab_agent_item_from_tool_completed,
    _collab_agent_item_from_tool_started,
    _command_exec_argv,
    _command_exec_cwd,
    _command_exec_env,
    _command_exec_is_streaming,
    _command_exec_output_bytes_cap,
    _command_exec_response,
    _command_exec_timeout_seconds,
    _command_execution_item,
    _command_execution_item_from_tool_completed,
    _command_execution_item_from_tool_started,
    _config_read_response,
    _config_write_response,
    _context_compaction_item,
    _conversation_summary_response,
    _cwd_filter,
    _decode_process_capture,
    _empty_plugin_detail,
    _file_change_item_from_apply_patch_completed,
    _file_change_patch_updated_payload,
    _find_rollout_path,
    _fs_copy,
    _fs_create_directory,
    _fs_get_metadata_response,
    _fs_path,
    _fs_read_directory_response,
    _fs_read_file_response,
    _fs_remove,
    _fs_write_file,
    _fuzzy_file_search_response,
    _git_diff_to_remote_response,
    _goal_status_from_param,
    _initialize_response,
    _input_text,
    _is_agent_message_delta,
    _initialize_client_name,
    _join_reader_threads,
    _jsonrpc_error,
    _last_user_message_index,
    _marketplace_empty_response,
    _model_list_response,
    _plugin_share_empty_response,
    _preview_from_history,
    _process_spawn_argv,
    _process_spawn_cwd,
    _process_spawn_output_bytes_cap,
    _process_spawn_timeout_seconds,
    _process_stream_cap_reached,
    _remote_approval_decision_grants,
    _remote_control_status_payload,
    _reasoning_delta_notification,
    _request_user_input_question_payload,
    _redact_thread_resume_payloads,
    _resize_remote_process_pty,
    _response_item_is_assistant_message,
    _response_item_is_live_tool_echo,
    _response_item_protocol_id,
    _rollout_paths,
    _rollout_cwd,
    _sandbox_policy_payload,
    _should_redact_thread_resume_payloads,
    _source_kinds_filter,
    _spawn_remote_process,
    _start_remote_process_timeout,
    _terminal_interaction_from_write_stdin,
    _terminate_remote_process,
    _thread_item_from_response_item,
    _thread_payload,
    _thread_payload_from_rollout,
    _thread_settings_payload,
    _thread_source_matches,
    _token_usage_payload,
    _turn_plan_update_payload,
    _turn_payload,
    _turns_from_history,
    _utc_now_iso,
    _validate_command_exec_params,
    _write_remote_process,
)

if TYPE_CHECKING:
    from .service import RemoteControlService


class _JsonRpcRemoteControlError(RemoteControlError):
    def __init__(self, message: str, *, code: int = -32000) -> None:
        super().__init__(message)
        self.code = code


def _config_sandbox_from_remote_policy(policy: Any, template: CodexConfig) -> tuple[str, str, tuple[Path | str, ...], bool, bool]:
    if isinstance(policy, str):
        normalized = policy.strip()
        if normalized in {"danger-full-access", "dangerFullAccess"}:
            return "danger-full-access", template.network_access, template.writable_roots, False, False
        if normalized in {"read-only", "readOnly"}:
            return "read-only", template.network_access, template.writable_roots, False, False
        if normalized in {"workspace-write", "workspaceWrite"}:
            return "workspace-write", template.network_access, template.writable_roots, False, False
        return template.sandbox, template.network_access, template.writable_roots, template.exclude_tmpdir_env_var, template.exclude_slash_tmp
    if not isinstance(policy, dict):
        return template.sandbox, template.network_access, template.writable_roots, template.exclude_tmpdir_env_var, template.exclude_slash_tmp
    policy_type = str(policy.get("type") or "")
    network_access = "enabled" if policy.get("networkAccess") is True else "restricted"
    if policy_type == "dangerFullAccess":
        return "danger-full-access", network_access, template.writable_roots, False, False
    if policy_type == "readOnly":
        return "read-only", network_access, template.writable_roots, False, False
    roots = policy.get("writableRoots")
    writable_roots = tuple(str(root) for root in roots if isinstance(root, str) and root) if isinstance(roots, list) else template.writable_roots
    return (
        "workspace-write",
        network_access,
        writable_roots,
        bool(policy.get("excludeTmpdirEnvVar")),
        bool(policy.get("excludeSlashTmp")),
    )


def _config_collaboration_mode_from_remote(value: Any, fallback: str) -> str:
    raw = value.get("mode") if isinstance(value, dict) else value
    if not isinstance(raw, str) or not raw:
        return fallback
    normalized = raw.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized == "plan":
        return "Plan"
    if normalized == "pair_programming":
        return "Pair Programming"
    if normalized == "execute":
        return "Execute"
    return "Default"


def _remote_debug_enabled() -> bool:
    return os.environ.get("PY_CODEX_REMOTE_CONTROL_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _request_param_summary(params: dict[str, Any]) -> dict[str, Any]:
    visible_keys = (
        "threadId",
        "path",
        "cursor",
        "limit",
        "sortKey",
        "sortDirection",
        "includeTurns",
        "excludeTurns",
        "itemsView",
        "sourceKinds",
        "cwd",
    )
    summary: dict[str, Any] = {}
    for key in visible_keys:
        if key not in params:
            continue
        value = params.get(key)
        if key in {"path", "cwd"} and isinstance(value, str) and len(value) > 160:
            summary[key] = f"...{value[-157:]}"
        elif key == "cursor" and isinstance(value, str) and len(value) > 160:
            summary[key] = f"{value[:157]}..."
        else:
            summary[key] = value
    return summary


def _result_metric_summary(result: Any) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if isinstance(result, dict):
        if isinstance(result.get("data"), list):
            metrics["data_len"] = len(result["data"])
        thread = result.get("thread")
        if isinstance(thread, dict):
            turns = thread.get("turns")
            metrics["thread_id"] = thread.get("id")
            metrics["turns_len"] = len(turns) if isinstance(turns, list) else None
            metrics["thread_updated_at"] = thread.get("updatedAt")
    try:
        metrics["result_bytes"] = len(json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    except Exception:
        pass
    return metrics


def _remote_config_override_kwargs(
    template: CodexConfig,
    params: dict[str, Any],
    *,
    default_cwd: Path | str | None = None,
) -> dict[str, Any]:
    cwd = params.get("cwd")
    model = params.get("model")
    model_provider = params.get("modelProvider")
    sandbox = params.get("sandbox") or params.get("sandboxPolicy")
    approval = params.get("approvalPolicy")
    effort = params.get("effort")
    summary = params.get("summary")
    collaboration_payload = params.get("collaborationMode")
    if isinstance(collaboration_payload, dict):
        settings = collaboration_payload.get("settings")
        if isinstance(settings, dict):
            if not isinstance(model, str) or not model:
                model = settings.get("model")
            if not isinstance(effort, str) or not effort:
                effort = settings.get("reasoning_effort")
    service_tier = params.get("serviceTier") if "serviceTier" in params else template.service_tier
    sandbox_mode, network_access, writable_roots, exclude_tmpdir_env_var, exclude_slash_tmp = (
        _config_sandbox_from_remote_policy(sandbox, template)
    )
    return {
        "cwd": Path(cwd).expanduser() if isinstance(cwd, str) and cwd else (default_cwd or template.cwd),
        "model": model if isinstance(model, str) and model else template.model,
        "model_provider_id": model_provider
        if isinstance(model_provider, str) and model_provider
        else template.model_provider_id,
        "sandbox": sandbox_mode,
        "network_access": network_access,
        "writable_roots": writable_roots,
        "exclude_tmpdir_env_var": exclude_tmpdir_env_var,
        "exclude_slash_tmp": exclude_slash_tmp,
        "approval_policy": approval
        if approval in {"untrusted", "on-failure", "on-request", "never"}
        else template.approval_policy,
        "model_reasoning_effort": effort if isinstance(effort, str) and effort else template.model_reasoning_effort,
        "model_reasoning_summary": summary if isinstance(summary, str) and summary else template.model_reasoning_summary,
        "service_tier": service_tier if isinstance(service_tier, str) and service_tier else None,
        "collaboration_mode": _config_collaboration_mode_from_remote(
            collaboration_payload,
            template.collaboration_mode,
        ),
    }


class _RemoteAppServer:
    def __init__(self, service: RemoteControlService):
        self.service = service
        self._lock = threading.RLock()
        self._sessions: dict[str, CodexSession] = {}
        self._thread_subscribers: dict[str, dict[tuple[str, str], tuple[Any, str, str]]] = {}
        self._active_turn_clients: dict[str, tuple[Any, str, str]] = {}
        self._turn_threads: dict[str, threading.Thread] = {}
        self._thread_names: dict[str, str] = {}
        self._thread_git_info: dict[str, dict[str, str | None]] = {}
        self._thread_elicitation_counts: dict[str, int] = {}
        self._thread_memory_modes: dict[str, str] = {}
        self._remote_environments: dict[str, str] = {}
        self._fs_watches: dict[tuple[str, str], Path] = {}
        self._fuzzy_search_sessions: dict[tuple[str, str], list[str]] = {}
        self._command_processes: dict[tuple[str, str], _RemoteCommandProcess] = {}
        self._process_processes: dict[tuple[str, str], _RemoteCommandProcess] = {}
        self._pending_server_requests: dict[Any, queue.Queue[dict[str, Any]]] = {}
        self._pending_server_request_targets: dict[Any, tuple[str, str, str]] = {}
        self._client_names: dict[tuple[str, str], str | None] = {}
        self._thread_active_flag_counts: dict[str, dict[str, int]] = {}
        self._next_server_request_id = 1

    def close_client(self, client_id: str, stream_id: str) -> None:
        canceled_requests: list[tuple[Any, queue.Queue[dict[str, Any]]]] = []
        with self._lock:
            for key in list(self._client_names):
                if key[0] == client_id and (stream_id is None or key[1] == stream_id):
                    self._client_names.pop(key, None)
            for thread_id, target in list(self._active_turn_clients.items()):
                if target[1] == client_id and (stream_id is None or target[2] == stream_id):
                    self._active_turn_clients.pop(thread_id, None)
            for key in list(self._fs_watches):
                if key[0] == client_id:
                    self._fs_watches.pop(key, None)
            for key in list(self._fuzzy_search_sessions):
                if key[0] == client_id:
                    self._fuzzy_search_sessions.pop(key, None)
            for thread_id, subscribers in list(self._thread_subscribers.items()):
                for key in list(subscribers):
                    if key[0] == client_id and (stream_id is None or key[1] == stream_id):
                        subscribers.pop(key, None)
                if not subscribers:
                    self._thread_subscribers.pop(thread_id, None)
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
            for request_id, target in list(self._pending_server_request_targets.items()):
                if target[0] == client_id and (stream_id is None or target[1] == stream_id):
                    self._pending_server_request_targets.pop(request_id, None)
                    queue_ref = self._pending_server_requests.pop(request_id, None)
                    if queue_ref is not None:
                        canceled_requests.append((request_id, queue_ref))
        for process in processes:
            _terminate_remote_process(process)
        for request_id, queue_ref in canceled_requests:
            try:
                queue_ref.put_nowait(
                    {
                        "id": request_id,
                        "error": {"code": -32000, "message": "client disconnected"},
                    }
                )
            except queue.Full:
                pass

    def handle_message(self, ws: Any, client_id: str, stream_id: str, message: dict[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params") if isinstance(message.get("params"), dict) else {}
        if not isinstance(method, str) and request_id is not None:
            self._handle_server_request_response(request_id, message)
            return
        if not isinstance(method, str):
            return
        started_at = time.monotonic()
        if _remote_debug_enabled():
            _remote_log(
                "dispatch_request",
                method=method,
                request_id=request_id,
                params=_request_param_summary(params),
            )
        try:
            result = self._dispatch(ws, client_id, stream_id, method, params, request_id=request_id)
        except Exception as exc:
            _remote_log(
                "dispatch_error",
                method=method,
                request_id=request_id,
                elapsed_ms=int((time.monotonic() - started_at) * 1000),
                error=str(exc),
            )
            if request_id is not None:
                self.service.send_message(ws, client_id, stream_id, _jsonrpc_error(request_id, str(exc), code=getattr(exc, "code", -32000)))
            return
        if isinstance(result, _DeferredResponse):
            return
        if _remote_debug_enabled():
            _remote_log(
                "dispatch_result",
                method=method,
                request_id=request_id,
                elapsed_ms=int((time.monotonic() - started_at) * 1000),
                **_result_metric_summary(result),
            )
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
            with self._lock:
                self._client_names[(client_id, stream_id)] = _initialize_client_name(params)
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
        if method == "environment/add":
            environment_id = str(params.get("environmentId") or "")
            exec_server_url = str(params.get("execServerUrl") or "")
            if not environment_id:
                raise RemoteControlError("environmentId must not be empty")
            with self._lock:
                self._remote_environments[environment_id] = exec_server_url
            return {}
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
            return _config_write_response(self.service.config)
        if method == "thread/list":
            return self._thread_list_response(params)
        if method == "thread/search":
            return self._thread_search_response(params)
        if method == "thread/turns/list":
            return self._thread_turns_list(params)
        if method == "thread/turns/items/list":
            raise _JsonRpcRemoteControlError("thread/turns/items/list is not supported yet", code=-32601)
        if method == "thread/loaded/list":
            return self._thread_loaded_list(params)
        if method == "thread/start":
            session = self._create_session(params)
            payload = self._thread_start_response(session)
            self._subscribe_thread(session, ws, client_id, stream_id)
            self._announce_loaded_thread(session, thread=payload["thread"], fallback=(ws, client_id, stream_id))
            return payload
        if method == "thread/read":
            return {"thread": self._thread_read_payload(params)}
        if method == "thread/resume":
            session = self._resume_session(params)
            self._subscribe_thread(session, ws, client_id, stream_id)
            return self._thread_resume_response(
                session,
                include_turns=not _remote_bool(params, "excludeTurns", "exclude_turns"),
                client_id=client_id,
                stream_id=stream_id,
            )
        if method == "thread/fork":
            session = self._fork_session(params)
            payload = self._thread_start_response(session, include_turns=not _remote_bool(params, "excludeTurns", "exclude_turns"))
            self._subscribe_thread(session, ws, client_id, stream_id)
            self._announce_loaded_thread(session, fallback=(ws, client_id, stream_id))
            return payload
        if method == "thread/name/set":
            session = self._session_by_id(str(params.get("threadId") or ""))
            name = str(params.get("name") or "").strip()
            if not name:
                raise RemoteControlError("thread name must not be empty")
            with self._lock:
                self._thread_names[session.state.thread_id] = name
            self._notify_thread(
                session.state.thread_id,
                "thread/name/updated",
                {"threadId": session.state.thread_id, "threadName": name},
            )
            return {}
        if method == "thread/increment_elicitation":
            session = self._session_by_id(str(params.get("threadId") or ""))
            with self._lock:
                count = self._thread_elicitation_counts.get(session.state.thread_id, 0) + 1
                self._thread_elicitation_counts[session.state.thread_id] = count
            return {"count": count, "paused": count > 0}
        if method == "thread/decrement_elicitation":
            session = self._session_by_id(str(params.get("threadId") or ""))
            with self._lock:
                count = self._thread_elicitation_counts.get(session.state.thread_id, 0)
                if count <= 0:
                    raise RemoteControlError("out-of-band elicitation count is already zero")
                count -= 1
                if count:
                    self._thread_elicitation_counts[session.state.thread_id] = count
                else:
                    self._thread_elicitation_counts.pop(session.state.thread_id, None)
            return {"count": count, "paused": count > 0}
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
        if method == "thread/settings/update":
            session = self._session_by_id(str(params.get("threadId") or ""))
            self._apply_settings_overrides(session, params)
            self._notify_thread(
                session.state.thread_id,
                "thread/settings/updated",
                {
                    "threadId": session.state.thread_id,
                    "threadSettings": _thread_settings_payload(session),
                },
                fallback=(ws, client_id, stream_id),
            )
            return {}
        if method == "thread/memoryMode/set":
            session = self._session_by_id(str(params.get("threadId") or ""))
            mode = str(params.get("mode") or "")
            if mode not in {"enabled", "disabled"}:
                raise RemoteControlError(f"invalid thread memory mode: {mode}")
            with self._lock:
                self._thread_memory_modes[session.state.thread_id] = mode
            store = getattr(session.config, "memory_state_store", None)
            if store is not None and hasattr(store, "set_thread_memory_mode"):
                try:
                    store.set_thread_memory_mode(session.state.thread_id, mode)
                except Exception:
                    pass
            return {}
        if method == "memory/reset":
            store = getattr(self.service.config.codex_config, "memory_state_store", None) if self.service.config.codex_config is not None else None
            if store is not None and hasattr(store, "clear_memory_data"):
                try:
                    store.clear_memory_data()
                except Exception as exc:
                    raise RemoteControlError(f"failed to clear memory data: {exc}") from exc
            return {}
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
        if method == "thread/backgroundTerminals/clean":
            self._session_by_id(str(params.get("threadId") or ""))
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
            self._notify_thread(
                session.state.thread_id,
                "thread/goal/updated",
                {"threadId": session.state.thread_id, "turnId": None, "goal": goal.to_protocol()},
            )
            return {"goal": goal.to_protocol()}
        if method == "thread/goal/clear":
            session = self._session_by_id(str(params.get("threadId") or params.get("thread_id") or ""))
            cleared, _events = session.goals.clear_goal_external()
            if cleared:
                self._notify_thread(
                    session.state.thread_id,
                    "thread/goal/cleared",
                    {"threadId": session.state.thread_id},
                )
            return {"cleared": cleared}
        if method == "turn/start":
            session = self._session_by_id(str(params.get("threadId") or ""))
            self._apply_settings_overrides(session, params)
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
                self._thread_subscribers.setdefault(session.state.thread_id, {})[(client_id, stream_id)] = (
                    ws,
                    client_id,
                    stream_id,
                )
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
        if method == "fuzzyFileSearch/sessionStart":
            session_id = str(params.get("sessionId") or "")
            if not session_id:
                raise RemoteControlError("sessionId must not be empty")
            roots = params.get("roots")
            if not isinstance(roots, list):
                roots = []
            with self._lock:
                self._fuzzy_search_sessions[(client_id, session_id)] = [root for root in roots if isinstance(root, str)]
            return {}
        if method == "fuzzyFileSearch/sessionUpdate":
            session_id = str(params.get("sessionId") or "")
            query = str(params.get("query") or "")
            with self._lock:
                roots = self._fuzzy_search_sessions.get((client_id, session_id))
            if roots is None:
                raise RemoteControlError(f"fuzzy file search session not found: {session_id}")
            files = _fuzzy_file_search_response({"query": query, "roots": roots}).get("files", [])
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "fuzzyFileSearch/sessionUpdated",
                {"sessionId": session_id, "query": query, "files": files},
            )
            return {}
        if method == "fuzzyFileSearch/sessionStop":
            session_id = str(params.get("sessionId") or "")
            with self._lock:
                self._fuzzy_search_sessions.pop((client_id, session_id), None)
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "fuzzyFileSearch/sessionCompleted",
                {"sessionId": session_id},
            )
            return {}
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
        if method == "thread/unsubscribe":
            status = self._unsubscribe_thread(str(params.get("threadId") or ""), client_id, stream_id)
            return {"status": status}
        if method in {"thread/archive", "thread/unarchive"}:
            return {}
        raise RemoteControlError(f"method `{method}` is not implemented in Python remote control yet")

    def _subscribe_thread(self, session: CodexSession, ws: Any, client_id: str, stream_id: str) -> None:
        with self._lock:
            self._thread_subscribers.setdefault(session.state.thread_id, {})[(client_id, stream_id)] = (
                ws,
                client_id,
                stream_id,
            )

    def _attach_client_to_loaded_threads(self, ws: Any, client_id: str, stream_id: str) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            self._subscribe_thread(session, ws, client_id, stream_id)
            self.service.send_notification(
                ws,
                client_id,
                stream_id,
                "thread/started",
                {"thread": self._thread_payload(session, include_turns=False)},
            )

    def _announce_loaded_thread(
        self,
        session: CodexSession,
        *,
        thread: dict[str, Any] | None = None,
        fallback: tuple[Any, str, str] | None = None,
    ) -> None:
        payload = {"thread": thread or self._thread_payload(session, include_turns=False)}
        refs = self._initialized_client_refs()
        if not refs and fallback is not None:
            refs = [fallback]
        seen: set[tuple[str, str]] = set()
        for ref_ws, ref_client_id, ref_stream_id in refs:
            key = (ref_client_id, ref_stream_id)
            if key in seen:
                continue
            seen.add(key)
            self._subscribe_thread(session, ref_ws, ref_client_id, ref_stream_id)
            try:
                self.service.send_notification(ref_ws, ref_client_id, ref_stream_id, "thread/started", payload)
            except Exception:
                pass

    def _initialized_client_refs(self) -> list[tuple[Any, str, str]]:
        refs = getattr(self.service, "initialized_client_refs", None)
        if not callable(refs):
            return []
        try:
            return list(refs())
        except Exception:
            return []

    def _unsubscribe_thread(self, thread_id: str, client_id: str, stream_id: str) -> str:
        if not thread_id:
            return "notSubscribed"
        with self._lock:
            subscribers = self._thread_subscribers.get(thread_id)
            if subscribers is None:
                return "notLoaded" if thread_id not in self._sessions else "notSubscribed"
            removed = subscribers.pop((client_id, stream_id), None)
            if not subscribers:
                self._thread_subscribers.pop(thread_id, None)
            return "unsubscribed" if removed is not None else "notSubscribed"

    def _thread_subscriber_refs(
        self,
        thread_id: str,
        fallback: tuple[Any, str, str] | None = None,
    ) -> list[tuple[Any, str, str]]:
        with self._lock:
            refs = list((self._thread_subscribers.get(thread_id) or {}).values())
        if not refs and fallback is not None:
            return [fallback]
        return refs

    def _thread_status_payload_for(self, thread_id: str) -> dict[str, Any]:
        with self._lock:
            running = thread_id in self._turn_threads
            counts = dict(self._thread_active_flag_counts.get(thread_id) or {})
        active_flags = [
            flag
            for flag in ("waitingOnApproval", "waitingOnUserInput")
            if counts.get(flag, 0) > 0
        ]
        if running or active_flags:
            return {"type": "active", "activeFlags": active_flags}
        return {"type": "idle"}

    def _set_thread_active_flag(
        self,
        thread_id: str,
        flag: str,
        enabled: bool,
        *,
        fallback: tuple[Any, str, str] | None = None,
    ) -> None:
        with self._lock:
            counts = self._thread_active_flag_counts.setdefault(thread_id, {})
            current = counts.get(flag, 0)
            if enabled:
                counts[flag] = current + 1
            elif current <= 1:
                counts.pop(flag, None)
                if not counts:
                    self._thread_active_flag_counts.pop(thread_id, None)
            else:
                counts[flag] = current - 1
        self._notify_thread(
            thread_id,
            "thread/status/changed",
            {"threadId": thread_id, "status": self._thread_status_payload_for(thread_id)},
            fallback=fallback,
        )

    def _notify_thread(
        self,
        thread_id: str,
        method: str,
        params: dict[str, Any],
        *,
        fallback: tuple[Any, str, str] | None = None,
    ) -> None:
        stale: list[tuple[str, str]] = []
        for sub_ws, sub_client_id, sub_stream_id in self._thread_subscriber_refs(thread_id, fallback=fallback):
            try:
                self.service.send_notification(sub_ws, sub_client_id, sub_stream_id, method, params)
            except Exception:
                stale.append((sub_client_id, sub_stream_id))
        if stale:
            with self._lock:
                subscribers = self._thread_subscribers.get(thread_id)
                if subscribers is not None:
                    for key in stale:
                        subscribers.pop(key, None)
                    if not subscribers:
                        self._thread_subscribers.pop(thread_id, None)

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
        current_compaction_item_id: str | None = None
        announced_start = False
        tool_arguments_by_call_id: dict[str, Any] = {}
        live_tool_call_ids: set[str] = set()
        agent_message_item_ids: set[str] = set()
        non_agent_delta_item_ids: set[str] = set()
        final_turn = _turn_payload(session.state.turn_id, status="completed", started_at=started_at)
        fallback_ref = (ws, client_id, stream_id)
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
                    self._notify_thread(
                        session.state.thread_id,
                        "thread/status/changed",
                        {
                            "threadId": session.state.thread_id,
                            "status": self._thread_status_payload_for(session.state.thread_id),
                        },
                        fallback=fallback_ref,
                    )
                    self._notify_thread(
                        session.state.thread_id,
                        "turn/started",
                        {"threadId": session.state.thread_id, "turn": turn},
                        fallback=fallback_ref,
                    )
                elif event.type == "item.completed":
                    raw_item = event.payload.get("item")
                    if _response_item_is_live_tool_echo(raw_item, live_tool_call_ids):
                        continue
                    item = _thread_item_from_response_item(raw_item)
                    if item is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/completed",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "completedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "item.started":
                    raw_item = event.payload.get("item")
                    raw_item_id = _response_item_protocol_id(raw_item, item_id=event.payload.get("item_id"))
                    if _response_item_is_assistant_message(raw_item):
                        agent_message_item_ids.add(raw_item_id)
                    elif raw_item_id:
                        non_agent_delta_item_ids.add(raw_item_id)
                    if _response_item_is_live_tool_echo(raw_item, live_tool_call_ids):
                        continue
                    item = _thread_item_from_response_item(raw_item, item_id=event.payload.get("item_id"))
                    if item is not None:
                        current_agent_item_id = item.get("id") if item.get("type") == "agentMessage" else current_agent_item_id
                        self._notify_thread(
                            session.state.thread_id,
                            "item/started",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "startedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "item.delta":
                    reasoning_delta = _reasoning_delta_notification(
                        session.state.thread_id,
                        session.state.turn_id,
                        event.payload,
                    )
                    if reasoning_delta is not None:
                        method, params = reasoning_delta
                        self._notify_thread(
                            session.state.thread_id,
                            method,
                            params,
                            fallback=fallback_ref,
                        )
                        continue
                    delta = event.payload.get("delta")
                    if isinstance(delta, str) and delta and _is_agent_message_delta(
                        event.payload,
                        agent_message_item_ids=agent_message_item_ids,
                        non_agent_delta_item_ids=non_agent_delta_item_ids,
                    ):
                        item_id = str(event.payload.get("item_id") or current_agent_item_id or f"msg_{uuid.uuid4()}")
                        current_agent_item_id = item_id
                        self._notify_thread(
                            session.state.thread_id,
                            "item/agentMessage/delta",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "itemId": item_id,
                                "delta": delta,
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "tool.started":
                    call_id = str(event.payload.get("call_id") or "")
                    if call_id:
                        tool_arguments_by_call_id[call_id] = event.payload.get("arguments")
                    if str(event.payload.get("name") or "") in {"exec_command", "shell_command", "write_stdin"} and call_id:
                        live_tool_call_ids.add(call_id)
                    collab_item = _collab_agent_item_from_tool_started(session, event.payload)
                    if collab_item is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/started",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": collab_item,
                                "startedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                        continue
                    item = _command_execution_item_from_tool_started(session, event.payload)
                    if item is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/started",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "startedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "exec_command.output_delta":
                    call_id = str(event.payload.get("call_id") or "")
                    delta = event.payload.get("delta")
                    if call_id and isinstance(delta, str) and delta:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/commandExecution/outputDelta",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "itemId": call_id,
                                "delta": delta,
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "plan_update":
                    plan_payload = _turn_plan_update_payload(session.state.thread_id, session.state.turn_id, event.payload)
                    if plan_payload is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "turn/plan/updated",
                            plan_payload,
                            fallback=fallback_ref,
                        )
                elif event.type == "tool.completed":
                    call_id = str(event.payload.get("call_id") or "")
                    arguments = tool_arguments_by_call_id.pop(call_id, {}) if call_id else {}
                    collab_item = _collab_agent_item_from_tool_completed(session, event.payload, arguments)
                    if collab_item is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/completed",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": collab_item,
                                "completedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                        continue
                    plan_payload = _turn_plan_update_payload(session.state.thread_id, session.state.turn_id, event.payload)
                    if plan_payload is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "turn/plan/updated",
                            plan_payload,
                            fallback=fallback_ref,
                        )
                        continue
                    file_change_item = _file_change_item_from_apply_patch_completed(event.payload)
                    if file_change_item is not None:
                        patch_payload = _file_change_patch_updated_payload(
                            session.state.thread_id,
                            session.state.turn_id,
                            file_change_item,
                        )
                        if patch_payload is not None:
                            self._notify_thread(
                                session.state.thread_id,
                                "item/fileChange/patchUpdated",
                                patch_payload,
                                fallback=fallback_ref,
                            )
                        self._notify_thread(
                            session.state.thread_id,
                            "item/completed",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": file_change_item,
                                "completedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                        continue
                    terminal_interaction = _terminal_interaction_from_write_stdin(session, event.payload, arguments)
                    if terminal_interaction is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/commandExecution/terminalInteraction",
                            terminal_interaction,
                            fallback=fallback_ref,
                        )
                    item = _command_execution_item_from_tool_completed(session, event.payload, arguments)
                    if item is not None:
                        self._notify_thread(
                            session.state.thread_id,
                            "item/completed",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "item": item,
                                "completedAtMs": _now_ms(),
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "token_count":
                    usage = event.payload.get("usage")
                    if isinstance(usage, dict):
                        self._notify_thread(
                            session.state.thread_id,
                            "thread/tokenUsage/updated",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "tokenUsage": _token_usage_payload(usage),
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "turn_diff":
                    diff = event.payload.get("unified_diff")
                    if isinstance(diff, str):
                        self._notify_thread(
                            session.state.thread_id,
                            "turn/diff/updated",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "diff": diff,
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "context_compaction.started":
                    current_compaction_item_id = f"context_compaction_{uuid.uuid4().hex}"
                    self._notify_thread(
                        session.state.thread_id,
                        "item/started",
                        {
                            "threadId": session.state.thread_id,
                            "turnId": session.state.turn_id,
                            "item": _context_compaction_item(current_compaction_item_id),
                            "startedAtMs": _now_ms(),
                        },
                        fallback=fallback_ref,
                    )
                elif event.type == "context_compaction.completed":
                    if current_compaction_item_id is None:
                        current_compaction_item_id = f"context_compaction_{uuid.uuid4().hex}"
                    self._notify_thread(
                        session.state.thread_id,
                        "item/completed",
                        {
                            "threadId": session.state.thread_id,
                            "turnId": session.state.turn_id,
                            "item": _context_compaction_item(current_compaction_item_id),
                            "completedAtMs": _now_ms(),
                        },
                        fallback=fallback_ref,
                    )
                elif event.type == "warning":
                    message = event.payload.get("message")
                    if isinstance(message, str) and message:
                        self._notify_thread(
                            session.state.thread_id,
                            "warning",
                            {"threadId": session.state.thread_id, "message": message},
                            fallback=fallback_ref,
                        )
                elif event.type == "stream_error":
                    message = str(event.payload.get("message") or event.payload.get("error") or "")
                    if message:
                        self._notify_thread(
                            session.state.thread_id,
                            "error",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "willRetry": True,
                                "error": {
                                    "message": message,
                                    "codexErrorInfo": None,
                                    "additionalDetails": str(event.payload.get("error") or "") or None,
                                },
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "thread.goal.updated":
                    goal = event.payload.get("goal")
                    if isinstance(goal, dict):
                        thread_id = str(event.payload.get("thread_id") or goal.get("threadId") or session.state.thread_id)
                        turn_id = event.payload.get("turn_id")
                        self._notify_thread(
                            session.state.thread_id,
                            "thread/goal/updated",
                            {
                                "threadId": thread_id,
                                "turnId": turn_id if isinstance(turn_id, str) else None,
                                "goal": goal,
                            },
                            fallback=fallback_ref,
                        )
                elif event.type == "thread.goal.cleared":
                    self._notify_thread(
                        session.state.thread_id,
                        "thread/goal/cleared",
                        {"threadId": session.state.thread_id},
                        fallback=fallback_ref,
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
            self._notify_thread(
                session.state.thread_id,
                "error",
                {
                    "threadId": session.state.thread_id,
                    "turnId": session.state.turn_id,
                    "willRetry": False,
                    "error": {"message": str(exc), "codexErrorInfo": None, "additionalDetails": None},
                },
                fallback=fallback_ref,
            )
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
            self._notify_thread(
                session.state.thread_id,
                "turn/completed",
                {"threadId": session.state.thread_id, "turn": final_turn},
                fallback=fallback_ref,
            )
            with self._lock:
                self._active_turn_clients.pop(session.state.thread_id, None)
                self._turn_threads.pop(session.state.thread_id, None)
            self._notify_thread(
                session.state.thread_id,
                "thread/status/changed",
                {"threadId": session.state.thread_id, "status": self._thread_status_payload_for(session.state.thread_id)},
                fallback=fallback_ref,
            )

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
        if isinstance(thread_id, str) and thread_id:
            with self._lock:
                session = self._sessions.get(thread_id)
            if session is not None:
                return session
        path = params.get("path")
        rollout_path = Path(path).expanduser() if isinstance(path, str) and path else None
        if rollout_path is not None:
            resolved_rollout = rollout_path.resolve()
            with self._lock:
                for session in self._sessions.values():
                    try:
                        if session.state.rollout_path().resolve() == resolved_rollout:
                            return session
                    except Exception:
                        continue
        if rollout_path is None and isinstance(thread_id, str):
            rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
        if rollout_path is None:
            raise RemoteControlError("thread/resume could not find the requested thread")
        session_ref: list[CodexSession] = []
        session = CodexSession.resume_from_rollout(
            rollout_path,
            self._config_from_params(
                params,
                session_ref=session_ref,
                default_cwd=_rollout_cwd(rollout_path),
            ),
        )
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
        session = CodexSession.fork_from_rollout(
            rollout_path,
            self._config_from_params(
                params,
                session_ref=session_ref,
                default_cwd=_rollout_cwd(rollout_path),
            ),
        )
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
            rollout_path = Path(path).expanduser()
            session = CodexSession.resume_from_rollout(
                rollout_path,
                self._config_from_params({}, session_ref=session_ref, default_cwd=_rollout_cwd(rollout_path)),
            )
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
        session = CodexSession.resume_from_rollout(
            rollout_path,
            self._config_from_params({}, session_ref=session_ref, default_cwd=_rollout_cwd(rollout_path)),
        )
        session_ref.append(session)
        with self._lock:
            self._sessions[session.state.thread_id] = session
        return session

    def _thread_read_payload(self, params: dict[str, Any]) -> dict[str, Any]:
        thread_id = str(params.get("threadId") or "")
        include_turns = _remote_bool(params, "includeTurns", "include_turns")
        if not thread_id:
            raise RemoteControlError("missing threadId")
        with self._lock:
            session = self._sessions.get(thread_id)
        if session is not None:
            if not include_turns:
                return self._thread_metadata_payload_for_session(session)
            return self._thread_payload(session, include_turns=include_turns)
        rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
        if rollout_path is None:
            raise RemoteControlError(f"unknown thread `{thread_id}`")
        thread = _thread_payload_from_rollout(rollout_path, self.service.config, include_turns=include_turns)
        if thread is None:
            raise RemoteControlError(f"could not read thread `{thread_id}`")
        return thread

    def _thread_list_response(self, params: dict[str, Any]) -> dict[str, Any]:
        rows = self._thread_list(params)
        sort_key = _thread_list_sort_key_param(params.get("sortKey") or params.get("sort_key"))
        sort_direction = _sort_direction_param(params.get("sortDirection") or params.get("sort_direction"))
        _sort_thread_list_rows(rows, sort_key=sort_key, sort_direction=sort_direction)
        return _paginate_thread_list_rows(
            rows,
            cursor=params.get("cursor"),
            limit=params.get("limit"),
            sort_direction=sort_direction,
        )

    def _thread_list(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        cwd_filter = _cwd_filter(params.get("cwd"))
        model_provider_param = params.get("modelProviders")
        if model_provider_param is None and "model_providers" in params:
            model_provider_param = params.get("model_providers")
        model_providers = _model_providers_filter(model_provider_param)
        source_kinds = _source_kinds_filter(params.get("sourceKinds"))
        archived = bool(params.get("archived"))
        search_term = str(params.get("searchTerm") or params.get("search_term") or "").strip().lower()
        rows: list[dict[str, Any]] = []
        with self._lock:
            rows.extend(self._thread_metadata_payload_for_session(session) for session in self._sessions.values())
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
        if model_providers is None and not isinstance(model_provider_param, list):
            model_providers = {_default_thread_model_provider(self.service.config)}
        if model_providers is not None:
            rows = [row for row in rows if row.get("modelProvider") in model_providers]
        if archived:
            rows = []
        if search_term:
            rows = [
                row
                for row in rows
                if search_term in str(row.get("preview") or "").lower()
                or search_term in str(row.get("name") or "").lower()
            ]
        return rows

    def _thread_search_response(self, params: dict[str, Any]) -> dict[str, Any]:
        search_term = str(params.get("searchTerm") or params.get("search_term") or "").strip()
        if not search_term:
            raise RemoteControlError("thread/search requires a non-empty searchTerm")
        rows = self._thread_list({**params, "searchTerm": search_term})
        sort_key = _thread_list_sort_key_param(params.get("sortKey") or params.get("sort_key"))
        sort_direction = _sort_direction_param(params.get("sortDirection") or params.get("sort_direction"))
        _sort_thread_list_rows(rows, sort_key=sort_key, sort_direction=sort_direction)
        page = _paginate_thread_list_rows(
            rows,
            cursor=params.get("cursor"),
            limit=params.get("limit"),
            sort_direction=sort_direction,
        )
        needle = search_term.lower()
        return {
            "data": [
                {
                    "thread": row,
                    "snippet": _thread_search_snippet(row, needle),
                }
                for row in page["data"]
            ],
            "nextCursor": page.get("nextCursor"),
            "backwardsCursor": page.get("backwardsCursor"),
        }

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

    def _thread_turns_list(self, params: dict[str, Any]) -> dict[str, Any]:
        thread_id = str(params.get("threadId") or "")
        if not thread_id:
            raise RemoteControlError("missing threadId")
        items_view = _items_view_param(params.get("itemsView") or params.get("items_view")) or "summary"
        with self._lock:
            session = self._sessions.get(thread_id)
        if session is not None:
            turns = _turns_from_history(session, items_view=items_view)
        else:
            rollout_path = _find_rollout_path(self.service.config.codex_home, thread_id)
            if rollout_path is None:
                raise RemoteControlError(f"unknown thread `{thread_id}`")
            thread = _thread_payload_from_rollout(
                rollout_path,
                self.service.config,
                include_turns=True,
                items_view=items_view,
            )
            if thread is None:
                raise RemoteControlError(f"could not read thread `{thread_id}`")
            turns = list(thread.get("turns") or [])
        return _paginate_thread_turns(
            turns,
            cursor=params.get("cursor"),
            limit=params.get("limit"),
            sort_direction=params.get("sortDirection") or params.get("sort_direction"),
        )

    def _thread_metadata_payload_for_session(self, session: CodexSession) -> dict[str, Any]:
        if not session.config.ephemeral:
            try:
                rollout_path = session.state.rollout_path()
            except Exception:
                rollout_path = None
            if isinstance(rollout_path, Path) and rollout_path.exists():
                thread = _thread_payload_from_rollout(
                    rollout_path,
                    self.service.config,
                    include_turns=False,
                )
                if thread is not None:
                    thread["status"] = self._thread_status_payload_for(session.state.thread_id)
                    with self._lock:
                        name = self._thread_names.get(session.state.thread_id)
                        git_info = self._thread_git_info.get(session.state.thread_id)
                    if name is not None:
                        thread["name"] = name
                    if git_info is not None:
                        thread["gitInfo"] = git_info
                    return thread
        return self._thread_payload(session, include_turns=False)

    def _thread_start_response(self, session: CodexSession, *, include_turns: bool = False) -> dict[str, Any]:
        return {
            "thread": self._thread_payload(session, include_turns=include_turns),
            "model": session.config.model,
            "modelProvider": session.config.model_provider_id,
            "cwd": str(session.config.resolved_cwd()),
            "runtimeWorkspaceRoots": [str(session.config.resolved_cwd())],
            "approvalPolicy": session.config.approval_policy,
            "approvalsReviewer": "user",
            "sandbox": _sandbox_policy_payload(session.config),
            "activePermissionProfile": None,
            "serviceTier": session.config.resolved_service_tier(),
            "reasoningEffort": session.config.model_reasoning_effort,
            "instructionSources": [],
        }

    def _thread_resume_response(
        self,
        session: CodexSession,
        *,
        include_turns: bool,
        client_id: str,
        stream_id: str,
    ) -> dict[str, Any]:
        payload = self._thread_start_response(session, include_turns=include_turns)
        self._redact_thread_payload_for_client(payload["thread"], client_id=client_id, stream_id=stream_id)
        return payload

    def _redact_thread_payload_for_client(self, thread: dict[str, Any], *, client_id: str, stream_id: str) -> None:
        with self._lock:
            client_name = self._client_names.get((client_id, stream_id))
        if _should_redact_thread_resume_payloads(client_name):
            _redact_thread_resume_payloads(thread)

    def _thread_payload(self, session: CodexSession, *, include_turns: bool) -> dict[str, Any]:
        with self._lock:
            name = self._thread_names.get(session.state.thread_id)
            git_info = self._thread_git_info.get(session.state.thread_id)
        created_at, updated_at = self._session_thread_timestamps(session)
        return _thread_payload(
            thread_id=session.state.thread_id,
            session_id=session.state.thread_id,
            cwd=str(session.config.resolved_cwd()),
            model_provider=session.config.model_provider_id,
            source=_api_session_source(session.config.session_source or "cli"),
            preview=_preview_from_history(session.state.history),
            path=str(session.state.rollout_path()) if not session.config.ephemeral else None,
            status=self._thread_status_payload_for(session.state.thread_id),
            turns=_turns_from_history(session) if include_turns else [],
            created_at=created_at,
            updated_at=updated_at,
            ephemeral=session.config.ephemeral,
            name=name,
            git_info=git_info,
        )

    def _session_thread_timestamps(self, session: CodexSession) -> tuple[int, int]:
        now = int(time.time())
        if session.config.ephemeral:
            return now, now
        try:
            rollout_path = session.state.rollout_path()
        except Exception:
            return now, now
        if rollout_path.exists():
            thread = _thread_payload_from_rollout(rollout_path, self.service.config, include_turns=False)
            if thread is not None:
                created_at = _optional_int(thread.get("createdAt")) or now
                updated_at = _optional_int(thread.get("updatedAt")) or created_at
                return created_at, updated_at
            try:
                updated_at = int(rollout_path.stat().st_mtime)
            except OSError:
                updated_at = now
        else:
            updated_at = now
        started_at = getattr(session.state, "_started_at", None)
        if started_at is None:
            try:
                started_at = session.state._session_started_at()
            except Exception:
                return now, updated_at
        try:
            created_at = int(started_at.timestamp())
        except Exception:
            created_at = now
        return created_at, max(updated_at, created_at)

    def _inject_thread_items(self, session: CodexSession, items: list[Any]) -> None:
        normalized_items = [dict(item) for item in items if isinstance(item, dict)]
        if not normalized_items:
            return
        for item in normalized_items:
            session.state.append_history(item)
            session.state.emit("item.completed", item=item)

    def _compact_thread(self, ws: Any, client_id: str, stream_id: str, session: CodexSession) -> None:
        fallback_ref = (ws, client_id, stream_id)
        started_at = int(time.time())
        turn = _turn_payload(session.state.turn_id, status="inProgress", started_at=started_at)
        final_turn = turn
        compaction_item_id: str | None = None
        try:
            for event in session.stream_compact():
                if event.type == "turn.started":
                    started_at = int(time.time())
                    turn = _turn_payload(session.state.turn_id, status="inProgress", started_at=started_at)
                    final_turn = turn
                    self._notify_thread(
                        session.state.thread_id,
                        "thread/status/changed",
                        {"threadId": session.state.thread_id, "status": {"type": "active", "activeFlags": []}},
                        fallback=fallback_ref,
                    )
                    self._notify_thread(
                        session.state.thread_id,
                        "turn/started",
                        {"threadId": session.state.thread_id, "turn": turn},
                        fallback=fallback_ref,
                    )
                elif event.type == "context_compaction.started":
                    compaction_item_id = f"context_compaction_{uuid.uuid4().hex}"
                    self._notify_thread(
                        session.state.thread_id,
                        "item/started",
                        {
                            "threadId": session.state.thread_id,
                            "turnId": session.state.turn_id,
                            "item": _context_compaction_item(compaction_item_id),
                            "startedAtMs": _now_ms(),
                        },
                        fallback=fallback_ref,
                    )
                elif event.type == "context_compaction.completed":
                    if compaction_item_id is None:
                        compaction_item_id = f"context_compaction_{uuid.uuid4().hex}"
                    item = _context_compaction_item(compaction_item_id)
                    self._notify_thread(
                        session.state.thread_id,
                        "item/completed",
                        {
                            "threadId": session.state.thread_id,
                            "turnId": session.state.turn_id,
                            "item": item,
                            "completedAtMs": _now_ms(),
                        },
                        fallback=fallback_ref,
                    )
                    final_turn = _turn_payload(
                        session.state.turn_id,
                        status="completed",
                        started_at=started_at,
                        completed_at=int(time.time()),
                        items=[item],
                    )
                elif event.type == "warning":
                    message = event.payload.get("message")
                    if isinstance(message, str) and message:
                        self._notify_thread(
                            session.state.thread_id,
                            "warning",
                            {"threadId": session.state.thread_id, "message": message},
                            fallback=fallback_ref,
                        )
                elif event.type == "stream_error":
                    message = str(event.payload.get("message") or event.payload.get("error") or "")
                    if message:
                        self._notify_thread(
                            session.state.thread_id,
                            "error",
                            {
                                "threadId": session.state.thread_id,
                                "turnId": session.state.turn_id,
                                "willRetry": True,
                                "error": {
                                    "message": message,
                                    "codexErrorInfo": None,
                                    "additionalDetails": str(event.payload.get("error") or "") or None,
                                },
                            },
                            fallback=fallback_ref,
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
            self._notify_thread(
                session.state.thread_id,
                "error",
                {
                    "threadId": session.state.thread_id,
                    "turnId": session.state.turn_id,
                    "willRetry": False,
                    "error": {"message": str(exc), "codexErrorInfo": None, "additionalDetails": None},
                },
                fallback=fallback_ref,
            )
            final_turn = _turn_payload(
                session.state.turn_id,
                status="failed",
                started_at=started_at,
                completed_at=int(time.time()),
                error=str(exc),
            )
        self._notify_thread(
            session.state.thread_id,
            "turn/completed",
            {"threadId": session.state.thread_id, "turn": final_turn},
            fallback=fallback_ref,
        )
        self._notify_thread(
            session.state.thread_id,
            "thread/status/changed",
            {"threadId": session.state.thread_id, "status": {"type": "idle"}},
            fallback=fallback_ref,
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
        fallback_ref = (ws, client_id, stream_id)
        turn = _turn_payload(session.state.turn_id, status="inProgress", started_at=int(time.time()))
        item_id = f"shell_{uuid.uuid4().hex}"
        item = _command_execution_item(
            item_id,
            command=command,
            cwd=str(session.config.resolved_cwd()),
            status="inProgress",
        )
        self._notify_thread(
            session.state.thread_id,
            "thread/status/changed",
            {"threadId": session.state.thread_id, "status": {"type": "active", "activeFlags": []}},
            fallback=fallback_ref,
        )
        self._notify_thread(
            session.state.thread_id,
            "turn/started",
            {"threadId": session.state.thread_id, "turn": turn},
            fallback=fallback_ref,
        )
        self._notify_thread(
            session.state.thread_id,
            "item/started",
            {"threadId": session.state.thread_id, "turnId": session.state.turn_id, "item": item, "startedAtMs": _now_ms()},
            fallback=fallback_ref,
        )
        started_at = time.time()
        try:
            process = subprocess.Popen(
                command,
                cwd=str(session.config.resolved_cwd()),
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
            )
            chunks: list[bytes] = []
            assert process.stdout is not None
            stdout_fd = process.stdout.fileno()
            while True:
                try:
                    chunk = os.read(stdout_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                chunks.append(chunk)
                delta = chunk.decode("utf-8", errors="replace")
                self._notify_thread(
                    session.state.thread_id,
                    "item/commandExecution/outputDelta",
                    {
                        "threadId": session.state.thread_id,
                        "turnId": session.state.turn_id,
                        "itemId": item_id,
                        "delta": delta,
                    },
                    fallback=fallback_ref,
                )
            try:
                process.stdout.close()
            except OSError:
                pass
            exit_code = process.wait()
            output = b"".join(chunks).decode("utf-8", errors="replace")
            item = _command_execution_item(
                item_id,
                command=command,
                cwd=str(session.config.resolved_cwd()),
                status="completed" if exit_code == 0 else "failed",
                aggregated_output=output,
                exit_code=exit_code,
                duration_ms=int((time.time() - started_at) * 1000),
            )
            session.state.append_history(
                {
                    "type": "function_call_output",
                    "call_id": item_id,
                    "output": output,
                    "status": "completed" if exit_code == 0 else "failed",
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
        self._notify_thread(
            session.state.thread_id,
            "item/completed",
            {"threadId": session.state.thread_id, "turnId": session.state.turn_id, "item": item, "completedAtMs": _now_ms()},
            fallback=fallback_ref,
        )
        self._notify_thread(
            session.state.thread_id,
            "turn/completed",
            {"threadId": session.state.thread_id, "turn": completed_turn},
            fallback=fallback_ref,
        )
        self._notify_thread(
            session.state.thread_id,
            "thread/status/changed",
            {"threadId": session.state.thread_id, "status": {"type": "idle"}},
            fallback=fallback_ref,
        )

    def _start_command_exec(
        self,
        ws: Any,
        client_id: str,
        stream_id: str,
        request_id: Any,
        params: dict[str, Any],
    ) -> None:
        _validate_command_exec_params(params)
        command = _command_exec_argv(params)
        process_id = str(params.get("processId") or "")
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
        _start_remote_process_timeout(process, _command_exec_timeout_seconds(params))
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
        timeout_seconds = _process_spawn_timeout_seconds(params)
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
        _start_remote_process_timeout(process, timeout_seconds)
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
        _cancel_remote_process_timeout(process)
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
        _cancel_remote_process_timeout(process)
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
        default_cwd: Path | str | None = None,
    ) -> CodexConfig:
        base = self.service.config
        template = base.codex_config or CodexConfig(
            cwd=base.cwd,
            codex_home=base.codex_home,
            auth_codex_home=base.auth_codex_home,
            model=base.model or CodexConfig().model,
            skip_git_repo_check=True,
        )
        return replace(
            template,
            **_remote_config_override_kwargs(template, params, default_cwd=default_cwd or base.cwd),
            codex_home=base.codex_home,
            auth_codex_home=base.auth_codex_home,
            session_source="cli",
            approval_provider=self._remote_approval_provider(session_ref) if session_ref is not None else None,
            request_user_input_provider=self._remote_request_user_input_provider(session_ref) if session_ref is not None else None,
            skip_git_repo_check=True,
        )

    def _apply_settings_overrides(self, session: CodexSession, params: dict[str, Any]) -> None:
        config = replace(
            session.config,
            **_remote_config_override_kwargs(session.config, params, default_cwd=session.config.cwd),
        )
        session.config = config
        session.state.config = config
        session.tools.config = config

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
                thread_id=session.state.thread_id,
                active_flag="waitingOnUserInput",
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
            result = self._send_server_request(
                ws,
                client_id,
                stream_id,
                method,
                params,
                timeout_seconds=None,
                thread_id=session.state.thread_id,
                active_flag="waitingOnApproval",
            )
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
        thread_id: str | None = None,
        active_flag: str | None = None,
    ) -> dict[str, Any] | None:
        request_id = self._next_server_request_id_value()
        response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
        fallback_ref = (ws, client_id, stream_id)
        with self._lock:
            self._pending_server_requests[request_id] = response_queue
            self._pending_server_request_targets[request_id] = (client_id, stream_id, thread_id or "")
        if thread_id is not None and active_flag is not None:
            self._set_thread_active_flag(thread_id, active_flag, True, fallback=fallback_ref)
        self.service.send_message(ws, client_id, stream_id, {"id": request_id, "method": method, "params": params})
        try:
            response = response_queue.get(timeout=timeout_seconds)
        except queue.Empty:
            with self._lock:
                self._pending_server_requests.pop(request_id, None)
                self._pending_server_request_targets.pop(request_id, None)
            return None
        finally:
            with self._lock:
                self._pending_server_requests.pop(request_id, None)
                self._pending_server_request_targets.pop(request_id, None)
            if thread_id is not None:
                self._notify_thread(
                    thread_id,
                    "serverRequest/resolved",
                    {"threadId": thread_id, "requestId": request_id},
                    fallback=fallback_ref,
                )
                if active_flag is not None:
                    self._set_thread_active_flag(thread_id, active_flag, False, fallback=fallback_ref)
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


def _items_view_param(raw: Any) -> str | None:
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if value in {"notLoaded", "summary", "full"}:
        return value
    aliases = {
        "not_loaded": "notLoaded",
        "notloaded": "notLoaded",
        "Summary": "summary",
        "Full": "full",
        "NotLoaded": "notLoaded",
    }
    return aliases.get(value)


def _remote_bool(params: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = params.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
    return False


def _thread_list_sort_key_param(raw: Any) -> str:
    if not isinstance(raw, str):
        return "createdAt"
    value = raw.strip()
    if value in {"updatedAt", "updated_at", "UpdatedAt"}:
        return "updatedAt"
    return "createdAt"


def _thread_search_snippet(row: dict[str, Any], needle: str) -> str:
    preview = str(row.get("preview") or row.get("name") or "")
    if not needle:
        return preview
    index = preview.lower().find(needle)
    if index < 0:
        return preview[:160]
    start = max(0, index - 60)
    end = min(len(preview), index + len(needle) + 100)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(preview) else ""
    return f"{prefix}{preview[start:end]}{suffix}"


def _sort_direction_param(raw: Any) -> str:
    if not isinstance(raw, str):
        return "desc"
    value = raw.strip().lower()
    if value == "asc":
        return "asc"
    return "desc"


def _model_providers_filter(raw: Any) -> set[str] | None:
    if not isinstance(raw, list):
        return None
    providers = {str(item) for item in raw if isinstance(item, str) and item}
    return providers or None


def _sort_thread_list_rows(rows: list[dict[str, Any]], *, sort_key: str, sort_direction: str) -> None:
    timestamp_key = "updatedAt" if sort_key == "updatedAt" else "createdAt"
    groups: dict[int, list[str]] = {}
    for row in rows:
        timestamp = _optional_int(row.get(timestamp_key)) or 0
        groups.setdefault(timestamp, []).append(str(row.get("id") or ""))
    ranks: dict[tuple[int, str], int] = {}
    for timestamp, ids in groups.items():
        for rank, thread_id in enumerate(sorted(set(ids))):
            ranks[(timestamp, thread_id)] = rank

    def key(row: dict[str, Any]) -> int:
        timestamp = _optional_int(row.get(timestamp_key)) or 0
        thread_id = str(row.get("id") or "")
        sort_millis = timestamp * 1000 + ranks.get((timestamp, thread_id), 0)
        row["_threadListSortMillis"] = sort_millis
        return sort_millis

    rows.sort(key=key, reverse=sort_direction != "asc")


def _paginate_thread_list_rows(
    rows: list[dict[str, Any]],
    *,
    cursor: Any,
    limit: Any,
    sort_direction: str,
) -> dict[str, Any]:
    anchor = _parse_thread_list_cursor(cursor)
    if anchor is not None:
        if "sortMillis" in anchor:
            anchor_millis = _optional_int(anchor.get("sortMillis"))
            if anchor_millis is None:
                raise RemoteControlError("invalid cursor: missing timestamp")
            if sort_direction == "asc":
                rows = [row for row in rows if _thread_list_sort_millis(row) > anchor_millis]
            else:
                rows = [row for row in rows if _thread_list_sort_millis(row) < anchor_millis]
        else:
            anchor_id = str(anchor.get("threadId") or anchor.get("thread_id") or "")
            include_anchor = bool(anchor.get("includeAnchor") or anchor.get("include_anchor"))
            anchor_index = next((index for index, row in enumerate(rows) if str(row.get("id") or "") == anchor_id), None)
            if anchor_index is None:
                raise RemoteControlError("invalid cursor: anchor thread is no longer present")
            rows = rows[anchor_index if include_anchor else anchor_index + 1 :]
    page_size = _optional_int(limit) or 25
    page_size = max(1, min(page_size, 100))
    more = len(rows) > page_size
    raw_page = rows[:page_size]
    backwards_cursor = None
    next_cursor = None
    if raw_page:
        first_millis = _thread_list_sort_millis(raw_page[0])
        backwards_anchor = first_millis + 1 if sort_direction == "asc" else first_millis - 1
        backwards_cursor = _serialize_thread_list_cursor_millis(backwards_anchor)
        if more:
            next_cursor = _serialize_thread_list_cursor_millis(_thread_list_sort_millis(raw_page[-1]))
    page = [_strip_thread_list_sort_metadata(row) for row in raw_page]
    return {"data": page, "nextCursor": next_cursor, "backwardsCursor": backwards_cursor}


def _parse_thread_list_cursor(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise RemoteControlError(f"invalid cursor: {raw}")
    timestamp_millis = _parse_thread_list_cursor_millis(raw)
    if timestamp_millis is not None:
        return {"sortMillis": timestamp_millis}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RemoteControlError(f"invalid cursor: {raw}") from exc
    if not isinstance(parsed, dict):
        raise RemoteControlError(f"invalid cursor: {raw}")
    return parsed


def _serialize_thread_list_cursor(thread_id: str, *, include_anchor: bool) -> str:
    return json.dumps({"threadId": thread_id, "includeAnchor": include_anchor}, separators=(",", ":"))


def _thread_list_sort_millis(row: dict[str, Any]) -> int:
    return _optional_int(row.get("_threadListSortMillis")) or 0


def _strip_thread_list_sort_metadata(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(row)
    cleaned.pop("_threadListSortMillis", None)
    return cleaned


def _serialize_thread_list_cursor_millis(value: int) -> str:
    dt = datetime.fromtimestamp(max(0, value) / 1000, tz=timezone.utc)
    text = dt.isoformat(timespec="milliseconds")
    return text.replace("+00:00", "Z")


def _parse_thread_list_cursor_millis(raw: str) -> int | None:
    text = raw.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _default_thread_model_provider(config: Any) -> str:
    codex_config = getattr(config, "codex_config", None)
    provider = getattr(codex_config, "model_provider_id", None)
    return str(provider or "openai")


def _paginate_thread_turns(
    turns: list[dict[str, Any]],
    *,
    cursor: Any,
    limit: Any,
    sort_direction: Any,
) -> dict[str, Any]:
    if not turns:
        return {"data": [], "nextCursor": None, "backwardsCursor": None}
    anchor = _parse_thread_turns_cursor(cursor)
    page_size = _optional_int(limit) or 25
    page_size = max(1, min(page_size, 100))
    direction = str(sort_direction or "desc")
    if direction not in {"asc", "desc"}:
        direction = "desc"

    anchor_index: int | None = None
    if anchor is not None:
        turn_id = str(anchor.get("turnId") or anchor.get("turn_id") or "")
        for index, turn in enumerate(turns):
            if str(turn.get("id") or "") == turn_id:
                anchor_index = index
                break
        if anchor_index is None:
            raise RemoteControlError("invalid cursor: anchor turn is no longer present")

    indexed = list(enumerate(turns))
    if direction == "asc":
        if anchor is not None and anchor_index is not None:
            include_anchor = bool(anchor.get("includeAnchor") or anchor.get("include_anchor"))
            indexed = [(index, turn) for index, turn in indexed if index >= anchor_index] if include_anchor else [
                (index, turn) for index, turn in indexed if index > anchor_index
            ]
    else:
        indexed.reverse()
        if anchor is not None and anchor_index is not None:
            include_anchor = bool(anchor.get("includeAnchor") or anchor.get("include_anchor"))
            indexed = [(index, turn) for index, turn in indexed if index <= anchor_index] if include_anchor else [
                (index, turn) for index, turn in indexed if index < anchor_index
            ]
    more = len(indexed) > page_size
    indexed = indexed[:page_size]
    page = [turn for _index, turn in indexed]
    backwards_cursor = _serialize_thread_turns_cursor(str(page[0].get("id") or ""), include_anchor=True) if page else None
    next_cursor = _serialize_thread_turns_cursor(str(page[-1].get("id") or ""), include_anchor=False) if page and more else None
    return {"data": page, "nextCursor": next_cursor, "backwardsCursor": backwards_cursor}


def _parse_thread_turns_cursor(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw:
        raise RemoteControlError(f"invalid cursor: {raw}")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RemoteControlError(f"invalid cursor: {raw}") from exc
    if not isinstance(parsed, dict):
        raise RemoteControlError(f"invalid cursor: {raw}")
    return parsed


def _serialize_thread_turns_cursor(turn_id: str, *, include_anchor: bool) -> str:
    return json.dumps({"turnId": turn_id, "includeAnchor": include_anchor}, separators=(",", ":"))
