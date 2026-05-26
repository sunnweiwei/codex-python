from __future__ import annotations

import base64
import datetime as _dt
import json
import os
import platform
import shlex
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..auth import (
    auth_json_path,
    load_auth_snapshot,
)
from ..core import CodexSession
from ..state import load_rollout_records, parse_command_actions, reconstruct_history_from_rollout
from ..types import CodexConfig
from .constants import DEFAULT_REMOTE_PROCESS_OUTPUT_BYTES_CAP, PYTHON_REMOTE_CONTROL_VERSION
from .types import RemoteControlConfig, RemoteControlError
from .utils import (
    _codex_user_agent,
    _env_truthy,
    _optional_int,
    _optional_string,
    _remote_control_client_identity,
)

if TYPE_CHECKING:
    from .service import RemoteControlService
    from .types import RemoteControlConnectionStatus


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
    if mutates_global_identity and config.user_agent_override and suffix:
        user_agent = f"{config.user_agent_override.strip()} ({suffix})"
    else:
        user_agent = _codex_user_agent(originator, suffix, override=config.user_agent_override)
    return {
        "userAgent": user_agent,
        "codexHome": str(config.codex_home),
        "platformFamily": platform_family,
        "platformOs": platform_os,
    }


def _initialize_client_name(params: Any) -> str | None:
    if not isinstance(params, dict):
        return None
    client_info = params.get("clientInfo")
    if not isinstance(client_info, dict):
        return None
    name = client_info.get("name")
    return name if isinstance(name, str) else None


def _opt_out_notification_methods_from_initialize_params(params: Any) -> set[str]:
    if not isinstance(params, dict):
        return set()
    capabilities = params.get("capabilities")
    if not isinstance(capabilities, dict):
        return set()
    raw_methods = capabilities.get("optOutNotificationMethods")
    if not isinstance(raw_methods, list):
        return set()
    return {method for method in raw_methods if isinstance(method, str) and method}


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


def _thread_settings_payload(session: CodexSession) -> dict[str, Any]:
    config = session.config
    return {
        "cwd": str(config.resolved_cwd()),
        "approvalPolicy": config.approval_policy,
        "approvalsReviewer": "user",
        "sandboxPolicy": _sandbox_policy_payload(config),
        "activePermissionProfile": None,
        "model": config.model,
        "modelProvider": config.model_provider_id,
        "serviceTier": config.resolved_service_tier(),
        "effort": config.model_reasoning_effort,
        "summary": config.model_reasoning_summary,
        "collaborationMode": _collaboration_mode_payload(config),
        "personality": "pragmatic",
    }


def _collaboration_mode_payload(config: CodexConfig) -> dict[str, Any]:
    mode = _collaboration_mode_protocol_value(config.collaboration_mode)
    settings: dict[str, Any] = {
        "model": config.model,
        "reasoning_effort": config.model_reasoning_effort,
    }
    developer_instructions = _collaboration_mode_developer_instructions(config.collaboration_mode)
    if developer_instructions is not None:
        settings["developer_instructions"] = developer_instructions
    return {"mode": mode, "settings": settings}


def _collaboration_mode_protocol_value(value: str) -> str:
    normalized = value.strip().lower().replace("_", " ").replace("-", " ")
    if normalized == "plan":
        return "plan"
    if normalized == "pair programming":
        return "pair_programming"
    if normalized == "execute":
        return "execute"
    return "default"


def _collaboration_mode_developer_instructions(value: str) -> str | None:
    known_mode_names = "Default and Plan"
    normalized = _collaboration_mode_protocol_value(value)
    if normalized == "default":
        return (
            "# Collaboration Mode: Default\n\n"
            "You are now in Default mode. Any previous instructions for other modes (e.g. Plan mode) are no longer active.\n\n"
            "Your active mode changes only when new developer instructions with a different `<collaboration_mode>...</collaboration_mode>` change it; user requests or tool descriptions do not change mode by themselves. "
            f"Known mode names are {known_mode_names}.\n\n"
            "## request_user_input availability\n\n"
            "Use the `request_user_input` tool only when it is listed in the available tools for this turn.\n\n"
            "In Default mode, strongly prefer making reasonable assumptions and executing the user's request rather than stopping to ask questions. "
            "If you absolutely must ask a question because the answer cannot be discovered from local context and a reasonable assumption would be risky, ask the user directly with a concise plain-text question. "
            "Never write a multiple choice question as a textual assistant message.\n"
        )
    if normalized == "plan":
        return (
            "# Plan Mode (Conversational)\n\n"
            "You work in 3 phases, and you should chat your way to a great plan before finalizing it. "
            "A great plan is very detailed and decision complete, so that it can be handed to another engineer or agent to be implemented right away.\n\n"
            "You are in Plan Mode until a developer message explicitly ends it. Plan Mode is not changed by user intent, tone, or imperative language. "
            "If a user asks for execution while still in Plan Mode, treat it as a request to plan the execution, not perform it.\n\n"
            "Strongly prefer using the `request_user_input` tool to ask important questions. "
            "Only produce the final plan when it is decision complete and wrap it in a single `<proposed_plan>` block.\n"
        )
    return None


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
        from ..auth import auth_status

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
        from ..auth import fetch_chatgpt_rate_limits

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
    from ..goal import GOAL_STATUS_FROM_WIRE

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
    if not _env_truthy("PY_CODEX_REMOTE_CONTROL_DEBUG"):
        return
    payload = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "event": event,
        **{key: value for key, value in fields.items() if value is not None},
    }
    print(
        f"remote-control {json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}",
        file=sys.stderr,
        flush=True,
    )


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
    items_view: str | None = None,
) -> dict[str, Any]:
    duration_ms = None
    if started_at is not None and completed_at is not None:
        duration_ms = max(0, int((completed_at - started_at) * 1000))
    turn_items = list(items or [])
    effective_items_view = items_view or ("full" if turn_items else "notLoaded")
    if effective_items_view == "notLoaded":
        turn_items = []
    elif effective_items_view == "summary":
        turn_items = _summary_turn_items(turn_items)
    payload: dict[str, Any] = {
        "id": turn_id,
        "items": turn_items,
        "itemsView": effective_items_view,
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


def _summary_turn_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first_user_message = next((item for item in items if item.get("type") == "userMessage"), None)
    final_agent_message = next((item for item in reversed(items) if item.get("type") == "agentMessage"), None)
    if first_user_message is not None and final_agent_message is not None:
        if str(first_user_message.get("id") or "") != str(final_agent_message.get("id") or ""):
            return [first_user_message, final_agent_message]
    if first_user_message is not None:
        return [first_user_message]
    if final_agent_message is not None:
        return [final_agent_message]
    return []


def _apply_turn_items_view(turn: dict[str, Any], items_view: str) -> dict[str, Any]:
    normalized = dict(turn)
    items = list(normalized.get("items") or [])
    if items_view == "notLoaded":
        normalized["items"] = []
    elif items_view == "summary":
        normalized["items"] = _summary_turn_items([item for item in items if isinstance(item, dict)])
    elif items_view == "full":
        normalized["items"] = items
    normalized["itemsView"] = items_view
    return normalized


def _apply_turns_items_view(turns: list[dict[str, Any]], items_view: str) -> list[dict[str, Any]]:
    return [_apply_turn_items_view(turn, items_view) for turn in turns]


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


def _thread_payload_from_rollout(
    path: Path,
    config: RemoteControlConfig,
    *,
    include_turns: bool = True,
    items_view: str | None = None,
) -> dict[str, Any] | None:
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
        if items_view is not None:
            turns = _apply_turns_items_view(turns, items_view)
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


def _turns_from_history(session: CodexSession, *, items_view: str | None = None) -> list[dict[str, Any]]:
    rollout_path = getattr(session.state, "_rollout_path", None)
    if isinstance(rollout_path, Path) and rollout_path.exists():
        turns = _turns_from_rollout_records(load_rollout_records(rollout_path))
        if turns:
            return _apply_turns_items_view(turns, items_view) if items_view is not None else turns
    compact_items = _thread_items_from_response_history(session.state.history)
    if not compact_items:
        return []
    return [_turn_payload(session.state.turn_id, status="completed", items=compact_items, items_view=items_view)]


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


_REMOTE_LIVE_TOOL_NAMES = {"exec_command", "shell_command", "write_stdin"}


def _response_item_protocol_id(item: Any, *, item_id: Any | None = None) -> str:
    if item_id is not None:
        return str(item_id)
    if isinstance(item, dict):
        value = item.get("id") or item.get("call_id")
        if value is not None:
            return str(value)
    return ""


def _response_item_is_assistant_message(item: Any) -> bool:
    return isinstance(item, dict) and item.get("type") == "message" and item.get("role") == "assistant"


def _response_item_is_live_tool_echo(item: Any, live_tool_call_ids: set[str]) -> bool:
    if not isinstance(item, dict):
        return False
    response_type = item.get("type")
    call_id = str(item.get("call_id") or item.get("id") or "")
    if response_type in {"function_call", "custom_tool_call"}:
        name = str(item.get("name") or "")
        return name in _REMOTE_LIVE_TOOL_NAMES or call_id in live_tool_call_ids
    if response_type == "function_call_output":
        return call_id in live_tool_call_ids
    return False


def _is_agent_message_delta(
    payload: dict[str, Any],
    *,
    agent_message_item_ids: set[str],
    non_agent_delta_item_ids: set[str],
) -> bool:
    raw_type = str(payload.get("raw_type") or "")
    item_id = str(payload.get("item_id") or "")
    if raw_type in {
        "response.function_call_arguments.delta",
        "response.custom_tool_call_input.delta",
        "response.reasoning_summary_text.delta",
        "response.reasoning_text.delta",
    }:
        return False
    if item_id in non_agent_delta_item_ids and item_id not in agent_message_item_ids:
        return False
    if item_id in agent_message_item_ids:
        return True
    return raw_type in {"response.output_text.delta", "response.refusal.delta"}


def _command_execution_item_from_tool_started(session: CodexSession, payload: dict[str, Any]) -> dict[str, Any] | None:
    name = str(payload.get("name") or "")
    if name not in {"exec_command", "shell_command"}:
        return None
    call_id = str(payload.get("call_id") or "")
    if not call_id:
        return None
    arguments = payload.get("arguments")
    command, cwd = _command_from_tool_arguments(arguments)
    if not command:
        return None
    return _command_execution_item(
        call_id,
        command=command,
        cwd=cwd or str(session.config.resolved_cwd()),
        source="agent",
        status="inProgress",
        command_actions=_command_actions_from_payload(parse_command_actions(command), command),
    )


def _command_execution_item_from_tool_completed(
    session: CodexSession,
    payload: dict[str, Any],
    arguments: Any,
) -> dict[str, Any] | None:
    name = str(payload.get("name") or "")
    if name not in {"exec_command", "shell_command", "write_stdin"}:
        return None
    metadata = payload.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    if name == "write_stdin":
        item_id = str(metadata.get("event_call_id") or "")
    else:
        item_id = str(payload.get("call_id") or "")
    if not item_id:
        return None
    exit_code = _optional_int(metadata.get("exit_code"))
    ok = payload.get("ok")
    if exit_code is None and ok is True and metadata.get("session_id") is not None:
        return None
    command, cwd = _command_from_completed_tool_payload(session, payload, arguments, metadata)
    if not command:
        return None
    output = _completed_tool_output(payload, metadata)
    status = "completed"
    if exit_code is not None:
        status = "completed" if exit_code == 0 else "failed"
    elif ok is False:
        status = "failed"
    return _command_execution_item(
        item_id,
        command=command,
        cwd=cwd,
        process_id=_optional_string(metadata.get("session_id") or metadata.get("process_id")),
        source="agent",
        status=status,
        command_actions=_command_actions_from_payload(parse_command_actions(command), command),
        aggregated_output=output,
        exit_code=exit_code if exit_code is not None else (1 if ok is False else None),
        duration_ms=_duration_ms_from_tool_metadata(metadata),
    )


def _terminal_interaction_from_write_stdin(
    session: CodexSession,
    payload: dict[str, Any],
    arguments: Any,
) -> dict[str, Any] | None:
    if str(payload.get("name") or "") != "write_stdin":
        return None
    metadata = payload.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    args = arguments if isinstance(arguments, dict) else {}
    item_id = str(metadata.get("event_call_id") or "")
    process_id = args.get("session_id")
    if not item_id or process_id is None:
        return None
    return {
        "threadId": session.state.thread_id,
        "turnId": session.state.turn_id,
        "itemId": item_id,
        "processId": str(process_id),
        "stdin": str(args.get("chars") or ""),
    }


def _command_from_completed_tool_payload(
    session: CodexSession,
    payload: dict[str, Any],
    arguments: Any,
    metadata: dict[str, Any],
) -> tuple[str, str]:
    command = _command_string(metadata.get("command"))
    cwd = _optional_string(metadata.get("workdir") or metadata.get("cwd"))
    if not command:
        command, arg_cwd = _command_from_tool_arguments(arguments)
        cwd = cwd or arg_cwd
    return command, cwd or str(session.config.resolved_cwd())


def _completed_tool_output(payload: dict[str, Any], metadata: dict[str, Any]) -> str | None:
    if any(
        key in metadata
        for key in (
            "chunk_id",
            "wall_time_seconds",
            "session_id",
            "exit_code",
            "stdout",
            "stderr",
            "aggregated_output",
        )
    ):
        for key in ("aggregated_output", "stdout", "output"):
            value = metadata.get(key)
            if isinstance(value, str):
                return _strip_unified_exec_response_metadata(value)
        return ""
    for key in ("aggregated_output", "output", "stdout"):
        value = metadata.get(key)
        if isinstance(value, str) and value:
            return _strip_unified_exec_response_metadata(value)
    output = payload.get("output")
    return _strip_unified_exec_response_metadata(output) if isinstance(output, str) and output else None


def _file_change_item_from_apply_patch_completed(payload: dict[str, Any]) -> dict[str, Any] | None:
    if str(payload.get("name") or "") != "apply_patch":
        return None
    metadata = payload.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    changes = _protocol_file_update_changes(metadata.get("changes"))
    if not changes:
        return None
    status = "completed" if payload.get("ok") else "failed"
    return {
        "type": "fileChange",
        "id": str(payload.get("call_id") or f"file_change_{uuid.uuid4().hex}"),
        "changes": changes,
        "status": status,
    }


def _protocol_file_update_changes(raw_changes: Any) -> list[dict[str, Any]]:
    if isinstance(raw_changes, dict):
        iterable: list[Any] = []
        for path, change in raw_changes.items():
            if not isinstance(change, dict):
                continue
            normalized = dict(change)
            normalized.setdefault("path", path)
            iterable.append(normalized)
    elif isinstance(raw_changes, list):
        iterable = raw_changes
    else:
        return []
    out: list[dict[str, Any]] = []
    for raw in iterable:
        if not isinstance(raw, dict):
            continue
        path = str(raw.get("path") or "")
        if not path:
            continue
        kind_value = str(raw.get("type") or "update")
        if kind_value == "add":
            kind: dict[str, Any] = {"type": "add"}
            diff = str(raw.get("content") or "")
        elif kind_value == "delete":
            kind = {"type": "delete"}
            diff = str(raw.get("content") or "")
        else:
            move_path = raw.get("move_path")
            kind = {"type": "update", "movePath": str(move_path) if move_path else None}
            diff = str(raw.get("unified_diff") or "")
            if move_path:
                diff = f"{diff}\n\nMoved to: {move_path}"
        out.append({"path": path, "kind": kind, "diff": diff})
    out.sort(key=lambda item: str(item.get("path") or ""))
    return out


def _strip_unified_exec_response_metadata(text: str) -> str:
    if "\nOutput:\n" not in text:
        return text
    head, output = text.split("\nOutput:\n", 1)
    metadata_lines = head.splitlines()
    known_prefixes = (
        "Chunk ID:",
        "Wall time:",
        "Process exited with code ",
        "Process running with session ID ",
        "Original token count:",
    )
    if metadata_lines and all(line.startswith(known_prefixes) for line in metadata_lines):
        return output
    return text


def _duration_ms_from_tool_metadata(metadata: dict[str, Any]) -> int | None:
    direct = _duration_ms_from_event_payload(metadata)
    if direct is not None:
        return direct
    seconds = metadata.get("wall_time_seconds")
    try:
        return int(float(seconds) * 1000)
    except (TypeError, ValueError):
        return None


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
        return int(_dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except Exception:
        try:
            return int(_dt.datetime.fromisoformat(value[:19]).replace(tzinfo=_dt.timezone.utc).timestamp())
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


__all__ = [name for name in globals() if name.startswith("_")]
