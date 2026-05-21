from __future__ import annotations

import json
import sys
import subprocess
import tempfile

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence

from .prompts import (
    ASSETS_DIR,
    PACKAGE_DIR,
    PINNED_UPSTREAM_COMMIT,
    verify_asset_hashes,
)
from .state import UPSTREAM_ROLLOUT_ITEM_TYPES, reconstruct_history_from_rollout
from .tools import ToolRuntime
from .types import CodexConfig, KNOWN_EVENT_TYPES, TERMINAL_TURN_EVENT_TYPES


# Upstream awareness lives here (parity tool only) — the rest of the agent
# package is upstream-agnostic. The upstream source location can be overridden
# via the CODEX_UPSTREAM_DIR env var.
import os as _os
_UPSTREAM_ENV = _os.environ.get("CODEX_UPSTREAM_DIR")
UPSTREAM_DIR = Path(_UPSTREAM_ENV).expanduser() if _UPSTREAM_ENV else PACKAGE_DIR / "upstream" / "openai-codex"

_UPSTREAM_ASSET_PATHS = {
    "prompts/gpt_5_codex_prompt.md": "codex-rs/core/gpt_5_codex_prompt.md",
    "prompts/gpt_5_2_prompt.md": "codex-rs/core/gpt_5_2_prompt.md",
    "prompts/gpt-5.2-codex_prompt.md": "codex-rs/core/gpt-5.2-codex_prompt.md",
    "prompts/prompt_with_apply_patch_instructions.md": "codex-rs/core/prompt_with_apply_patch_instructions.md",
    "prompts/compact/prompt.md": "codex-rs/core/templates/compact/prompt.md",
    "prompts/compact/summary_prefix.md": "codex-rs/core/templates/compact/summary_prefix.md",
    "prompts/memories/read_path.md": "codex-rs/memories/read/templates/memories/read_path.md",
    "prompts/memories/write/stage_one_system.md": "codex-rs/memories/write/templates/memories/stage_one_system.md",
    "prompts/memories/write/stage_one_input.md": "codex-rs/memories/write/templates/memories/stage_one_input.md",
    "prompts/memories/write/consolidation.md": "codex-rs/memories/write/templates/memories/consolidation.md",
    "prompts/memories/write/extensions/ad_hoc/instructions.md": "codex-rs/memories/write/templates/extensions/ad_hoc/instructions.md",
    "grammars/apply_patch.lark": "codex-rs/core/src/tools/handlers/apply_patch.lark",
}


def compare_assets_to_upstream() -> dict[str, bool]:
    result = {}
    for asset_path, upstream_path in _UPSTREAM_ASSET_PATHS.items():
        upstream = UPSTREAM_DIR / upstream_path
        result[asset_path] = upstream.exists() and (ASSETS_DIR / asset_path).read_bytes() == upstream.read_bytes()
    return result


MANIFEST_PATH = PACKAGE_DIR / "parity_manifest.json"
AUDIT_PATH = PACKAGE_DIR / "PARITY_AUDIT.md"

MODULE_STATUS_VALUES = {"missing", "scaffold", "partial", "complete", "parity", "blocked"}
GATE_STATUS_VALUES = {
    "blocked",
    "failed",
    "missing",
    "not_applicable",
    "out_of_scope",
    "partial",
    "passed",
    "passed_static",
    "scaffold",
}


@dataclass(frozen=True)
class ParityCheckResult:
    name: str
    ok: bool
    status: str
    detail: str
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "ok": self.ok,
            "status": self.status,
            "detail": self.detail,
        }
        if self.data:
            payload["data"] = self.data
        return payload


@dataclass(frozen=True)
class ParityReport:
    overall_status: str
    checks: list[ParityCheckResult]
    modules: dict[str, str]

    @property
    def ok(self) -> bool:
        return all(check.ok for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status,
            "ok": self.ok,
            "modules": self.modules,
            "checks": [check.to_dict() for check in self.checks],
        }


def run_parity_checks() -> ParityReport:
    """Run the local, non-oracle parity framework checks.

    These checks do not call the official Codex binary and do not claim full
    parity. They verify the fixed scaffolding that later module work must pass.
    """

    manifest = load_manifest()
    checks = [
        _check_manifest_shape(manifest),
        _check_upstream_commit(manifest),
        _check_prompt_assets(),
        _check_default_tool_surface(manifest),
        _check_invalid_tools_absent(manifest),
        _check_tool_classification(manifest),
        _check_audit_coverage(manifest),
        _check_module_lifecycle(manifest),
        _check_session_lifecycle_catalog(),
        _check_agent_lifecycle_contract(),
        _check_rollout_item_catalog(),
        _check_rollout_reconstruction_api(),
    ]
    modules = _manifest_modules(manifest)
    if any(not check.ok for check in checks):
        overall_status = "failed"
    elif modules and all(status == "parity" for status in modules.values()):
        overall_status = "parity"
    else:
        overall_status = "partial"
    return ParityReport(overall_status, checks, modules)


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def format_parity_report(report: ParityReport) -> str:
    lines = [
        f"Python Codex parity status: {report.overall_status}",
        f"Framework checks: {'passed' if report.ok else 'failed'}",
        "",
        "Checks:",
    ]
    for check in report.checks:
        mark = "OK" if check.ok else "FAIL"
        lines.append(f"- {mark} {check.name}: {check.status} - {check.detail}")
    if report.modules:
        lines.extend(["", "Modules:"])
        for name, status in sorted(report.modules.items()):
            lines.append(f"- {name}: {status}")
    return "\n".join(lines)


def load_jsonl_events(path: str | Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def normalize_exec_trace(events: Sequence[Any]) -> list[dict[str, Any]]:
    """Normalize Python-port or official `codex exec --json` events.

    This is intentionally lossy: it keeps the facts useful for parity work
    (tool sequence, shell commands, tool status, final-message shape) and drops
    volatile ids, timestamps, token counts, and live model wording.
    """

    steps: list[dict[str, Any]] = []
    for raw_event in events:
        event = _event_dict(raw_event)
        event_type = event.get("type")
        if event_type == "tool.started":
            step = _python_tool_started_step(event)
        elif event_type == "tool.completed":
            step = _python_tool_completed_step(event)
        elif event_type in {"item.started", "item.completed"}:
            step = _thread_item_step(event.get("item"), event_type)
            if step is None and event_type == "item.completed":
                step = _python_response_item_step(event.get("item"))
        else:
            step = None
        if step is not None:
            steps.append(step)
    return steps


def summarize_exec_trace(events: Sequence[Any]) -> dict[str, Any]:
    steps = normalize_exec_trace(events)
    tool_sequence = [
        step["name"]
        for step in steps
        if step.get("kind") in {"tool_start", "hosted_tool", "file_change", "plan_update"}
        and isinstance(step.get("name"), str)
    ]
    commands = [
        str(step.get("command"))
        for step in steps
        if step.get("kind") == "tool_start" and step.get("name") in {"exec_command", "shell_command"}
    ]
    final_messages = [step for step in steps if step.get("kind") == "assistant_message"]
    final_message = final_messages[-1] if final_messages else {}
    return {
        "step_count": len(steps),
        "tool_sequence": tool_sequence,
        "tool_counts": _counts(tool_sequence),
        "commands": commands,
        "assistant_shape": {
            "chars": final_message.get("chars", 0),
            "lines": final_message.get("lines", 0),
            "has_tree": final_message.get("has_tree", False),
            "asks_followup": final_message.get("asks_followup", False),
        },
    }


def compare_exec_traces(left_events: Sequence[Any], right_events: Sequence[Any]) -> dict[str, Any]:
    left = summarize_exec_trace(left_events)
    right = summarize_exec_trace(right_events)
    return {
        "left": left,
        "right": right,
        "matches": {
            "tool_sequence": left["tool_sequence"] == right["tool_sequence"],
            "tool_counts": left["tool_counts"] == right["tool_counts"],
            "commands": left["commands"] == right["commands"],
            "assistant_shape": left["assistant_shape"] == right["assistant_shape"],
        },
    }


def format_trace_report(events_by_label: Sequence[tuple[str, Sequence[Any]]]) -> str:
    lines: list[str] = []
    summaries: list[tuple[str, dict[str, Any]]] = []
    for label, events in events_by_label:
        summary = summarize_exec_trace(events)
        summaries.append((label, summary))
        lines.append(f"Trace: {label}")
        lines.append(f"- steps: {summary['step_count']}")
        lines.append(f"- tools: {_format_counts(summary['tool_counts'])}")
        if summary["commands"]:
            lines.append("- commands:")
            lines.extend(f"  - {command}" for command in summary["commands"])
        shape = summary["assistant_shape"]
        lines.append(
            "- final shape: "
            f"{shape['chars']} chars, {shape['lines']} lines, "
            f"tree={'yes' if shape['has_tree'] else 'no'}, "
            f"followup={'yes' if shape['asks_followup'] else 'no'}"
        )
        lines.append("")
    if len(events_by_label) == 2:
        comparison = compare_exec_traces(events_by_label[0][1], events_by_label[1][1])
        lines.append("Comparison:")
        for key, value in comparison["matches"].items():
            lines.append(f"- {key}: {'match' if value else 'different'}")
    return "\n".join(lines).rstrip()


def current_default_tool_names(config: CodexConfig | None = None) -> list[str]:
    runtime = ToolRuntime(config or CodexConfig(skip_git_repo_check=True, ephemeral=True))
    return [definition.name for definition in runtime.definitions()]


def _event_dict(raw_event: Any) -> dict[str, Any]:
    if isinstance(raw_event, dict):
        return raw_event
    to_dict = getattr(raw_event, "to_dict", None)
    if callable(to_dict):
        value = to_dict()
        return value if isinstance(value, dict) else {}
    return {}


def _python_tool_started_step(event: dict[str, Any]) -> dict[str, Any]:
    name = str(event.get("name") or "")
    arguments = event.get("arguments")
    args = arguments if isinstance(arguments, dict) else {}
    step: dict[str, Any] = {"kind": "tool_start", "name": name}
    if name in {"exec_command", "shell_command"}:
        command = args.get("cmd") or args.get("command")
        if isinstance(command, str):
            step["command"] = command
    if name == "web_search" and isinstance(args.get("query"), str):
        step["query"] = args["query"]
    return step


def _python_tool_completed_step(event: dict[str, Any]) -> dict[str, Any] | None:
    name = str(event.get("name") or "")
    metadata = event.get("metadata")
    meta = metadata if isinstance(metadata, dict) else {}
    if name in {"exec_command", "shell_command", "write_stdin"}:
        return {
            "kind": "tool_end",
            "name": "exec_command" if name == "write_stdin" else name,
            "status": _command_status(bool(event.get("ok")), meta.get("exit_code")),
            "exit_code": meta.get("exit_code"),
            "output_chars": len(str(meta.get("aggregated_output") or meta.get("output") or event.get("output") or "")),
        }
    if name == "apply_patch":
        return {"kind": "file_change", "name": "apply_patch", "status": "completed" if event.get("ok") else "failed"}
    if name == "update_plan":
        return {"kind": "plan_update", "name": "update_plan"}
    if name == "web_search":
        return {"kind": "hosted_tool", "name": "web_search"}
    if name in {"spawn_agent", "send_input", "resume_agent", "wait_agent", "close_agent"}:
        return {"kind": "tool_end", "name": name, "status": "completed" if event.get("ok") else "failed"}
    return {"kind": "tool_end", "name": name, "status": "completed" if event.get("ok") else "failed"} if name else None


def _thread_item_step(item: Any, event_type: str) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    item_type = item.get("type")
    if item_type == "agent_message":
        return _assistant_message_step(str(item.get("text") or ""))
    if item_type == "reasoning":
        return {"kind": "reasoning", "chars": len(str(item.get("text") or ""))}
    if item_type == "command_execution":
        name = "exec_command"
        if event_type == "item.started":
            return {"kind": "tool_start", "name": name, "command": str(item.get("command") or "")}
        return {
            "kind": "tool_end",
            "name": name,
            "status": str(item.get("status") or ""),
            "exit_code": item.get("exit_code"),
            "output_chars": len(str(item.get("aggregated_output") or "")),
        }
    if item_type == "file_change":
        return {"kind": "file_change", "name": "apply_patch", "status": str(item.get("status") or "")}
    if item_type == "web_search":
        return {"kind": "hosted_tool", "name": "web_search", "query": str(item.get("query") or "")}
    if item_type == "todo_list":
        return {"kind": "plan_update", "name": "update_plan"}
    if item_type == "collab_tool_call":
        return {"kind": "tool_start" if event_type == "item.started" else "tool_end", "name": str(item.get("tool") or "agent_tool")}
    if item_type == "mcp_tool_call":
        tool = item.get("tool")
        server = item.get("server")
        name = f"{server}/{tool}" if server and tool else str(tool or "mcp_tool")
        return {"kind": "tool_start" if event_type == "item.started" else "tool_end", "name": name}
    return None


def _python_response_item_step(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    item_type = item.get("type")
    if item_type == "message" and item.get("role") == "assistant":
        return _assistant_message_step(_message_text(item))
    if item_type == "reasoning":
        return {"kind": "reasoning", "chars": len(_reasoning_text(item))}
    if item_type == "web_search_call":
        return {"kind": "hosted_tool", "name": "web_search", "query": _web_search_query(item)}
    return None


def _assistant_message_step(text: str) -> dict[str, Any]:
    return {
        "kind": "assistant_message",
        "chars": len(text),
        "lines": len(text.splitlines()) if text else 0,
        "has_tree": any(marker in text for marker in ("├", "└", "|--", "`--")),
        "asks_followup": _asks_followup(text),
    }


def _message_text(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    content = item.get("content")
    if not isinstance(content, list):
        return ""
    for part in content:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            chunks.append(part["text"])
    return "".join(chunks)


def _reasoning_text(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in ("summary", "content"):
        value = item.get(key)
        if isinstance(value, list):
            for part in value:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    chunks.append(part["text"])
                elif isinstance(part, str):
                    chunks.append(part)
        elif isinstance(value, str):
            chunks.append(value)
    return "\n".join(chunks)


def _web_search_query(item: dict[str, Any]) -> str:
    action = item.get("action")
    if isinstance(action, dict) and isinstance(action.get("query"), str):
        return action["query"]
    query = item.get("query")
    return query if isinstance(query, str) else ""


def _asks_followup(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    tail = stripped[-500:]
    return "?" in tail or "？" in tail or "你希望" in tail or "要不要" in tail


def _command_status(ok: bool, exit_code: Any) -> str:
    if exit_code is None:
        return "in_progress" if ok else "failed"
    try:
        return "completed" if int(exit_code) == 0 else "failed"
    except (TypeError, ValueError):
        return "completed" if ok else "failed"


def _counts(values: Sequence[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "(none)"
    return ", ".join(f"{name} x{count}" for name, count in sorted(counts.items()))


def _check_manifest_shape(manifest: dict[str, Any]) -> ParityCheckResult:
    missing = [
        key
        for key in ("schema_version", "audit_scope", "upstream", "python_current", "module_lifecycle")
        if key not in manifest
    ]
    ok = not missing and manifest.get("schema_version") == 1
    detail = "manifest has required top-level sections" if ok else f"manifest missing/invalid: {missing}"
    return ParityCheckResult("manifest_shape", ok, "passed" if ok else "failed", detail)


def _check_upstream_commit(manifest: dict[str, Any]) -> ParityCheckResult:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=UPSTREAM_DIR,
            text=True,
            capture_output=True,
            check=True,
        )
        git_commit = completed.stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        return ParityCheckResult(
            "upstream_commit",
            False,
            "failed",
            f"could not read upstream commit: {exc}",
        )
    manifest_commit = manifest.get("upstream", {}).get("commit")
    ok = git_commit == PINNED_UPSTREAM_COMMIT == manifest_commit
    return ParityCheckResult(
        "upstream_commit",
        ok,
        "passed" if ok else "failed",
        "pinned upstream commit matches local checkout" if ok else "pinned upstream commit mismatch",
        {
            "git": git_commit,
            "manifest": manifest_commit,
            "pinned": PINNED_UPSTREAM_COMMIT,
        },
    )


def _check_prompt_assets() -> ParityCheckResult:
    hash_results = verify_asset_hashes()
    upstream_results = compare_assets_to_upstream()
    failed_hashes = sorted(path for path, ok in hash_results.items() if not ok)
    failed_upstream = sorted(path for path, ok in upstream_results.items() if not ok)
    ok = not failed_hashes and not failed_upstream
    return ParityCheckResult(
        "prompt_assets",
        ok,
        "passed" if ok else "failed",
        "copied assets match pinned hashes and upstream files" if ok else "copied asset mismatch",
        {"failed_hashes": failed_hashes, "failed_upstream": failed_upstream},
    )


def _check_default_tool_surface(manifest: dict[str, Any]) -> ParityCheckResult:
    platform_key = "default_exec_tools_windows" if sys.platform == "win32" else "default_exec_tools_non_windows"
    expected = list(manifest.get("upstream", {}).get(platform_key, []))
    current = current_default_tool_names()
    ok = current == expected
    return ParityCheckResult(
        "default_tool_surface",
        ok,
        "passed" if ok else "failed",
        "current default tool names match upstream default order" if ok else "default tool names differ",
        {"expected": expected, "current": current},
    )


def _check_invalid_tools_absent(manifest: dict[str, Any]) -> ParityCheckResult:
    current = set(current_default_tool_names())
    status = manifest.get("python_current", {}).get("tool_status", {}).get("search_files", {}).get("status")
    ok = "search_files" not in current and status == "removed_invalid"
    return ParityCheckResult(
        "invalid_tools_absent",
        ok,
        "passed" if ok else "failed",
        "`search_files` is absent from runtime and classified as removed_invalid" if ok else "`search_files` invalid state",
        {"search_files_status": status, "current": sorted(current)},
    )


def _check_tool_classification(manifest: dict[str, Any]) -> ParityCheckResult:
    classified = set(manifest.get("python_current", {}).get("tool_status", {}))
    current = set(current_default_tool_names())
    missing = sorted(current - classified)
    ok = not missing
    return ParityCheckResult(
        "tool_classification",
        ok,
        "passed" if ok else "failed",
        "all current default tools are classified in manifest" if ok else "unclassified current tools",
        {"missing": missing},
    )


def _check_audit_coverage(manifest: dict[str, Any]) -> ParityCheckResult:
    try:
        audit_text = AUDIT_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ParityCheckResult("audit_coverage", False, "failed", "PARITY_AUDIT.md is missing")

    required_phrases = [
        "Memory status: partial",
        "Compaction status: partial",
        "## Module Lifecycle Framework",
    ]
    current_tools = current_default_tool_names()
    missing_phrases = [phrase for phrase in required_phrases if phrase not in audit_text]
    missing_tools = [tool for tool in current_tools if f"`{tool}`" not in audit_text]
    ok = not missing_phrases and not missing_tools
    return ParityCheckResult(
        "audit_coverage",
        ok,
        "passed" if ok else "failed",
        "audit covers required statuses and current tool names" if ok else "audit coverage gaps",
        {"missing_phrases": missing_phrases, "missing_tools": missing_tools},
    )


def _check_module_lifecycle(manifest: dict[str, Any]) -> ParityCheckResult:
    modules_obj = manifest.get("module_lifecycle", {}).get("modules", {})
    if not isinstance(modules_obj, dict) or not modules_obj:
        return ParityCheckResult("module_lifecycle", False, "failed", "module_lifecycle.modules is missing")

    bad_modules: dict[str, str] = {}
    bad_gates: dict[str, dict[str, str]] = {}
    for name, module in modules_obj.items():
        if not isinstance(module, dict):
            bad_modules[name] = "not an object"
            continue
        status = module.get("status")
        if status not in MODULE_STATUS_VALUES:
            bad_modules[name] = str(status)
        gates = module.get("gates", {})
        if not isinstance(gates, dict):
            bad_gates[name] = {"<module>": "gates is not an object"}
            continue
        invalid = {gate: str(value) for gate, value in gates.items() if value not in GATE_STATUS_VALUES}
        if invalid:
            bad_gates[name] = invalid

    ok = not bad_modules and not bad_gates
    return ParityCheckResult(
        "module_lifecycle",
        ok,
        "passed" if ok else "failed",
        "module lifecycle statuses and gates are machine-checkable" if ok else "invalid lifecycle status values",
        {"bad_modules": bad_modules, "bad_gates": bad_gates},
    )


def _check_session_lifecycle_catalog() -> ParityCheckResult:
    required = {
        "thread.started",
        "turn.started",
        "item.started",
        "item.delta",
        "item.completed",
        "model.request",
        "model.response",
        "token_count",
        "tool.started",
        "tool.completed",
        "turn.completed",
        "turn.aborted",
        "turn.failed",
        "context_compaction.started",
        "context_compaction.completed",
        "warning",
    }
    missing = sorted(required - KNOWN_EVENT_TYPES)
    terminal_missing = sorted({"turn.completed", "turn.aborted", "turn.failed"} - TERMINAL_TURN_EVENT_TYPES)
    ok = not missing and not terminal_missing
    return ParityCheckResult(
        "session_lifecycle_catalog",
        ok,
        "passed" if ok else "failed",
        "canonical high-level exec/compaction event catalog is fixed" if ok else "lifecycle catalog is incomplete",
        {"missing": missing, "terminal_missing": terminal_missing},
    )


def _check_agent_lifecycle_contract() -> ParityCheckResult:
    required_files = {
        "upstream_session": UPSTREAM_DIR / "codex-rs" / "core" / "src" / "session" / "mod.rs",
        "upstream_turn": UPSTREAM_DIR / "codex-rs" / "core" / "src" / "session" / "turn.rs",
        "upstream_turn_state": UPSTREAM_DIR / "codex-rs" / "core" / "src" / "state" / "turn.rs",
        "upstream_marker": UPSTREAM_DIR / "codex-rs" / "core" / "src" / "context" / "turn_aborted.rs",
        "python_core": PACKAGE_DIR / "core.py",
        "python_state": PACKAGE_DIR / "state.py",
    }
    try:
        texts = {name: path.read_text(encoding="utf-8") for name, path in required_files.items()}
    except FileNotFoundError as exc:
        return ParityCheckResult("agent_lifecycle_contract", False, "failed", f"missing lifecycle source: {exc}")

    required_snippets = {
        "upstream_session.steer_input": ("upstream_session", "pub async fn steer_input"),
        "upstream_session.get_pending_input": ("upstream_session", "pub async fn get_pending_input"),
        "upstream_session.interrupt_task": ("upstream_session", "pub async fn interrupt_task"),
        "upstream_turn.drain_pending": ("upstream_turn", "sess.get_pending_input().await"),
        "upstream_turn_state.pending": ("upstream_turn_state", "push_pending_input"),
        "upstream_marker": ("upstream_marker", "<turn_aborted>"),
        "python_core.steer_input": ("python_core", "def steer_input"),
        "python_core.take_pending": ("python_core", "def _take_pending_input"),
        "python_core.turn_aborted_event": ("python_core", "turn.aborted"),
        "python_core.turn_aborted_marker": ("python_core", "<turn_aborted>"),
        "python_state.turn_aborted_rollout": ("python_state", "turn_aborted"),
    }
    missing = sorted(name for name, (file_key, snippet) in required_snippets.items() if snippet not in texts[file_key])
    ok = not missing
    return ParityCheckResult(
        "agent_lifecycle_contract",
        ok,
        "passed_static" if ok else "failed",
        "upstream and Python lifecycle sources expose pending-input and abort-marker contracts"
        if ok
        else "pending-input or abort-marker contract snippets are missing",
        {"missing": missing},
    )


def _check_rollout_item_catalog() -> ParityCheckResult:
    required = {"session_meta", "turn_context", "event_msg", "response_item", "compacted"}
    missing = sorted(required - UPSTREAM_ROLLOUT_ITEM_TYPES)
    ok = not missing
    return ParityCheckResult(
        "rollout_item_catalog",
        ok,
        "passed" if ok else "failed",
        "upstream rollout item types are fixed in state layer" if ok else "rollout item catalog is incomplete",
        {"missing": missing},
    )


def _check_rollout_reconstruction_api() -> ParityCheckResult:
    user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]}
    assistant = {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "ok"}]}
    result = reconstruct_history_from_rollout(
        [
            {"type": "turn_context", "payload": {"turn_id": "turn", "model": "gpt-test"}},
            {"type": "response_item", "payload": user},
            {"type": "response_item", "payload": assistant},
        ]
    )
    ok = (
        result.history == [user, assistant]
        and result.previous_turn_settings == {"model": "gpt-test", "realtime_active": None}
        and result.reference_context_item == {"turn_id": "turn", "model": "gpt-test"}
    )
    return ParityCheckResult(
        "rollout_reconstruction_api",
        ok,
        "passed" if ok else "failed",
        "rollout reconstruction helper materializes response history and turn metadata"
        if ok
        else "rollout reconstruction helper did not return the expected history or metadata",
        {"history_len": len(result.history)},
    )


def _manifest_modules(manifest: dict[str, Any]) -> dict[str, str]:
    modules_obj = manifest.get("module_lifecycle", {}).get("modules", {})
    if not isinstance(modules_obj, dict):
        return {}
    modules = {}
    for name, module in modules_obj.items():
        if isinstance(module, dict) and isinstance(module.get("status"), str):
            modules[name] = module["status"]
    return modules


@dataclass(frozen=True)
class CodexExecOptions:
    cwd: str | Path | None = None
    codex_path: str = "codex"
    model: str | None = None
    sandbox: str | None = "workspace-write"
    approval_policy: str | None = "never"
    skip_git_repo_check: bool = True
    ephemeral: bool = True
    json_events: bool = True
    color: str = "never"
    output_last_message: str | Path | None = None
    extra_args: Sequence[str] = field(default_factory=tuple)
    timeout: float | None = None


@dataclass(frozen=True)
class CodexCLIResult:
    returncode: int
    final_message: str
    events: list[dict[str, Any]]
    stdout: str
    stderr: str
    command: list[str]

    @property
    def ok(self) -> bool:
        return self.returncode == 0


class CodexCLIOracle:
    """Parity helper that may call the official Codex CLI.

    Native `codex` execution never uses this class.
    """

    def __init__(self, options: CodexExecOptions | None = None):
        self.options = options or CodexExecOptions()

    def run(self, prompt: str) -> CodexCLIResult:
        output_file, cleanup = self._output_file()
        command = self._build_command(output_file)
        try:
            completed = subprocess.run(
                command,
                input=prompt,
                text=True,
                capture_output=True,
                cwd=self._resolved_cwd(),
                timeout=self.options.timeout,
                check=False,
            )
            events = list(_iter_json_objects(completed.stdout))
            final_message = _read_text(output_file) or _infer_final_message(events)
            return CodexCLIResult(
                completed.returncode,
                final_message,
                events,
                completed.stdout,
                completed.stderr,
                command,
            )
        finally:
            if cleanup:
                Path(output_file).unlink(missing_ok=True)

    def _build_command(self, output_file: str) -> list[str]:
        opts = self.options
        command = [opts.codex_path, "exec"]
        if opts.model:
            command.extend(["--model", opts.model])
        if opts.sandbox:
            command.extend(["--sandbox", opts.sandbox])
        if opts.approval_policy:
            command.extend(["--ask-for-approval", opts.approval_policy])
        cwd = self._resolved_cwd()
        if cwd is not None:
            command.extend(["--cd", cwd])
        if opts.skip_git_repo_check:
            command.append("--skip-git-repo-check")
        if opts.ephemeral:
            command.append("--ephemeral")
        if opts.json_events:
            command.append("--json")
        if opts.color:
            command.extend(["--color", opts.color])
        command.extend(["--output-last-message", output_file])
        command.extend(opts.extra_args)
        command.append("-")
        return command

    def _resolved_cwd(self) -> str | None:
        if self.options.cwd is None:
            return None
        return str(Path(self.options.cwd).resolve())

    def _output_file(self) -> tuple[str, bool]:
        if self.options.output_last_message is not None:
            return str(self.options.output_last_message), False
        handle = tempfile.NamedTemporaryFile(prefix="codex-oracle-last-message-", delete=False)
        handle.close()
        return handle.name, True


def _iter_json_objects(text: str) -> Iterator[dict[str, Any]]:
    for line in text.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            yield value


def _read_text(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return ""


def _infer_final_message(events: Sequence[dict[str, Any]]) -> str:
    for event in reversed(events):
        for key in ("final_message", "last_message", "message", "output_text"):
            value = event.get(key)
            if isinstance(value, str) and value:
                return value
    return ""
