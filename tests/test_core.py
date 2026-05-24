from __future__ import annotations

import json
import base64
import io
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

from contextlib import redirect_stderr
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from copy import deepcopy
from pathlib import Path
from typing import Any

# --- safe import shim (do not remove) -------------------------------------
# Every `from codex.* import X` below is wrapped so that if a candidate
# implementation is missing symbol X, the test module still loads. Only the
# tests that actually touch X fail (with a clear ImportError) instead of the
# whole suite collapsing at import time.
import importlib as _importlib

def _codex_unavail(qualname, exc):
    msg = "{} is not implemented in this codex package: {}: {}".format(
        qualname, type(exc).__name__, exc
    )
    class _Unavailable:
        def __call__(self, *a, **k): raise ImportError(msg)
        def __getattr__(self, n): raise ImportError(msg)
        def __iter__(self): raise ImportError(msg)
        def __bool__(self): return False
        def __repr__(self): return "<unavailable {}>".format(qualname)
    _Unavailable.__name__ = qualname.rsplit(".", 1)[-1]
    return _Unavailable()

def _codex_safe_from(modname, *names_with_aliases):
    """names_with_aliases items are either 'Name' or 'Name as Alias'."""
    pairs = []
    for spec in names_with_aliases:
        spec = spec.strip()
        if " as " in spec:
            name, alias = [p.strip() for p in spec.split(" as ")]
        else:
            name = alias = spec
        pairs.append((name, alias))
    try:
        mod = _importlib.import_module(modname)
    except Exception as _e:
        for name, alias in pairs:
            globals()[alias] = _codex_unavail(modname + "." + name, _e)
        return
    for name, alias in pairs:
        try:
            globals()[alias] = getattr(mod, name)
        except AttributeError as _e:
            globals()[alias] = _codex_unavail(modname + "." + name, _e)

def _codex_safe_import(modname, alias):
    try:
        globals()[alias] = _importlib.import_module(modname)
    except Exception as _e:
        globals()[alias] = _codex_unavail(modname, _e)
# --- end safe import shim -------------------------------------------------

# --- subprocess.run default-timeout guard (do not remove) -----------------
# A broken candidate may hang when the test launches `python -m codex ...` as
# a subprocess (e.g. it tries to call OpenAI instead of honoring the fake-
# responses env). Default every subprocess.run() to a generous timeout so a
# single hung test cannot stall the whole suite.
import subprocess as _codex_subprocess
_codex_subprocess_run_orig = _codex_subprocess.run
def _codex_subprocess_run(*args, **kwargs):
    kwargs.setdefault("timeout", 30)
    return _codex_subprocess_run_orig(*args, **kwargs)
_codex_subprocess.run = _codex_subprocess_run
# --- end subprocess.run guard ---------------------------------------------

_codex_safe_import("codex.types", "codex_types")
_codex_safe_from("codex", "CodexConfig", "CodexSession")
_codex_safe_from("codex.memory", "MemoryStageOneRecord")
_codex_safe_from("codex.memory", "MemoryStateStore")
_codex_safe_from("codex.memory", "MemoryThreadRecord")
_codex_safe_from("codex.memory", "MemoryWorkspaceChange")
_codex_safe_from("codex.memory", "load_memory_rollout")
_codex_safe_from("codex.memory", "memory_rollout_candidates")
_codex_safe_from("codex.memory", "memory_rollout_is_stage1_startup_eligible")
_codex_safe_from("codex.memory", "memory_workspace_diff")
_codex_safe_from("codex.memory", "memory_extensions_root")
_codex_safe_from("codex.memory", "memory_rate_limit_allows_startup")
_codex_safe_from("codex.memory", "prepare_memory_workspace")
_codex_safe_from("codex.memory", "prune_old_extension_resources")
_codex_safe_from("codex.memory", "prune_stage1_records_for_retention")
_codex_safe_from("codex.memory", "reset_memory_workspace_baseline")
_codex_safe_from("codex.memory", "run_memory_stage_one_for_rollout")
_codex_safe_from("codex.memory", "run_memory_startup_once")
_codex_safe_from("codex.memory", "sanitize_response_item_for_memories")
_codex_safe_from("codex.memory", "seed_extension_instructions")
_codex_safe_from("codex.memory", "serialize_filtered_rollout_response_items")
_codex_safe_from("codex.memory", "select_phase2_memory_inputs")
_codex_safe_from("codex.memory", "build_memory_consolidation_config")
_codex_safe_from("codex.memory", "rebuild_raw_memories_file_from_memories")
_codex_safe_from("codex.memory", "render_memory_workspace_diff_file")
_codex_safe_from("codex.memory", "rollout_summaries_dir")
_codex_safe_from("codex.memory", "rollout_summary_file_stem")
_codex_safe_from("codex.memory", "extract_memory_stage_one")
_codex_safe_from("codex.memory", "memory_stage_one_output_schema")
_codex_safe_from("codex.memory", "parse_memory_stage_one_output")
_codex_safe_from("codex.memory", "raw_memories_file")
_codex_safe_from("codex.memory", "run_memory_consolidation_session")
_codex_safe_from("codex.memory", "run_memory_phase2_once")
_codex_safe_from("codex.memory", "run_memory_startup_pipeline_once")
_codex_safe_from("codex.memory", "start_memory_startup_task")
_codex_safe_from("codex.memory", "sync_rollout_summaries_from_memories")
_codex_safe_from("codex.memory", "sync_phase2_workspace_inputs")
_codex_safe_from("codex.memory", "write_current_memory_workspace_diff")
_codex_safe_from("codex.memory", "write_memory_workspace_diff")
_codex_safe_from("codex.model", "ScriptedResponsesModel")
_codex_safe_from("codex.model", "ModelStreamEvent")
_codex_safe_from("codex.model", "OpenAIResponsesModel")
_codex_safe_from("codex.model", "default_model_client")
_codex_safe_from("codex.model", "collect_stream_response")
_codex_safe_from("codex.model", "load_env_file")
_codex_safe_from("codex.prompts", "build_base_instructions", "build_environment_context", "build_initial_context_items")
_codex_safe_from("codex.prompts", "build_permissions_instructions", "collect_agents_md")
_codex_safe_from("codex.prompts", "verify_asset_hashes", "read_model_catalog_instructions")
_codex_safe_from("codex.prompts", "ASSETS_DIR as _CODEX_ASSETS_DIR")


# Inlined from codex.parity so codex.parity isn't part of the eval
# surface (it's an audit/internal module — see codex-impl/SPEC.md). The
# audit-side coverage of compare_assets_to_upstream lives in
# tests/test_codex_parity_audit.py.
_EVAL_UPSTREAM_ASSET_PATHS = {
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


def _eval_compare_assets_to_upstream() -> dict[str, bool]:
    upstream_env = os.environ.get("CODEX_UPSTREAM_DIR")
    upstream_dir = Path(upstream_env).expanduser() if upstream_env else (
        Path(_CODEX_ASSETS_DIR).parent / "upstream" / "openai-codex"
    )
    result: dict[str, bool] = {}
    for asset_path, upstream_path in _EVAL_UPSTREAM_ASSET_PATHS.items():
        upstream = upstream_dir / upstream_path
        result[asset_path] = (
            upstream.exists()
            and (Path(_CODEX_ASSETS_DIR) / asset_path).read_bytes() == upstream.read_bytes()
        )
    return result
# --- safe import shim (do not remove) -------------------------------------
# Every `from codex.* import X` below is wrapped so that if a candidate
# implementation is missing symbol X, the test module still loads. Only the
# tests that actually touch X fail (with a clear ImportError) instead of the
# whole suite collapsing at import time.
import importlib as _importlib

def _codex_unavail(qualname, exc):
    msg = "{} is not implemented in this codex package: {}: {}".format(
        qualname, type(exc).__name__, exc
    )
    class _Unavailable:
        def __call__(self, *a, **k): raise ImportError(msg)
        def __getattr__(self, n): raise ImportError(msg)
        def __iter__(self): raise ImportError(msg)
        def __bool__(self): return False
        def __repr__(self): return "<unavailable {}>".format(qualname)
    _Unavailable.__name__ = qualname.rsplit(".", 1)[-1]
    return _Unavailable()

def _codex_safe_from(modname, *names_with_aliases):
    """names_with_aliases items are either 'Name' or 'Name as Alias'."""
    pairs = []
    for spec in names_with_aliases:
        spec = spec.strip()
        if " as " in spec:
            name, alias = [p.strip() for p in spec.split(" as ")]
        else:
            name = alias = spec
        pairs.append((name, alias))
    try:
        mod = _importlib.import_module(modname)
    except Exception as _e:
        for name, alias in pairs:
            globals()[alias] = _codex_unavail(modname + "." + name, _e)
        return
    for name, alias in pairs:
        try:
            globals()[alias] = getattr(mod, name)
        except AttributeError as _e:
            globals()[alias] = _codex_unavail(modname + "." + name, _e)

def _codex_safe_import(modname, alias):
    try:
        globals()[alias] = _importlib.import_module(modname)
    except Exception as _e:
        globals()[alias] = _codex_unavail(modname, _e)
# --- end safe import shim -------------------------------------------------

_codex_safe_from("codex.prompts", "build_memory_consolidation_prompt")
_codex_safe_from("codex.prompts", "build_memory_stage_one_input_message")
_codex_safe_from("codex.prompts", "memory_stage_one_rollout_token_limit")
_codex_safe_from("codex.prompts", "memory_stage_one_system_prompt")
_codex_safe_from("codex.state", "build_compacted_history")
_codex_safe_from("codex.state", "build_compaction_summary_text")
_codex_safe_from("codex.state", "parse_command_actions")
_codex_safe_from("codex.state", "prepare_prompt_history")
_codex_safe_from("codex.state", "reconstruct_history_from_rollout")
_codex_safe_from("codex.state", "parse_memory_citation")
_codex_safe_from("codex.state", "extract_proposed_plan_text")
_codex_safe_from("codex.state", "strip_memory_citations")
_codex_safe_from("codex.state", "strip_proposed_plan_blocks")
_codex_safe_from("codex.state", "summarization_prompt")
_codex_safe_from("codex.state", "CODEX_ROLLOUT_ITEM_TYPES")
_codex_safe_import("codex.tools", "codex_tools")
_codex_safe_from("codex.tools", "ToolRuntime")
_codex_safe_from("codex.tools", "ToolResult")
_codex_safe_from("codex.types", "KNOWN_EVENT_TYPES")
_codex_safe_from("codex.types", "PromptRequest")
_codex_safe_from("codex.types", "TERMINAL_TURN_EVENT_TYPES")


def message(text: str) -> dict:
    return {
        "output": [
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }
        ]
    }


class MultiAgentRoutingModel:
    def __init__(self):
        self.requests: list[PromptRequest] = []

    def create(self, request: PromptRequest):
        self.requests.append(request)
        texts = _request_texts(request)
        if any("child task" in text for text in texts):
            return _model_response(message("child done"))
        outputs = [item for item in request.input if item.get("type") == "function_call_output"]
        if outputs and outputs[-1].get("call_id") == "spawn-1":
            agent_id = json.loads(outputs[-1]["output"])["agent_id"]
            return _model_response(
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "wait_agent",
                            "call_id": "wait-1",
                            "arguments": json.dumps({"targets": [agent_id], "timeout_ms": 5000}),
                        }
                    ]
                }
            )
        if outputs and outputs[-1].get("call_id") == "wait-1":
            return _model_response(message("parent saw child"))
        return _model_response(
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "spawn_agent",
                        "call_id": "spawn-1",
                        "arguments": json.dumps({"message": "child task", "agent_type": "worker"}),
                    }
                ]
            }
        )


class RemoteCompactModel:
    def __init__(self, *, compact_output: list[dict], local_responses: list[dict] | None = None, fail_remote: bool = False):
        self.compact_output = compact_output
        self.local = ScriptedResponsesModel(local_responses or [])
        self.fail_remote = fail_remote
        self.compact_requests: list[PromptRequest] = []

    @property
    def requests(self) -> list[PromptRequest]:
        return self.local.requests

    def compact(self, request: PromptRequest, **_: Any) -> list[dict]:
        self.compact_requests.append(request)
        if self.fail_remote:
            raise RuntimeError("compact endpoint unavailable")
        return deepcopy(self.compact_output)

    def stream(self, request: PromptRequest):
        yield from self.local.stream(request)

    def create(self, request: PromptRequest):
        return self.local.create(request)


def _model_response(payload: dict):
    from codex.types import ModelResponse

    return ModelResponse(id="routed", output=list(payload.get("output", [])), raw=dict(payload))


def _stream_message(text: str) -> ModelStreamEvent:
    return ModelStreamEvent(
        "item.completed",
        {
            "item": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }
        },
    )


def _request_texts(request: PromptRequest) -> list[str]:
    texts: list[str] = []
    for item in request.input:
        for part in item.get("content", []):
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                texts.append(part["text"])
    return texts


def _plain_terminal_output(text: str) -> str:
    import re

    text = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", text)
    text = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", text)
    text = text.replace("\r", "\n")
    return text


class CodexCoreTests(unittest.TestCase):
    def test_prompt_assets_match_manifest(self) -> None:
        self.assertTrue(all(verify_asset_hashes().values()))
        self.assertTrue(all(_eval_compare_assets_to_upstream().values()))

    def test_core_tool_schema_names(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        expected = (
            [
                "shell_command",
                "update_plan",
                "request_user_input",
                "apply_patch",
                "view_image",
                "spawn_agent",
                "send_input",
                "resume_agent",
                "wait_agent",
                "close_agent",
                "web_search",
            ]
            if sys.platform == "win32"
            else [
                "exec_command",
                "write_stdin",
                "update_plan",
                "request_user_input",
                "apply_patch",
                "view_image",
                "spawn_agent",
                "send_input",
                "resume_agent",
                "wait_agent",
                "close_agent",
                "web_search",
            ]
        )
        self.assertEqual([spec.get("name", spec["type"]) for spec in runtime.specs()], expected)

    def test_turn_loop_has_no_default_iteration_cap_but_can_be_capped_for_debug(self) -> None:
        self.assertIsNone(CodexConfig(skip_git_repo_check=True, ephemeral=True).max_iterations)

        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "update_plan",
                            "call_id": "plan-1",
                            "arguments": json.dumps(
                                {"plan": [{"step": "keep looping", "status": "in_progress"}]}
                            ),
                        }
                    ]
                }
            ]
        )
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, max_iterations=1),
            model_client=model,
        )

        result = session.run("exercise the optional iteration cap")

        self.assertEqual(len(model.requests), 1)
        self.assertIn(
            "max_iterations exceeded",
            [str(event.payload.get("error")) for event in result.events if event.type == "turn.failed"],
        )

    def test_request_user_input_schema_and_default_unavailable_handler(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        spec = next(spec for spec in runtime.specs() if spec.get("name") == "request_user_input")
        self.assertIn("Plan mode", spec["description"])
        self.assertEqual(spec["parameters"]["required"], ["questions"])

        result = runtime.request_user_input(
            {
                "questions": [
                    {
                        "id": "choice",
                        "header": "Choice",
                        "question": "Pick one.",
                        "options": [{"label": "A (Recommended)", "description": "Choose A."}],
                    }
                ]
            }
        )
        self.assertFalse(result.ok)
        self.assertIn("unavailable in Default mode", result.output)

    def test_request_user_input_provider_returns_upstream_response_shape(self) -> None:
        def provider(questions: list[dict]) -> dict:
            self.assertTrue(questions[0]["isOther"])
            self.assertFalse(questions[0]["isSecret"])
            return {"answers": {"choice": {"answers": ["A"]}}}

        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                collaboration_mode="Plan",
                request_user_input_provider=provider,
            )
        )
        arguments = {
            "questions": [
                {
                    "id": "choice",
                    "header": "Choice",
                    "question": "Pick one.",
                    "options": [{"label": "A (Recommended)", "description": "Choose A."}],
                }
            ]
        }
        result = runtime.request_user_input(arguments)

        self.assertTrue(result.ok)
        self.assertEqual(json.loads(result.output), {"answers": {"choice": {"answers": ["A"]}}})
        self.assertNotIn("isOther", arguments["questions"][0])

    def test_request_user_input_is_root_thread_only(self) -> None:
        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                collaboration_mode="Plan",
                agent_depth=1,
                request_user_input_answers={"choice": {"answers": ["A"]}},
            )
        )
        result = runtime.request_user_input(
            {
                "questions": [
                    {
                        "id": "choice",
                        "header": "Choice",
                        "question": "Pick one.",
                        "options": [{"label": "A (Recommended)", "description": "Choose A."}],
                    }
                ]
            }
        )
        self.assertFalse(result.ok)
        self.assertIn("root thread", result.output)

    def test_update_plan_output_and_plan_mode_rejection_match_upstream_handler(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        result = runtime.update_plan({"plan": [{"step": "Inspect", "status": "in_progress"}]})
        self.assertTrue(result.ok)
        self.assertEqual(result.output, "Plan updated")

        plan_mode = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True, collaboration_mode="Plan"))
        rejected = plan_mode.update_plan({"plan": [{"step": "Inspect", "status": "in_progress"}]})
        self.assertFalse(rejected.ok)
        self.assertIn("not allowed in Plan mode", rejected.output)

    def test_web_search_tool_uses_upstream_hosted_shape(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        web_search = [spec for spec in runtime.specs() if spec.get("type") == "web_search"]
        self.assertEqual(web_search, [{"type": "web_search", "external_web_access": False}])

    def test_web_search_tool_serializes_upstream_config_shape(self) -> None:
        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                web_search_external_web_access=True,
                web_search_filters={"allowed_domains": ["example.com"]},
                web_search_user_location={
                    "type": "approximate",
                    "country": "US",
                    "region": "California",
                    "city": "San Francisco",
                    "timezone": "America/Los_Angeles",
                },
                web_search_context_size="high",
                web_search_content_types=("text", "image"),
            )
        )
        web_search = [spec for spec in runtime.specs() if spec.get("type") == "web_search"]
        self.assertEqual(
            web_search,
            [
                {
                    "type": "web_search",
                    "external_web_access": True,
                    "filters": {"allowed_domains": ["example.com"]},
                    "user_location": {
                        "type": "approximate",
                        "country": "US",
                        "region": "California",
                        "city": "San Francisco",
                        "timezone": "America/Los_Angeles",
                    },
                    "search_context_size": "high",
                    "search_content_types": ["text", "image"],
                }
            ],
        )

    def test_multi_agent_specs_are_visible_but_runtime_is_unavailable(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        names = {spec.get("name", spec["type"]) for spec in runtime.specs()}
        self.assertTrue({"spawn_agent", "send_input", "resume_agent", "wait_agent", "close_agent"} <= names)
        result = runtime.dispatch("spawn_agent", {"message": "inspect this"})
        self.assertFalse(result.ok)
        self.assertIn("multi-agent runtime is not implemented", result.output)

    def test_codex_session_runs_local_multi_agent_spawn_and_wait(self) -> None:
        model = MultiAgentRoutingModel()
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("delegate")

        self.assertEqual(result.final_message, "parent saw child")
        outputs = [item for item in result.history if item.get("type") == "function_call_output"]
        spawn_output = json.loads(next(item["output"] for item in outputs if item["call_id"] == "spawn-1"))
        wait_output = json.loads(next(item["output"] for item in outputs if item["call_id"] == "wait-1"))
        self.assertEqual(spawn_output["nickname"], "worker")
        self.assertEqual(wait_output["status"][spawn_output["agent_id"]], {"completed": "child done"})
        self.assertFalse(wait_output["timed_out"])
        notification_texts = [
            part["text"]
            for item in result.history
            if item.get("type") == "message" and item.get("role") == "user"
            for part in item.get("content", [])
            if isinstance(part, dict) and isinstance(part.get("text"), str)
        ]
        self.assertTrue(any("<subagent_notification>" in text for text in notification_texts))
        self.assertTrue(any('"status":{"completed":"child done"}' in text for text in notification_texts))

    def test_local_multi_agent_send_resume_and_close(self) -> None:
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=ScriptedResponsesModel([message("first child"), message("second child")]),
        )

        spawned = session.tools.spawn_agent({"message": "first"})
        agent_id = json.loads(spawned.output)["agent_id"]
        first_wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 5000}).output)
        self.assertEqual(first_wait["status"][agent_id], {"completed": "first child"})

        sent = session.tools.send_input({"target": agent_id, "message": "second"})
        self.assertTrue(sent.ok)
        second_wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 5000}).output)
        self.assertEqual(second_wait["status"][agent_id], {"completed": "second child"})

        closed = json.loads(session.tools.close_agent({"target": agent_id}).output)
        self.assertEqual(closed["previous_status"], {"completed": "second child"})
        resumed = json.loads(session.tools.resume_agent({"id": agent_id}).output)
        self.assertEqual(resumed["status"], {"completed": "second child"})

    def test_local_multi_agent_interrupts_running_child_for_send_input(self) -> None:
        sleep_command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(30)'"
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "sleep-1",
                                "arguments": json.dumps({"cmd": sleep_command, "yield_time_ms": 30000}),
                            }
                        ]
                    },
                    message("second child"),
                ]
            ),
        )

        spawned = session.tools.spawn_agent({"message": "first"})
        agent_id = json.loads(spawned.output)["agent_id"]
        time.sleep(0.2)
        sent = session.tools.send_input({"target": agent_id, "message": "second", "interrupt": True})
        self.assertTrue(sent.ok)
        second_wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 10000}).output)
        self.assertEqual(second_wait["status"][agent_id], {"completed": "second child"})

    def test_local_multi_agent_close_interrupts_running_child(self) -> None:
        sleep_command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(30)'"
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "sleep-1",
                                "arguments": json.dumps({"cmd": sleep_command, "yield_time_ms": 30000}),
                            }
                        ]
                    }
                ]
            ),
        )

        spawned = session.tools.spawn_agent({"message": "first"})
        agent_id = json.loads(spawned.output)["agent_id"]
        time.sleep(0.2)
        closed = json.loads(session.tools.close_agent({"target": agent_id}).output)
        self.assertEqual(closed["previous_status"], "running")
        wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 10000}).output)
        self.assertEqual(wait["status"][agent_id], "shutdown")

    def test_local_multi_agent_wait_timeout_returns_empty_statuses(self) -> None:
        import codex.core as core

        old_min = core._MIN_AGENT_WAIT_TIMEOUT_MS
        old_max = core._MAX_AGENT_WAIT_TIMEOUT_MS
        core._MIN_AGENT_WAIT_TIMEOUT_MS = 1
        core._MAX_AGENT_WAIT_TIMEOUT_MS = 10
        try:
            sleep_command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(30)'"
            session = CodexSession(
                CodexConfig(skip_git_repo_check=True, ephemeral=True),
                model_client=ScriptedResponsesModel(
                    [
                        {
                            "output": [
                                {
                                    "type": "function_call",
                                    "name": "exec_command",
                                    "call_id": "sleep-1",
                                    "arguments": json.dumps({"cmd": sleep_command, "yield_time_ms": 30000}),
                                }
                            ]
                        }
                    ]
                ),
            )
            spawned = session.tools.spawn_agent({"message": "first"})
            agent_id = json.loads(spawned.output)["agent_id"]
            time.sleep(0.2)

            wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 1}).output)

            self.assertEqual(wait, {"status": {}, "timed_out": True})
            session.tools.close_agent({"target": agent_id})
        finally:
            core._MIN_AGENT_WAIT_TIMEOUT_MS = old_min
            core._MAX_AGENT_WAIT_TIMEOUT_MS = old_max

    def test_spawn_agent_fork_context_sanitizes_runtime_items(self) -> None:
        model = ScriptedResponsesModel([message("child")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.state.history = [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "parent user"}]},
            {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "call-1",
                "arguments": json.dumps({"cmd": "pwd"}),
            },
            {"type": "function_call_output", "call_id": "call-1", "output": "parent tool output"},
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "parent final"}]},
        ]

        spawned = session.tools.spawn_agent({"message": "child", "fork_context": True})
        agent_id = json.loads(spawned.output)["agent_id"]
        wait = json.loads(session.tools.wait_agent({"targets": [agent_id], "timeout_ms": 10000}).output)
        self.assertEqual(wait["status"][agent_id], {"completed": "child"})

        child_input_types = [item.get("type") for item in model.requests[0].input]
        self.assertIn("message", child_input_types)
        self.assertNotIn("function_call", child_input_types)
        self.assertNotIn("function_call_output", child_input_types)

    def test_load_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "openai.env"
            path.write_text("# comment\nOPENAI_API_KEY='sk-test'\nOTHER=value\n", encoding="utf-8")
            self.assertEqual(load_env_file(path)["OPENAI_API_KEY"], "sk-test")

    def test_prompt_request_includes_stable_responses_request_fields(self) -> None:
        request = PromptRequest(
            model="gpt-test",
            instructions="base",
            input=[],
            tools=[{"type": "web_search", "external_web_access": False}],
            prompt_cache_key="thread-1",
            reasoning={"effort": "medium", "summary": "auto"},
            include=["reasoning.encrypted_content"],
            service_tier="flex",
            client_metadata={"x-codex-installation-id": "install-1"},
        )
        kwargs = request.to_responses_kwargs()
        self.assertEqual(kwargs["tools"], [{"type": "web_search", "external_web_access": False}])
        self.assertEqual(kwargs["tool_choice"], "auto")
        self.assertFalse(kwargs["store"])
        self.assertTrue(kwargs["stream"])
        self.assertTrue(kwargs["parallel_tool_calls"])
        self.assertEqual(kwargs["prompt_cache_key"], "thread-1")
        self.assertEqual(kwargs["reasoning"], {"effort": "medium", "summary": "auto"})
        self.assertEqual(kwargs["include"], ["reasoning.encrypted_content"])
        self.assertEqual(kwargs["service_tier"], "flex")
        self.assertEqual(kwargs["client_metadata"], {"x-codex-installation-id": "install-1"})

    def test_prompt_request_compact_payload_matches_upstream_endpoint_shape(self) -> None:
        request = PromptRequest(
            model="gpt-test",
            instructions="base",
            input=[
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hello"}],
                }
            ],
            tools=[{"type": "web_search", "external_web_access": False}],
            parallel_tool_calls=True,
            prompt_cache_key="thread-1",
            reasoning={"effort": "medium", "summary": "auto"},
            include=["reasoning.encrypted_content"],
            service_tier="flex",
            verbosity="low",
        )

        payload = request.to_compact_payload()

        self.assertEqual(
            set(payload),
            {
                "model",
                "input",
                "instructions",
                "tools",
                "parallel_tool_calls",
                "reasoning",
                "service_tier",
                "prompt_cache_key",
                "text",
            },
        )
        self.assertNotIn("stream", payload)
        self.assertNotIn("store", payload)
        self.assertNotIn("include", payload)
        self.assertNotIn("tool_choice", payload)
        self.assertEqual(payload["text"], {"verbosity": "low"})

    def test_openai_responses_model_exposes_remote_compact_endpoint(self) -> None:
        model = OpenAIResponsesModel(api_key="sk-test", base_url="https://example.test/v1")
        self.assertTrue(callable(model.compact))
        self.assertEqual(model._compact_url(), "https://example.test/v1/responses/compact")
        headers = model._compact_headers(
            session_id="session-1",
            thread_id="thread-1",
            installation_id="install-1",
        )
        self.assertEqual(headers["Authorization"], "Bearer sk-test")
        self.assertEqual(headers["x-codex-installation-id"], "install-1")
        self.assertEqual(headers["session-id"], "session-1")
        self.assertEqual(headers["thread-id"], "thread-1")

    def test_prompt_request_matches_upstream_empty_tools_shape(self) -> None:
        request = PromptRequest(
            model="gpt-test",
            instructions="base",
            input=[],
            tools=[],
            parallel_tool_calls=False,
        )
        kwargs = request.to_responses_kwargs()
        self.assertEqual(kwargs["tools"], [])
        self.assertEqual(kwargs["tool_choice"], "auto")
        self.assertFalse(kwargs["parallel_tool_calls"])
        self.assertIsNone(kwargs["reasoning"])
        self.assertEqual(kwargs["include"], [])

    def test_prompt_request_sanitizes_response_output_items_for_followup(self) -> None:
        request = PromptRequest(
            model="gpt-test",
            instructions="base",
            input=[
                {
                    "type": "function_call",
                    "id": "fc-1",
                    "status": "completed",
                    "name": "exec_command",
                    "call_id": "call-1",
                    "arguments": json.dumps({"cmd": "pwd"}),
                },
                {
                    "type": "message",
                    "id": "msg-1",
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "ok",
                            "annotations": [],
                            "logprobs": [],
                        }
                    ],
                },
                {
                    "type": "web_search_call",
                    "id": "ws-1",
                    "status": "completed",
                    "action": {"type": "search", "query": "Codex"},
                },
            ],
            tools=[],
        )

        kwargs = request.to_responses_kwargs()
        self.assertEqual(
            kwargs["input"][0],
            {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "call-1",
                "arguments": json.dumps({"cmd": "pwd"}),
            },
        )
        self.assertEqual(
            kwargs["input"][1],
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "ok"}],
            },
        )
        self.assertEqual(
            kwargs["input"][2],
            {
                "type": "web_search_call",
                "status": "completed",
                "action": {"type": "search", "query": "Codex"},
            },
        )

    def test_prompt_request_includes_output_schema_text_controls(self) -> None:
        request = PromptRequest(
            model="gpt-test",
            instructions="base",
            input=[],
            tools=[],
            output_schema={"type": "object"},
            output_schema_strict=True,
        )
        text = request.to_responses_kwargs()["text"]
        self.assertEqual(text["format"]["type"], "json_schema")
        self.assertTrue(text["format"]["strict"])
        self.assertEqual(text["format"]["name"], "codex_output_schema")
        self.assertEqual(text["format"]["schema"], {"type": "object"})

    def test_stream_response_events_are_collected_to_model_response(self) -> None:
        response = collect_stream_response(
            [
                {
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "partial"}],
                    },
                },
                {
                    "type": "response.completed",
                    "response": {
                        "id": "resp-1",
                        "output": [
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "final"}],
                            }
                        ],
                    },
                },
            ]
        )
        self.assertEqual(response.id, "resp-1")
        self.assertEqual(response.output[0]["content"][0]["text"], "final")

    def test_incomplete_stream_event_records_usage_before_failure(self) -> None:
        from codex.model import iter_model_stream_events

        events = list(
            iter_model_stream_events(
                [
                    {
                        "type": "response.incomplete",
                        "response": {
                            "id": "resp-1",
                            "usage": {
                                "input_tokens": 10,
                                "output_tokens": 260,
                                "output_tokens_details": {"reasoning_tokens": 256},
                                "total_tokens": 270,
                            },
                        },
                    }
                ]
            )
        )

        self.assertEqual([event.type for event in events], ["token_count", "model.failed"])
        self.assertEqual(events[0].payload["usage"]["output_tokens_details"]["reasoning_tokens"], 256)
        self.assertEqual(events[1].payload["response_id"], "resp-1")

    def test_core_emits_model_stream_lifecycle_events(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "streamed"}],
                        }
                    ],
                    "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3},
                }
            ]
        )
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )

        result = session.run("hello")

        self.assertEqual(result.final_message, "streamed")
        event_types = [event.type for event in result.events]
        started_index = event_types.index("item.started")
        delta_index = event_types.index("item.delta")
        completed_index = next(
            index for index, event_type in enumerate(event_types) if event_type == "item.completed" and index > delta_index
        )
        token_count_index = event_types.index("token_count")
        model_response_index = event_types.index("model.response")
        self.assertLess(started_index, delta_index)
        self.assertLess(delta_index, completed_index)
        self.assertLess(completed_index, token_count_index)
        self.assertLess(token_count_index, model_response_index)
        token_count = result.events[token_count_index]
        self.assertEqual(token_count.payload["usage"]["total_tokens"], 3)
        self.assertEqual(token_count.payload["info"]["total_token_usage"], 3)

    def test_token_usage_total_accumulates_api_usage_while_last_tracks_context(self) -> None:
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=ScriptedResponsesModel([]))

        session.state.record_token_usage(
            {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15, "reasoning_output_tokens": 3}
        )
        session.state.record_token_usage(
            {
                "input_tokens": 4,
                "output_tokens": 1,
                "total_tokens": 5,
                "output_tokens_details": {"reasoning_tokens": 2},
            }
        )

        self.assertEqual(session.state.active_context_tokens(), 5)
        self.assertEqual(session.state.session_usage_tokens(), 20)
        self.assertEqual(session.state.session_reasoning_usage_tokens(), 5)
        self.assertEqual(session.state.token_usage_info()["total_token_usage"], 20)
        self.assertEqual(session.state.token_usage_info()["session_reasoning_tokens"], 5)

    def test_active_context_estimate_uses_prompt_visible_tool_output_truncation(self) -> None:
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=ScriptedResponsesModel([]))
        token_limit = session.config.resolved_tool_output_truncation_tokens()
        long_output = "x" * (token_limit * 40)
        session.state.record_token_usage({"input_tokens": 80, "output_tokens": 20, "total_tokens": 100})
        session.state.append_history(
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "ready"}]}
        )
        session.state.append_history({"type": "function_call_output", "call_id": "call-1", "output": long_output})

        estimated_tokens, is_estimated = session.state.active_context_token_status()

        self.assertTrue(is_estimated)
        self.assertLess(estimated_tokens, token_limit * 2 + 1_000)
        self.assertLess(estimated_tokens, (len(long_output) + 3) // 8)
        self.assertEqual(session.state.history[-1]["output"], long_output)

    def test_recomputed_compaction_context_does_not_increment_api_session_usage(self) -> None:
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=ScriptedResponsesModel([]))
        session.state.record_token_usage({"input_tokens": 10, "output_tokens": 5, "total_tokens": 15})
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "after compact"}]}
        )

        session.state.recompute_token_usage_from_history()

        self.assertEqual(session.state.session_usage_tokens(), 15)
        self.assertTrue(session.state.last_token_usage["estimated"])

    def test_session_context_carries_pre_compaction_context_into_new_epoch(self) -> None:
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=ScriptedResponsesModel([]))
        session.state.record_token_usage({"input_tokens": 80, "output_tokens": 20, "total_tokens": 100})

        self.assertEqual(session.state.session_context_token_status(), (100, False))

        pre_compact_session, estimated = session.state.session_context_token_status()
        session.state.start_new_context_epoch(pre_compact_session, estimated=estimated)
        session.state.record_token_usage({"input_tokens": 12, "output_tokens": 8, "total_tokens": 20})

        self.assertEqual(session.state.active_context_token_status(), (20, False))
        self.assertEqual(session.state.session_context_token_status(), (120, False))

    def test_retryable_model_stream_error_retries_sampling_request(self) -> None:
        class FlakyStreamModel:
            def __init__(self) -> None:
                self.requests: list[PromptRequest] = []

            def create(self, request: PromptRequest):
                raise AssertionError("stream expected")

            def stream(self, request: PromptRequest):
                self.requests.append(request)
                if len(self.requests) == 1:
                    raise RuntimeError("stream closed before response.completed")
                yield _stream_message("recovered")
                yield ModelStreamEvent("model.response", {"response_id": "retry-ok"})

        model = FlakyStreamModel()
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_stream_max_retries=1,
                model_stream_retry_base_delay_ms=0,
            ),
            model_client=model,
        )

        result = session.run("hello")

        self.assertEqual(result.final_message, "recovered")
        self.assertEqual(len(model.requests), 2)
        self.assertEqual([event.type for event in result.events].count("stream_error"), 1)
        self.assertNotIn("turn.failed", [event.type for event in result.events])
        self.assertEqual([item.get("role") for item in result.history if item.get("role") == "assistant"], ["assistant"])

    def test_steer_input_is_drained_before_follow_up_request(self) -> None:
        class SteeringModel:
            def __init__(self) -> None:
                self.requests: list[PromptRequest] = []
                self.session: CodexSession | None = None

            def create(self, request: PromptRequest):
                raise AssertionError("stream expected")

            def stream(self, request: PromptRequest):
                self.requests.append(request)
                if len(self.requests) == 1:
                    assert self.session is not None
                    turn_id = self.session.steer_input("second steer", expected_turn_id=self.session.state.turn_id)
                    self.assertEqualTurn(turn_id)
                    yield _stream_message("first answer")
                else:
                    yield _stream_message("second answer")

            def assertEqualTurn(self, turn_id: str) -> None:
                assert self.session is not None
                if turn_id != self.session.state.turn_id:
                    raise AssertionError("steer returned the wrong active turn id")

        model = SteeringModel()
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=model)
        model.session = session

        result = session.run("first prompt")

        self.assertEqual(result.final_message, "second answer")
        self.assertEqual(len(model.requests), 2)
        self.assertIn("first prompt", _request_texts(model.requests[0]))
        self.assertNotIn("second steer", _request_texts(model.requests[0]))
        self.assertIn("second steer", _request_texts(model.requests[1]))
        pending_events = [
            event
            for event in result.events
            if event.type == "item.completed" and event.payload.get("pending_input")
        ]
        self.assertEqual(len(pending_events), 1)

    def test_steer_input_requires_active_regular_turn(self) -> None:
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=ScriptedResponsesModel([]))

        with self.assertRaisesRegex(Exception, "no active turn"):
            session.steer_input("too early")

    def test_interrupt_emits_turn_aborted_and_records_model_visible_marker(self) -> None:
        class InterruptingModel:
            def __init__(self) -> None:
                self.session: CodexSession | None = None

            def create(self, request: PromptRequest):
                raise AssertionError("stream expected")

            def stream(self, request: PromptRequest):
                assert self.session is not None
                self.session.interrupt()
                yield ModelStreamEvent("model.response", {"response_id": "interrupted"})

        model = InterruptingModel()
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=model)
        model.session = session

        events = list(session.stream("stop this"))

        self.assertIn("turn.aborted", [event.type for event in events])
        self.assertNotIn("turn.failed", [event.type for event in events])
        self.assertTrue(any("<turn_aborted>" in text for text in _request_texts(PromptRequest("m", "", session.state.history, []))))
        self.assertTrue(any(item.get("role") == "user" for item in session.state.history))

    def test_hook_provider_injects_user_prompt_context_before_model_request(self) -> None:
        class InspectingModel:
            def __init__(self) -> None:
                self.requests: list[PromptRequest] = []

            def create(self, request: PromptRequest):
                raise AssertionError("stream expected")

            def stream(self, request: PromptRequest):
                self.requests.append(request)
                yield _stream_message("hooked")

        calls: list[dict] = []

        def hook_provider(request: dict) -> dict:
            calls.append(request)
            if request["event"] == "user_prompt_submit":
                return {"additional_contexts": ["extra policy context"]}
            return {}

        model = InspectingModel()
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=model,
        )

        result = session.run("hello")
        texts = _request_texts(model.requests[0])

        self.assertEqual(result.final_message, "hooked")
        self.assertIn("session_start", [call["event"] for call in calls])
        self.assertIn("user_prompt_submit", [call["event"] for call in calls])
        self.assertTrue(any("<hook_context>" in text and "extra policy context" in text for text in texts))
        self.assertIn("hook.started", [event.type for event in result.events])
        self.assertIn("hook.completed", [event.type for event in result.events])

    def test_pre_tool_use_hook_blocks_tool_and_returns_output_to_model(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf should-not-run"}),
                    }
                ]
            },
            message("saw blocked tool"),
        ]

        def hook_provider(request: dict) -> dict:
            if request["event"] == "pre_tool_use":
                return {"should_block": True, "block_reason": "blocked by test hook"}
            return {}

        model = ScriptedResponsesModel(responses)
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=model,
        )

        result = session.run("run command")
        tool_outputs = [item for item in result.history if item.get("type") == "function_call_output"]

        self.assertEqual(result.final_message, "saw blocked tool")
        self.assertEqual(len(tool_outputs), 1)
        self.assertIn("blocked by test hook", tool_outputs[0]["output"])
        self.assertTrue(any(event.type == "hook.completed" for event in result.events))

    def test_pre_tool_use_hook_uses_bash_contract_and_can_rewrite_exec_command(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf original"}),
                    }
                ]
            },
            message("done"),
        ]
        hook_calls: list[dict] = []

        def hook_provider(request: dict) -> dict:
            hook_calls.append(request)
            if request["event"] == "pre_tool_use":
                return {"updated_input": {"command": "printf rewritten"}}
            return {}

        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=ScriptedResponsesModel(responses),
        )

        result = session.run("run command")
        pre = next(call for call in hook_calls if call["event"] == "pre_tool_use")
        post = next(call for call in hook_calls if call["event"] == "post_tool_use")
        tool_output = next(item for item in result.history if item.get("type") == "function_call_output")

        self.assertEqual(pre["tool_name"], "Bash")
        self.assertEqual(pre["tool_input"], {"command": "printf original"})
        self.assertEqual(post["tool_name"], "Bash")
        self.assertEqual(post["tool_input"], {"command": "printf rewritten"})
        self.assertEqual(post["tool_response"], "rewritten")
        self.assertIn("rewritten", tool_output["output"])
        self.assertNotIn("original", tool_output["output"])

    def test_apply_patch_hook_uses_command_contract_aliases_and_can_rewrite_patch(self) -> None:
        original_patch = "*** Begin Patch\n*** Add File: a.txt\n+old\n*** End Patch"
        rewritten_patch = "*** Begin Patch\n*** Add File: b.txt\n+new\n*** End Patch"
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "apply_patch",
                        "call_id": "patch-1",
                        "arguments": original_patch,
                    }
                ]
            },
            message("patched"),
        ]
        hook_calls: list[dict] = []

        def hook_provider(request: dict) -> dict:
            hook_calls.append(request)
            if request["event"] == "pre_tool_use":
                return {"updated_input": {"command": rewritten_patch}}
            return {}

        with tempfile.TemporaryDirectory() as temp:
            session = CodexSession(
                CodexConfig(
                    cwd=Path(temp),
                    skip_git_repo_check=True,
                    ephemeral=True,
                    hook_provider=hook_provider,
                ),
                model_client=ScriptedResponsesModel(responses),
            )
            result = session.run("patch file")

            self.assertFalse((Path(temp) / "a.txt").exists())
            self.assertEqual((Path(temp) / "b.txt").read_text(encoding="utf-8"), "new\n")

        pre = next(call for call in hook_calls if call["event"] == "pre_tool_use")
        post = next(call for call in hook_calls if call["event"] == "post_tool_use")
        self.assertEqual(pre["tool_name"], "apply_patch")
        self.assertEqual(pre["matcher_aliases"], ["Write", "Edit"])
        self.assertEqual(pre["tool_input"], {"command": original_patch})
        self.assertEqual(post["tool_name"], "apply_patch")
        self.assertEqual(post["matcher_aliases"], ["Write", "Edit"])
        self.assertEqual(post["tool_input"], {"command": rewritten_patch})
        self.assertEqual(post["tool_response"], "Success. Updated the following files:\nA b.txt\n")
        self.assertEqual(result.final_message, "patched")

    def test_pre_post_tool_hooks_only_run_for_upstream_handlers_with_payloads(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "update_plan",
                        "call_id": "plan-1",
                        "arguments": json.dumps({"plan": [{"step": "Inspect", "status": "in_progress"}]}),
                    }
                ]
            },
            message("done"),
        ]
        hook_events: list[str] = []

        def hook_provider(request: dict) -> dict:
            if request["event"] in {"pre_tool_use", "post_tool_use"}:
                hook_events.append(request["event"])
            return {}

        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=ScriptedResponsesModel(responses),
        )

        result = session.run("plan")

        self.assertEqual(result.final_message, "done")
        self.assertEqual(hook_events, [])

    def test_permission_request_hook_events_are_emitted_during_tool_approval(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-approval",
                        "arguments": json.dumps(
                            {
                                "cmd": f"{sys.executable!r} -c \"print('approved by hook')\"",
                                "sandbox_permissions": "require_escalated",
                                "justification": "Need hook approval",
                            }
                        ),
                    }
                ]
            },
            message("finished"),
        ]
        hook_calls: list[dict] = []

        def hook_provider(request: dict) -> dict:
            hook_calls.append(request)
            if request["event"] == "permission_request":
                return {"decision": "approved_for_session"}
            return {}

        model = ScriptedResponsesModel(responses)
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                approval_policy="on-request",
                hook_provider=hook_provider,
            ),
            model_client=model,
        )

        result = session.run("run approval command")

        permission_hook_events = [
            event for event in result.events if event.type.startswith("hook.") and event.payload.get("name") == "permission_request"
        ]
        self.assertEqual([event.type for event in permission_hook_events], ["hook.started", "hook.completed"])
        self.assertTrue(any(call["event"] == "permission_request" for call in hook_calls))
        self.assertEqual(result.final_message, "finished")
        self.assertLess(
            result.events.index(permission_hook_events[-1]),
            next(index for index, event in enumerate(result.events) if event.type == "tool.completed"),
        )

    def test_core_aggregates_streamed_tool_argument_deltas(self) -> None:
        arguments = json.dumps({"plan": [{"step": "inspect", "status": "in_progress"}]})
        model = ScriptedResponsesModel(
            [
                {
                    "events": [
                        {
                            "type": "response.output_item.added",
                            "output_index": 0,
                            "item": {
                                "type": "function_call",
                                "name": "update_plan",
                                "call_id": "plan-1",
                                "arguments": "",
                            },
                        },
                        {
                            "type": "response.function_call_arguments.delta",
                            "item_id": "plan-1",
                            "output_index": 0,
                            "delta": arguments[:20],
                        },
                        {
                            "type": "response.function_call_arguments.delta",
                            "item_id": "plan-1",
                            "output_index": 0,
                            "delta": arguments[20:],
                        },
                        {
                            "type": "response.output_item.done",
                            "output_index": 0,
                            "item": {
                                "type": "function_call",
                                "name": "update_plan",
                                "call_id": "plan-1",
                                "arguments": arguments,
                            },
                        },
                        {
                            "type": "response.completed",
                            "response": {
                                "id": "resp-tool",
                                "output": [
                                    {
                                        "type": "function_call",
                                        "name": "update_plan",
                                        "call_id": "plan-1",
                                        "arguments": arguments,
                                    }
                                ],
                            },
                        },
                    ]
                },
                message("done after streamed tool args"),
            ]
        )
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )

        result = session.run("stream tool args")

        self.assertEqual(result.final_message, "done after streamed tool args")
        deltas = [event for event in result.events if event.type == "item.delta" and event.payload.get("item_id") == "plan-1"]
        self.assertEqual(deltas[-1].payload["aggregate"]["arguments"], arguments)
        completed = next(
            event
            for event in result.events
            if event.type == "item.completed" and event.payload.get("item", {}).get("call_id") == "plan-1"
        )
        self.assertEqual(completed.payload["aggregate"]["arguments"], arguments)

    def test_compaction_assets_and_replacement_history_helper(self) -> None:
        self.assertIn("CONTEXT CHECKPOINT COMPACTION", summarization_prompt())
        summary = build_compaction_summary_text("continue from here")
        self.assertTrue(summary.startswith("Another language model started"))
        history = build_compacted_history([], ["first user", "second user"], summary)
        self.assertEqual([item["role"] for item in history], ["user", "user", "user"])
        self.assertEqual(history[-1]["content"][0]["text"], summary)

    def test_manual_compaction_runs_summary_turn_and_replaces_history(self) -> None:
        model = ScriptedResponsesModel([message("handoff summary")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first task"}]}
        )
        session.state.append_history(
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "assistant work"}]}
        )
        session.state.append_history(
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": build_compaction_summary_text("old summary")}],
            }
        )
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second task"}]}
        )

        result = session.compact()

        self.assertEqual(result.final_message, "handoff summary")
        request = model.requests[0]
        self.assertEqual(request.tools, [])
        self.assertFalse(request.parallel_tool_calls)
        self.assertIn("CONTEXT CHECKPOINT COMPACTION", request.input[-1]["content"][0]["text"])
        compacted_texts = [item["content"][0]["text"] for item in result.history]
        self.assertEqual(compacted_texts[:-1], ["first task", "second task"])
        self.assertTrue(compacted_texts[-1].startswith("Another language model started"))
        self.assertIn("handoff summary", compacted_texts[-1])
        self.assertTrue(any(event.type == "context_compaction.completed" for event in result.events))

    def test_remote_compaction_uses_compact_endpoint_payload_and_replacement_history(self) -> None:
        real_user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "kept user"}]}
        remote_summary = {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": build_compaction_summary_text("remote summary")}],
        }
        model = RemoteCompactModel(
            compact_output=[
                {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "stale dev"}]},
                real_user,
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "<environment_context>\nold\n</environment_context>"}],
                },
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": "drop"}]},
                {"type": "function_call", "name": "exec_command", "call_id": "call-1", "arguments": "{}"},
                remote_summary,
            ]
        )
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.state.append_history(real_user)

        result = session.compact()

        self.assertEqual(result.final_message, "")
        self.assertEqual(len(model.compact_requests), 1)
        self.assertEqual(model.requests, [])
        request = model.compact_requests[0]
        self.assertTrue(request.tools)
        self.assertTrue(request.parallel_tool_calls)
        self.assertNotIn("CONTEXT CHECKPOINT COMPACTION", json.dumps(request.input, ensure_ascii=False))
        self.assertEqual([item.get("role") for item in result.history], ["user", "user"])
        self.assertEqual(result.history[-1]["content"][0]["text"], remote_summary["content"][0]["text"])
        completed = next(event for event in result.events if event.type == "context_compaction.completed")
        self.assertEqual(completed.payload["implementation"], "responses_compact")
        self.assertTrue(completed.payload["remote_compaction"])
        self.assertEqual(completed.payload["compacted_message"], "")

    def test_remote_compaction_falls_back_to_local_prompt_when_auto_mode_fails(self) -> None:
        model = RemoteCompactModel(compact_output=[], local_responses=[message("local summary")], fail_remote=True)
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "task"}]}
        )

        result = session.compact()

        self.assertEqual(result.final_message, "local summary")
        self.assertEqual(len(model.compact_requests), 1)
        self.assertEqual(len(model.requests), 1)
        self.assertIn("CONTEXT CHECKPOINT COMPACTION", model.requests[0].input[-1]["content"][0]["text"])
        self.assertIn("stream_error", [event.type for event in result.events])
        completed = next(event for event in result.events if event.type == "context_compaction.completed")
        self.assertEqual(completed.payload["implementation"], "responses")
        self.assertFalse(completed.payload["remote_compaction"])

    def test_remote_compaction_can_be_disabled_for_prompt_summary_comparison(self) -> None:
        model = RemoteCompactModel(compact_output=[], local_responses=[message("local summary")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, remote_compaction="off"),
            model_client=model,
        )

        result = session.compact()

        self.assertEqual(result.final_message, "local summary")
        self.assertEqual(model.compact_requests, [])
        self.assertEqual(len(model.requests), 1)
        self.assertEqual(model.requests[0].tools, [])
        self.assertFalse(model.requests[0].parallel_tool_calls)

    def test_manual_compaction_runs_pre_and_post_hooks(self) -> None:
        calls: list[dict] = []

        def hook_provider(request: dict) -> dict:
            calls.append(request)
            if request["event"] == "pre_compact":
                return {"additional_contexts": ["pre compact policy"]}
            return {}

        model = ScriptedResponsesModel([message("handoff summary")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=model,
        )
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first task"}]}
        )

        result = session.compact()

        self.assertEqual(result.final_message, "handoff summary")
        self.assertEqual([call["event"] for call in calls], ["pre_compact", "post_compact"])
        self.assertEqual(calls[0]["trigger"], "manual")
        self.assertEqual(calls[0]["model"], session.config.model)
        self.assertTrue(any("<hook_context>" in text for text in _request_texts(model.requests[0])))
        self.assertIn("hook.started", [event.type for event in result.events])
        self.assertIn("hook.completed", [event.type for event in result.events])

    def test_pre_compact_hook_can_abort_before_model_request(self) -> None:
        def hook_provider(request: dict) -> dict:
            if request["event"] == "pre_compact":
                return {"should_stop": True, "stop_reason": "skip compact"}
            return {}

        model = ScriptedResponsesModel([])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=model,
        )

        events = list(session.stream_compact())

        self.assertEqual(model.requests, [])
        self.assertIn("turn.aborted", [event.type for event in events])
        self.assertNotIn("model.request", [event.type for event in events])

    def test_post_compact_hook_can_abort_after_history_replacement(self) -> None:
        def hook_provider(request: dict) -> dict:
            if request["event"] == "post_compact":
                return {"should_stop": True, "stop_reason": "stop after compact"}
            return {}

        model = ScriptedResponsesModel([message("handoff summary")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, hook_provider=hook_provider),
            model_client=model,
        )
        session.state.append_history(
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first task"}]}
        )

        events = list(session.stream_compact())

        event_types = [event.type for event in events]
        self.assertIn("context_compaction.completed", event_types)
        self.assertIn("turn.aborted", event_types)
        self.assertTrue(session.state.history[-1]["content"][0]["text"].startswith("Another language model started"))

    def test_manual_compaction_persists_upstream_compacted_rollout_item(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel([message("handoff summary")])
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            )
            session.state.append_history(
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first task"}]}
            )
            session.state.append_history(
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "work"}]}
            )

            result = session.compact()

            matches = list((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            self.assertEqual(len(matches), 1)
            records = [json.loads(line) for line in matches[0].read_text(encoding="utf-8").splitlines()]
            compacted = [record["payload"] for record in records if record["type"] == "compacted"]
            self.assertEqual(len(compacted), 1)
            self.assertTrue(compacted[0]["message"].startswith("Another language model started"))
            self.assertIn("handoff summary", compacted[0]["message"])
            self.assertEqual(compacted[0]["replacement_history"], result.history)

    def test_rollout_reconstruction_uses_replacement_history_checkpoint(self) -> None:
        old_user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "old"}]}
        compact_user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "summary"}]}
        new_assistant = {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "after"}]}
        records = [
            {"type": "session_meta", "payload": {"id": "thread-1", "cwd": "/tmp"}},
            {"type": "response_item", "payload": old_user},
            {
                "type": "compacted",
                "payload": {"message": "summary", "replacement_history": [compact_user]},
            },
            {"type": "response_item", "payload": new_assistant},
        ]

        reconstructed = reconstruct_history_from_rollout(records)

        self.assertEqual(reconstructed.history, [compact_user, new_assistant])
        self.assertEqual(reconstructed.session_meta, {"id": "thread-1", "cwd": "/tmp"})
        self.assertFalse(reconstructed.legacy_compaction_without_replacement_history)

    def test_rollout_reconstruction_supports_legacy_compaction_without_replacement_history(self) -> None:
        records = [
            {
                "type": "response_item",
                "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first"}]},
            },
            {
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "work"}]},
            },
            {"type": "compacted", "payload": {"message": build_compaction_summary_text("legacy summary")}},
        ]

        reconstructed = reconstruct_history_from_rollout(records)

        self.assertTrue(reconstructed.legacy_compaction_without_replacement_history)
        self.assertIsNone(reconstructed.reference_context_item)
        self.assertEqual([item["role"] for item in reconstructed.history], ["user", "user"])
        self.assertEqual(reconstructed.history[0]["content"][0]["text"], "first")
        self.assertIn("legacy summary", reconstructed.history[-1]["content"][0]["text"])

    def test_rollout_reconstruction_extracts_turn_context_and_rollbacks(self) -> None:
        first_user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first"}]}
        first_assistant = {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "one"}]}
        second_user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]}
        second_assistant = {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "two"}]}
        first_turn_context = {"turn_id": "turn-1", "model": "gpt-first", "realtime_active": False}
        second_turn_context = {"turn_id": "turn-2", "model": "gpt-second", "realtime_active": True}
        records = [
            {"type": "turn_context", "payload": first_turn_context},
            {"type": "response_item", "payload": first_user},
            {"type": "response_item", "payload": first_assistant},
            {"type": "turn_context", "payload": second_turn_context},
            {"type": "response_item", "payload": second_user},
            {"type": "response_item", "payload": second_assistant},
            {"type": "event_msg", "payload": {"type": "thread_rolled_back", "num_turns": 1}},
        ]

        reconstructed = reconstruct_history_from_rollout(records)

        self.assertEqual(reconstructed.history, [first_user, first_assistant])
        self.assertEqual(reconstructed.previous_turn_settings, {"model": "gpt-first", "realtime_active": False})
        self.assertEqual(reconstructed.reference_context_item, first_turn_context)

    def test_pre_sampling_auto_compaction_uses_prior_token_usage_before_current_prompt(self) -> None:
        model = ScriptedResponsesModel([message("auto summary"), message("final after compact")])
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_auto_compact_token_limit=1,
            ),
            model_client=model,
        )
        session.state.append_history(
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "previous turn"}]}
        )
        session.state.record_token_usage({"input_tokens": 1, "output_tokens": 1, "total_tokens": 2})

        result = session.run("new prompt is not counted before pre-sampling compaction")

        self.assertEqual(result.final_message, "final after compact")
        self.assertEqual(len(model.requests), 2)
        self.assertEqual(model.requests[0].tools, [])
        self.assertFalse(model.requests[0].parallel_tool_calls)
        self.assertTrue(model.requests[1].tools)
        completed = [event for event in result.events if event.type == "context_compaction.completed"]
        self.assertEqual(completed[0].payload["trigger"], "auto")
        self.assertEqual(completed[0].payload["phase"], "pre_sampling")
        self.assertTrue(completed[0].payload["initial_context_injected"])
        self.assertEqual([item["role"] for item in result.history[:3]], ["developer", "user", "user"])
        self.assertTrue(result.history[1]["content"][-1]["text"].startswith("<environment_context>"))
        self.assertTrue(any(
            item.get("role") == "user"
            and item["content"][0]["text"].startswith("Another language model started")
            for item in result.history
            ))

    def test_current_first_prompt_does_not_trigger_pre_sampling_compaction_by_itself(self) -> None:
        model = ScriptedResponsesModel([message("final without compact")])
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_auto_compact_token_limit=1,
            ),
            model_client=model,
        )

        result = session.run("this prompt is long enough to exceed a tiny local estimate")

        self.assertEqual(result.final_message, "final without compact")
        self.assertEqual(len(model.requests), 1)
        self.assertTrue(model.requests[0].tools)
        self.assertFalse(any(event.type == "context_compaction.completed" for event in result.events))

    def test_mid_turn_auto_compaction_runs_after_tool_followup_usage_crosses_limit(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "update_plan",
                            "call_id": "plan-1",
                            "arguments": json.dumps(
                                {
                                    "plan": [
                                        {"step": "inspect", "status": "completed"},
                                        {"step": "finish", "status": "in_progress"},
                                    ]
                                }
                            ),
                        }
                    ],
                    "usage": {"input_tokens": 900, "output_tokens": 200, "total_tokens": 1100},
                },
                message("mid-turn summary"),
                message("final after mid-turn compact"),
            ]
        )
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_auto_compact_token_limit=1000,
            ),
            model_client=model,
        )

        result = session.run("use a tool before finishing")

        self.assertEqual(result.final_message, "final after mid-turn compact")
        self.assertEqual(len(model.requests), 3)
        self.assertEqual(model.requests[1].tools, [])
        completed = [event for event in result.events if event.type == "context_compaction.completed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].payload["trigger"], "auto")
        self.assertEqual(completed[0].payload["phase"], "mid_turn")
        started_turn_id = next(event.payload["turn_id"] for event in result.events if event.type == "turn.started")
        self.assertEqual(completed[0].payload["turn_id"], started_turn_id)

    def test_mid_turn_compaction_counts_local_tool_output_after_last_model_item(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "exec_command",
                            "call_id": "exec-compact",
                            "arguments": json.dumps(
                                {"cmd": f"{sys.executable!r} -c \"print('x' * 400)\""}
                            ),
                        }
                    ],
                    "usage": {"input_tokens": 5, "output_tokens": 5, "total_tokens": 10},
                },
                message("summary after local output"),
                message("final after local-output compact"),
            ]
        )
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_auto_compact_token_limit=60,
            ),
            model_client=model,
        )

        result = session.run("run a verbose command")

        self.assertEqual(result.final_message, "final after local-output compact")
        self.assertEqual(len(model.requests), 3)
        self.assertEqual(model.requests[1].tools, [])
        completed = [event for event in result.events if event.type == "context_compaction.completed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].payload["phase"], "mid_turn")

    def test_previous_model_downshift_compaction_uses_previous_model_before_sampling(self) -> None:
        previous_catalog = codex_types._MODEL_CATALOG_CACHE
        codex_types._MODEL_CATALOG_CACHE = {
            "huge-test": {
                "slug": "huge-test",
                "context_window": 5000,
                "default_reasoning_summary": "none",
                "supports_parallel_tool_calls": True,
                "input_modalities": ["text"],
            },
            "tiny-test": {
                "slug": "tiny-test",
                "context_window": 1000,
                "default_reasoning_summary": "none",
                "supports_parallel_tool_calls": True,
                "input_modalities": ["text"],
            },
        }
        try:
            model = ScriptedResponsesModel([message("downshift summary"), message("final after downshift")])
            session = CodexSession(
                CodexConfig(
                    model="tiny-test",
                    skip_git_repo_check=True,
                    ephemeral=True,
                ),
                model_client=model,
            )
            session.state.previous_turn_settings = {"model": "huge-test", "realtime_active": False}
            session.state.record_token_usage({"input_tokens": 950, "output_tokens": 0, "total_tokens": 950})

            result = session.run("continue after switching to the smaller model")

            self.assertEqual(result.final_message, "final after downshift")
            self.assertEqual([request.model for request in model.requests], ["huge-test", "tiny-test"])
            self.assertEqual(model.requests[0].tools, [])
            completed = [event for event in result.events if event.type == "context_compaction.completed"]
            self.assertEqual(len(completed), 1)
            self.assertEqual(completed[0].payload["reason"], "model_downshift")
            self.assertEqual(completed[0].payload["phase"], "pre_sampling")
            self.assertFalse(completed[0].payload["initial_context_injected"])
        finally:
            codex_types._MODEL_CATALOG_CACHE = previous_catalog

    def test_persistent_auto_compaction_reconstructs_reference_turn_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                    model_auto_compact_token_limit=1,
                ),
                model_client=ScriptedResponsesModel([message("auto summary"), message("final")]),
            )
            session.state.append_history(
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "previous"}]}
            )
            session.state.record_token_usage({"input_tokens": 1, "output_tokens": 1, "total_tokens": 2})

            result = session.run("force automatic compaction")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            compacted_index = next(index for index, record in enumerate(records) if record["type"] == "compacted")
            reference_turn_context = records[compacted_index + 1]
            self.assertEqual(reference_turn_context["type"], "turn_context")

            reconstructed = reconstruct_history_from_rollout(rollout_path)

            self.assertEqual(reconstructed.reference_context_item, reference_turn_context["payload"])
            self.assertEqual(reconstructed.previous_turn_settings["model"], CodexConfig().model)
            self.assertTrue(any(
                item.get("role") == "user"
                and item["content"][0]["text"].startswith("Another language model started")
                for item in reconstructed.history
            ))

    def test_base_instructions_exclude_dynamic_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git").mkdir()
            (root / "AGENTS.md").write_text("project-only instruction", encoding="utf-8")
            codex_home = root / "codex-home"
            memories = codex_home / "memories"
            memories.mkdir(parents=True)
            (memories / "memory_summary.md").write_text("Important prior decision.", encoding="utf-8")

            base = build_base_instructions(
                prompt_asset="gpt_5_codex_prompt.md",
                cwd=root,
                sandbox="workspace-write",
                approval_policy="never",
                codex_home=codex_home,
                memory_tool_enabled=True,
                use_memories=True,
            )

            self.assertIn("You are", base)
            self.assertNotIn("project-only instruction", base)
            self.assertNotIn("Important prior decision.", base)
            self.assertNotIn("Approval policy is currently never.", base)

    def test_auto_base_instructions_use_upstream_model_catalog(self) -> None:
        base = build_base_instructions(
            prompt_asset="auto",
            model="gpt-5.5",
            cwd=Path.cwd(),
            sandbox="workspace-write",
            approval_policy="never",
        )

        self.assertEqual(base, read_model_catalog_instructions("gpt-5.5"))
        self.assertIn("Intermediary updates", base)

    def test_initial_context_matches_upstream_fragment_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git").mkdir()
            (root / "AGENTS.md").write_text("root doc", encoding="utf-8")
            nested = root / "workspace" / "crate_a"
            nested.mkdir(parents=True)
            (nested / "AGENTS.md").write_text("crate doc", encoding="utf-8")

            config = CodexConfig(
                cwd=nested,
                skip_git_repo_check=True,
                ephemeral=True,
                approval_policy="never",
                current_date="2026-05-14",
                timezone="America/New_York",
            )
            items = build_initial_context_items(config)

            self.assertEqual([item["role"] for item in items], ["developer", "user"])
            permissions_text = items[0]["content"][0]["text"]
            self.assertIn("`sandbox_mode` is `workspace-write`", permissions_text)
            self.assertIn("Network access is restricted.", permissions_text)
            self.assertIn(f"The writable root is `{nested.resolve()}`.", permissions_text)

            user_texts = [part["text"] for part in items[1]["content"]]
            self.assertTrue(user_texts[0].startswith(f"# AGENTS.md instructions for {nested.resolve()}"))
            self.assertIn("<INSTRUCTIONS>\nroot doc\n\ncrate doc\n</INSTRUCTIONS>", user_texts[0])
            self.assertTrue(user_texts[1].startswith("<environment_context>"))
            self.assertTrue(user_texts[1].endswith("</environment_context>"))
            self.assertIn(f"  <cwd>{nested.resolve()}</cwd>", user_texts[1])
            self.assertIn("  <shell>", user_texts[1])
            self.assertIn("  <current_date>2026-05-14</current_date>", user_texts[1])
            self.assertIn("  <timezone>America/New_York</timezone>", user_texts[1])
            self.assertNotIn("<os>", user_texts[1])

    def test_session_request_places_initial_context_before_user_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git").mkdir()
            (root / "AGENTS.md").write_text("be nice", encoding="utf-8")
            model = ScriptedResponsesModel([message("done")])
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    skip_git_repo_check=True,
                    ephemeral=True,
                    current_date="2026-05-14",
                    timezone="America/New_York",
                ),
                model_client=model,
            )

            session.run("hello")

            request = model.requests[0]
            self.assertNotIn("be nice", request.instructions)
            self.assertEqual([item["role"] for item in request.input[:3]], ["developer", "user", "user"])
            self.assertEqual(request.input[2]["content"][0]["text"], "hello")
            self.assertEqual(request.client_metadata, {"x-codex-installation-id": session.state.installation_id})

    def test_session_request_includes_configured_output_schema(self) -> None:
        schema = {
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": False,
        }
        model = ScriptedResponsesModel([message('{"answer":"done"}')])
        CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                output_schema=schema,
            ),
            model_client=model,
        ).run("hello")

        self.assertEqual(model.requests[0].output_schema, schema)
        self.assertTrue(model.requests[0].output_schema_strict)
        self.assertEqual(model.requests[0].to_responses_kwargs()["text"]["format"]["schema"], schema)

    def test_session_request_includes_local_image_inputs_before_text(self) -> None:
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow is required for local image input tests")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "input.png"
            Image.new("RGBA", (1, 1), (10, 20, 30, 255)).save(image_path)
            model = ScriptedResponsesModel([message("saw image")])

            CodexSession(
                CodexConfig(
                    cwd=root,
                    skip_git_repo_check=True,
                    ephemeral=True,
                    input_images=(Path("input.png"),),
                ),
                model_client=model,
            ).run("describe it")

            user_item = model.requests[0].input[-1]
            content = user_item["content"]
            self.assertEqual(content[0], {"type": "input_text", "text": "<image name=[Image #1]>"})
            self.assertEqual(content[1]["type"], "input_image")
            self.assertTrue(content[1]["image_url"].startswith("data:image/png;base64,"))
            self.assertEqual(content[1]["detail"], "high")
            self.assertEqual(content[2], {"type": "input_text", "text": "</image>"})
            self.assertEqual(content[3], {"type": "input_text", "text": "describe it"})

    def test_prompt_history_normalizes_call_output_pairs_for_model_request(self) -> None:
        model = ScriptedResponsesModel([message("done")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.state.append_history(
            {
                "type": "function_call",
                "name": "exec_command",
                "call_id": "missing-output",
                "arguments": json.dumps({"cmd": "pwd"}),
            }
        )
        session.state.append_history(
            {"type": "function_call_output", "call_id": "orphan-output", "output": "orphan"}
        )

        session.run("hello")

        request = model.requests[0]
        call_index = next(index for index, item in enumerate(request.input) if item.get("call_id") == "missing-output")
        self.assertEqual(request.input[call_index + 1], {
            "type": "function_call_output",
            "call_id": "missing-output",
            "output": "aborted",
        })
        self.assertFalse(any(item.get("call_id") == "orphan-output" for item in request.input))
        self.assertTrue(any(item.get("call_id") == "orphan-output" for item in session.state.history))

    def test_prepare_prompt_history_strips_images_when_model_does_not_support_images(self) -> None:
        history = [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "look"},
                    {"type": "input_image", "image_url": "data:image/png;base64,abc", "detail": "high"},
                ],
            },
            {
                "type": "function_call",
                "name": "view_image",
                "call_id": "view-1",
                "arguments": "{}",
            },
            {
                "type": "function_call_output",
                "call_id": "view-1",
                "output": {
                    "content": [
                        {"type": "input_text", "text": "image result"},
                        {"type": "input_image", "image_url": "data:image/png;base64,def", "detail": "high"},
                    ],
                    "success": True,
                },
            },
        ]

        prompt_history = prepare_prompt_history(
            history,
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model_supports_image_input=False,
            ),
        )

        placeholder = "image content omitted because you do not support image input"
        self.assertEqual(prompt_history[0]["content"][1], {"type": "input_text", "text": placeholder})
        self.assertEqual(prompt_history[2]["output"]["content"][1], {"type": "input_text", "text": placeholder})
        self.assertEqual(history[0]["content"][1]["type"], "input_image")

    def test_prepare_prompt_history_preserves_images_for_image_models(self) -> None:
        history = [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "look"},
                    {"type": "input_image", "image_url": "data:image/png;base64,abc", "detail": "high"},
                ],
            }
        ]

        prompt_history = prepare_prompt_history(history, CodexConfig(skip_git_repo_check=True, ephemeral=True))

        self.assertEqual(prompt_history[0]["content"][1]["type"], "input_image")

    def test_prepare_prompt_history_truncates_long_tool_outputs(self) -> None:
        long_output = "tool output line\n" * 5_000
        history = [
            {"type": "function_call", "name": "exec_command", "call_id": "call-1", "arguments": "{}"},
            {"type": "function_call_output", "call_id": "call-1", "output": long_output},
        ]

        prompt_history = prepare_prompt_history(
            history,
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                model="tiny-test-model",
                model_supports_image_input=False,
            ),
        )

        self.assertIn("tokens truncated", prompt_history[1]["output"])
        self.assertLess(len(prompt_history[1]["output"]), len(long_output))

    def test_agents_md_discovery_uses_root_to_cwd_order_and_local_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".git").mkdir()
            (root / "AGENTS.md").write_text("root versioned", encoding="utf-8")
            child = root / "pkg"
            child.mkdir()
            (child / "AGENTS.md").write_text("child versioned", encoding="utf-8")
            (child / "AGENTS.override.md").write_text("child local", encoding="utf-8")

            self.assertEqual(collect_agents_md(child), "root versioned\n\nchild local")

    def test_environment_context_and_permissions_helpers_match_upstream_shape(self) -> None:
        env = build_environment_context(
            Path("/tmp/example"),
            shell="bash",
            current_date="2026-05-14",
            timezone="America/New_York",
        )
        self.assertEqual(
            env,
            "<environment_context>\n"
            "  <cwd>/tmp/example</cwd>\n"
            "  <shell>bash</shell>\n"
            "  <current_date>2026-05-14</current_date>\n"
            "  <timezone>America/New_York</timezone>\n"
            "</environment_context>",
        )
        permissions = build_permissions_instructions(
            cwd=Path("/tmp/example"),
            sandbox="read-only",
            approval_policy="on-failure",
            network_access="enabled",
        )
        self.assertIn("`sandbox_mode` is `read-only`", permissions)
        self.assertIn("Network access is enabled.", permissions)
        self.assertIn("`approval_policy` is `on-failure`", permissions)

    def test_memory_read_instructions_are_feature_gated_in_initial_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            memories = codex_home / "memories"
            memories.mkdir(parents=True)
            (memories / "memory_summary.md").write_text("Important prior decision.", encoding="utf-8")

            disabled = build_initial_context_items(
                CodexConfig(
                    cwd=root,
                    sandbox="workspace-write",
                    approval_policy="never",
                    codex_home=codex_home,
                    memory_tool_enabled=False,
                    use_memories=True,
                    skip_git_repo_check=True,
                    ephemeral=True,
                )
            )
            enabled = build_initial_context_items(
                CodexConfig(
                    cwd=root,
                    sandbox="workspace-write",
                    approval_policy="never",
                    codex_home=codex_home,
                    memory_tool_enabled=True,
                    use_memories=True,
                    skip_git_repo_check=True,
                    ephemeral=True,
                )
            )
            disabled_text = "\n".join(part["text"] for item in disabled for part in item["content"])
            enabled_text = "\n".join(part["text"] for item in enabled for part in item["content"])
            self.assertNotIn("## Memory", disabled_text)
            self.assertIn("## Memory", enabled_text)
            self.assertIn("Important prior decision.", enabled_text)

    def test_memory_citation_parser_matches_upstream_shape(self) -> None:
        visible, citations = strip_memory_citations(
            "answer<oai-mem-citation><citation_entries>\n"
            "MEMORY.md:2-4|note=[used repo decision]\n"
            "</citation_entries>\n<rollout_ids>\n019cc2ea-1dff-7902-8d40-c8f6e5d83cc4\n"
            "019cc2ea-1dff-7902-8d40-c8f6e5d83cc4\n</rollout_ids></oai-mem-citation>"
        )
        self.assertEqual(visible, "answer")
        parsed = parse_memory_citation(citations)
        self.assertEqual(
            parsed,
            {
                "entries": [
                    {
                        "path": "MEMORY.md",
                        "line_start": 2,
                        "line_end": 4,
                        "note": "used repo decision",
                    }
                ],
                "rollout_ids": ["019cc2ea-1dff-7902-8d40-c8f6e5d83cc4"],
            },
        )

    def test_proposed_plan_block_parser_matches_upstream_line_shape(self) -> None:
        text = "before\n<proposed_plan>\n- step\n</proposed_plan>\nafter"
        self.assertEqual(strip_proposed_plan_blocks(text), "before\nafter")
        self.assertEqual(extract_proposed_plan_text(text), "- step\n")
        self.assertEqual(strip_proposed_plan_blocks("  <proposed_plan> extra\n"), "  <proposed_plan> extra\n")
        self.assertEqual(strip_proposed_plan_blocks("<proposed_plan>\n- step\n"), "")

    def test_plan_mode_hides_proposed_plan_from_final_message_and_item_text(self) -> None:
        model = ScriptedResponsesModel(
            [
                message(
                    "before\n<proposed_plan>\n- implement it\n</proposed_plan>\nafter"
                )
            ]
        )
        session = CodexSession(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                collaboration_mode="Plan",
            ),
            model_client=model,
        )

        result = session.run("make a plan")

        self.assertEqual(result.final_message, "before\nafter")
        assistant = next(item for item in result.history if item.get("role") == "assistant")
        self.assertEqual(assistant["content"][0]["text"], "before\nafter")
        self.assertEqual(assistant["proposed_plan"], "- implement it\n")

    def test_non_plan_mode_keeps_proposed_plan_text_visible(self) -> None:
        model = ScriptedResponsesModel([message("<proposed_plan>\n- visible\n</proposed_plan>")])
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("show raw")

        self.assertEqual(result.final_message, "<proposed_plan>\n- visible\n</proposed_plan>")

    def test_memory_write_stage_one_prompt_builders_match_upstream_shape(self) -> None:
        self.assertIn("Memory Writing Agent", memory_stage_one_system_prompt())
        self.assertEqual(
            memory_stage_one_rollout_token_limit(
                model_context_window=123_000,
                effective_context_window_percent=95,
            ),
            ((123_000 * 95) // 100) * 70 // 100,
        )
        self.assertEqual(memory_stage_one_rollout_token_limit(model_context_window=None), 150_000)

        contents = f"{'a' * 100}middle{'z' * 100}"
        message_text = build_memory_stage_one_input_message(
            rollout_path=Path("/tmp/rollout.jsonl"),
            rollout_cwd=Path("/tmp"),
            rollout_contents=contents,
            model_context_window=10,
            effective_context_window_percent=95,
        )

        self.assertIn("rollout_path: /tmp/rollout.jsonl", message_text)
        self.assertIn("rollout_cwd: /tmp", message_text)
        self.assertIn("tokens truncated", message_text)
        self.assertIn("aaaa", message_text)
        self.assertIn("zzzz", message_text)

    def test_memory_write_consolidation_prompt_points_to_diff_and_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            memory_root = Path(tmp) / "memories"
            (memory_root / "extensions").mkdir(parents=True)

            prompt = build_memory_consolidation_prompt(memory_root)

            self.assertIn("Memory workspace diff:", prompt)
            self.assertIn("phase2_workspace_diff.md", prompt)
            self.assertIn(f"Memory extensions (under {memory_root / 'extensions'}/):", prompt)
            self.assertIn("workspace diff shows deleted extension resource files", prompt)

    def test_memory_stage_one_schema_and_extractor_match_upstream_shape(self) -> None:
        schema = memory_stage_one_output_schema()
        self.assertEqual(sorted(schema["required"]), ["raw_memory", "rollout_slug", "rollout_summary"])
        self.assertEqual(schema["properties"]["rollout_slug"]["type"], ["string", "null"])
        self.assertFalse(schema["additionalProperties"])

        output = json.dumps(
            {
                "raw_memory": "Remember this.",
                "rollout_summary": "short summary",
                "rollout_slug": "Memory Work",
            }
        )
        model = ScriptedResponsesModel([message(output)])
        result = extract_memory_stage_one(
            model_client=model,
            rollout_path=Path("/tmp/rollout.jsonl"),
            rollout_cwd=Path("/tmp"),
            rollout_contents='[{"type":"message","role":"user"}]',
            prompt_cache_key="thread-1",
        )

        self.assertEqual(result.raw_memory, "Remember this.")
        self.assertEqual(result.rollout_summary, "short summary")
        self.assertEqual(result.rollout_slug, "Memory Work")
        request = model.requests[0]
        self.assertEqual(request.model, "gpt-5.4-mini")
        self.assertEqual(request.reasoning, {"effort": "low", "summary": "auto"})
        self.assertEqual(request.include, ["reasoning.encrypted_content"])
        self.assertEqual(request.prompt_cache_key, "thread-1")
        self.assertEqual(request.output_schema, schema)
        self.assertIn("rollout_path: /tmp/rollout.jsonl", request.input[0]["content"][0]["text"])

    def test_memory_stage_one_parser_rejects_unknown_fields_and_redacts_secrets(self) -> None:
        with self.assertRaises(ValueError):
            parse_memory_stage_one_output(
                json.dumps(
                    {
                        "raw_memory": "x",
                        "rollout_summary": "y",
                        "rollout_slug": None,
                        "extra": "z",
                    }
                )
            )

        parsed = parse_memory_stage_one_output(
            json.dumps(
                {
                    "raw_memory": "token=sk-1234567890abcdefghijklmnop",
                    "rollout_summary": "api_key=secret1234",
                    "rollout_slug": None,
                }
            )
        )
        self.assertEqual(parsed.raw_memory, "token=[REDACTED_SECRET]")
        self.assertEqual(parsed.rollout_summary, "api_key=[REDACTED_SECRET]")

    def test_memory_rollout_filter_matches_upstream_memory_policy_shape(self) -> None:
        developer = {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "dev"}]}
        agents = {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "# AGENTS.md instructions for /tmp\n\n<INSTRUCTIONS>\nbody\n</INSTRUCTIONS>",
                }
            ],
        }
        skill = {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "<skill>\nbody\n</skill>"}],
        }
        environment = {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "<environment_context>\n<cwd>/tmp</cwd>\n</environment_context>"}],
        }
        user = {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "keep"}]}
        reasoning = {"type": "reasoning", "summary": []}
        tool_call = {"type": "function_call", "name": "exec_command", "arguments": "{}", "call_id": "call-1"}
        web_search = {"type": "web_search_call", "id": "ws-1", "status": "completed"}

        self.assertIsNone(sanitize_response_item_for_memories(developer))
        self.assertIsNone(sanitize_response_item_for_memories(agents))
        self.assertIsNone(sanitize_response_item_for_memories(skill))
        self.assertEqual(sanitize_response_item_for_memories(environment), environment)
        self.assertEqual(sanitize_response_item_for_memories(user), user)
        self.assertIsNone(sanitize_response_item_for_memories(reasoning))
        self.assertEqual(sanitize_response_item_for_memories(tool_call), tool_call)
        self.assertEqual(sanitize_response_item_for_memories(web_search), web_search)

        serialized = serialize_filtered_rollout_response_items(
            [developer, agents, skill, environment, user, reasoning, tool_call, web_search]
        )
        parsed = json.loads(serialized)
        self.assertEqual([item["type"] for item in parsed], ["message", "message", "function_call", "web_search_call"])

    def test_memory_rollout_loader_supports_upstream_and_python_jsonl_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout.jsonl"
            lines = [
                {
                    "timestamp": "2026-01-01T00:00:00.000Z",
                    "type": "session_meta",
                    "payload": {
                        "meta": {
                            "id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                            "timestamp": "2026-01-01T00:00:00Z",
                            "cwd": "/tmp/upstream",
                        },
                        "git": {"branch": "main"},
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:01.000Z",
                    "type": "response_item",
                    "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                },
                {
                    "ts": 1767225602.0,
                    "type": "item.completed",
                    "thread_id": "python-thread",
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "hello"}],
                    },
                },
            ]
            path.write_text("\n".join(json.dumps(line) for line in lines), encoding="utf-8")

            rollout = load_memory_rollout(path)

            self.assertEqual(rollout.thread_id, "python-thread")
            self.assertEqual(rollout.cwd, Path("/tmp/upstream"))
            self.assertEqual(rollout.git_branch, "main")
            serialized = json.loads(rollout.serialized_contents)
            self.assertEqual([item["role"] for item in serialized], ["user", "assistant"])

    def test_memory_stage1_startup_eligibility_matches_upstream_source_idle_age_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollout.jsonl"
            path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "type": "session_meta",
                                "payload": {
                                    "meta": {
                                        "id": "thread-exec",
                                        "timestamp": "2026-01-01T00:00:00Z",
                                        "cwd": "/tmp/upstream",
                                        "source": "exec",
                                        "memory_mode": "enabled",
                                    }
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "hi"}],
                                },
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            exec_rollout = load_memory_rollout(path)

            self.assertEqual(exec_rollout.source, "exec")
            self.assertFalse(
                memory_rollout_is_stage1_startup_eligible(
                    exec_rollout,
                    now=datetime(2026, 1, 2, tzinfo=timezone.utc),
                    min_rollout_idle_hours=0,
                )
            )
            self.assertFalse(
                memory_rollout_is_stage1_startup_eligible(
                    exec_rollout,
                    current_thread_id="thread-exec",
                    allowed_sources=frozenset({"exec"}),
                    now=datetime(2026, 1, 2, tzinfo=timezone.utc),
                    min_rollout_idle_hours=0,
                )
            )
            self.assertTrue(
                memory_rollout_is_stage1_startup_eligible(
                    exec_rollout,
                    allowed_sources=frozenset({"exec"}),
                    now=datetime(2026, 1, 1, 7, tzinfo=timezone.utc),
                )
            )

    def test_memory_stage_one_for_rollout_and_startup_once_use_serialized_rollouts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            sessions.mkdir(parents=True)
            rollout_path = sessions / "0194f5a6-89ab-7cde-8123-456789abcdef.jsonl"
            rollout_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "ts": 1767225600.0,
                                "type": "item.completed",
                                "thread_id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                                "item": {
                                    "type": "message",
                                    "role": "user",
                                    "content": [{"type": "input_text", "text": "remember useful thing"}],
                                },
                            }
                        )
                    ]
                ),
                encoding="utf-8",
            )
            model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Useful thing.",
                                "rollout_summary": "Remembered useful thing.",
                                "rollout_slug": "Useful Thing",
                            }
                        )
                    ),
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Useful thing.",
                                "rollout_summary": "Remembered useful thing.",
                                "rollout_slug": "Useful Thing",
                            }
                        )
                    ),
                ]
            )

            record = run_memory_stage_one_for_rollout(model_client=model, rollout_path=rollout_path)
            self.assertIsNotNone(record)
            assert record is not None
            self.assertEqual(record.thread_id, "0194f5a6-89ab-7cde-8123-456789abcdef")
            self.assertEqual(record.rollout_slug, "Useful Thing")
            self.assertIn("remember useful thing", model.requests[0].input[0]["content"][0]["text"])

            self.assertEqual(memory_rollout_candidates(codex_home), [rollout_path])
            store = MemoryStateStore(Path(tmp) / "memory-state.sqlite3")
            startup = run_memory_startup_once(
                codex_home=codex_home,
                model_client=model,
                state_store=store,
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
            )
            self.assertEqual(len(startup.records), 1)
            raw_text = raw_memories_file(startup.memory_root).read_text(encoding="utf-8")
            self.assertIn("Useful thing.", raw_text)
            ad_hoc_instructions = memory_extensions_root(startup.memory_root) / "ad_hoc" / "instructions.md"
            self.assertIn("Ad-hoc notes", ad_hoc_instructions.read_text(encoding="utf-8"))
            stored = store.get_stage1_output("0194f5a6-89ab-7cde-8123-456789abcdef")
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored["raw_memory"], "Useful thing.")
            self.assertEqual(store.get_job("memory_consolidate_global", "global")["status"], "pending")
            self.assertEqual(
                [memory.thread_id for memory in store.get_phase2_input_selection(n=1, max_unused_days=36_500)],
                ["0194f5a6-89ab-7cde-8123-456789abcdef"],
            )
            store.close()

    def test_memory_startup_claims_before_model_and_skips_completed_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            sessions.mkdir(parents=True)
            rollout_path = sessions / "0194f5a6-89ab-7cde-8123-456789abcdef.jsonl"
            rollout_path.write_text(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).timestamp(),
                        "type": "item.completed",
                        "thread_id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                        "item": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "remember useful thing"}],
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = MemoryStateStore(Path(tmp) / "memory-state.sqlite3")
            first_model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Useful thing.",
                                "rollout_summary": "Remembered useful thing.",
                                "rollout_slug": "Useful Thing",
                            }
                        )
                    )
                ]
            )
            first = run_memory_startup_once(
                codex_home=codex_home,
                model_client=first_model,
                state_store=store,
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
            )
            self.assertEqual(len(first.records), 1)

            exhausted_model = ScriptedResponsesModel([])
            second = run_memory_startup_once(
                codex_home=codex_home,
                model_client=exhausted_model,
                state_store=store,
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
            )

            self.assertEqual(second.records, [])
            self.assertEqual(len(exhausted_model.requests), 0)
            store.close()

    def test_memory_startup_uses_thread_store_claim_scan_instead_of_recent_file_slice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            sessions.mkdir(parents=True)
            now = datetime.now(timezone.utc)

            def write_rollout(name: str, *, source: str, updated_at: datetime, text: str) -> Path:
                path = sessions / f"{name}.jsonl"
                path.write_text(
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "session_meta",
                                    "payload": {
                                        "meta": {
                                            "id": name,
                                            "timestamp": updated_at.isoformat(),
                                            "cwd": str(Path(tmp)),
                                            "source": source,
                                            "memory_mode": "enabled",
                                        }
                                    },
                                }
                            ),
                            json.dumps(
                                {
                                    "type": "response_item",
                                    "payload": {
                                        "type": "message",
                                        "role": "user",
                                        "content": [{"type": "input_text", "text": text}],
                                    },
                                }
                            ),
                        ]
                    ),
                    encoding="utf-8",
                )
                os.utime(path, (updated_at.timestamp(), updated_at.timestamp()))
                return path

            write_rollout("current-thread", source="cli", updated_at=now - timedelta(hours=1), text="current")
            write_rollout("exec-thread", source="exec", updated_at=now - timedelta(hours=2), text="exec")
            write_rollout("eligible-thread", source="cli", updated_at=now - timedelta(hours=3), text="eligible memory")

            store = MemoryStateStore(Path(tmp) / "memory-state.sqlite3")
            model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Eligible memory.",
                                "rollout_summary": "Remembered eligible.",
                                "rollout_slug": "Eligible",
                            }
                        )
                    )
                ]
            )
            startup = run_memory_startup_once(
                codex_home=codex_home,
                model_client=model,
                state_store=store,
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
                current_thread_id="current-thread",
            )

            self.assertEqual([record.thread_id for record in startup.records], ["eligible-thread"])
            self.assertIn("eligible memory", model.requests[0].input[0]["content"][0]["text"])
            self.assertIsNotNone(store.get_stage1_output("eligible-thread"))
            self.assertIsNone(store.get_stage1_output("exec-thread"))
            store.close()

    def test_session_runs_memory_startup_once_when_feature_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            root.mkdir()
            sessions.mkdir(parents=True)
            rollout_path = sessions / "0194f5a6-89ab-7cde-8123-456789abcdef.jsonl"
            rollout_path.write_text(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).timestamp(),
                        "type": "item.completed",
                        "thread_id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                        "item": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "remember startup"}],
                        },
                    }
                ),
                encoding="utf-8",
            )
            model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Startup memory.",
                                "rollout_summary": "Remembered startup.",
                                "rollout_slug": "Startup",
                            }
                        )
                    ),
                    message("main done"),
                ]
            )
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    memory_tool_enabled=True,
                    use_memories=True,
                    memory_min_rollout_idle_hours=0,
                    memory_max_rollout_age_days=36_500,
                    memory_startup_background=False,
                    memory_run_phase2_on_startup=False,
                ),
                model_client=model,
            )
            result = session.run("hello")

            self.assertEqual(result.final_message, "main done")
            self.assertIsNotNone(session.memory_startup_result)
            assert session.memory_startup_result is not None
            self.assertEqual([record.thread_id for record in session.memory_startup_result.records], [
                "0194f5a6-89ab-7cde-8123-456789abcdef"
            ])
            self.assertIn("Startup memory.", raw_memories_file(session.memory_startup_result.memory_root).read_text(encoding="utf-8"))
            self.assertIsNotNone(session.config.memory_state_store)
            session.config.memory_state_store.close()

    def test_memory_rate_limit_guard_matches_upstream_threshold_shape(self) -> None:
        self.assertTrue(memory_rate_limit_allows_startup(None, min_remaining_percent=25))
        self.assertTrue(
            memory_rate_limit_allows_startup(
                {"primary": {"used_percent": 74.9}, "secondary": {"used_percent": 10}},
                min_remaining_percent=25,
            )
        )
        self.assertFalse(
            memory_rate_limit_allows_startup(
                {"primary": {"used_percent": 75.1}, "secondary": {"used_percent": 10}},
                min_remaining_percent=25,
            )
        )
        self.assertFalse(
            memory_rate_limit_allows_startup(
                {"rate_limit_reached_type": "hard", "primary": {"used_percent": 1}},
                min_remaining_percent=0,
            )
        )

    @unittest.skipUnless(shutil.which("git"), "git CLI is required for memory startup pipeline tests")
    def test_memory_startup_pipeline_runs_phase2_after_claimed_stage1(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            sessions.mkdir(parents=True)
            rollout_path = sessions / "0194f5a6-89ab-7cde-8123-456789abcdef.jsonl"
            rollout_path.write_text(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).timestamp(),
                        "type": "item.completed",
                        "thread_id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                        "item": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "remember pipeline"}],
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = MemoryStateStore(Path(tmp) / "memory-state.sqlite3")
            model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Pipeline memory.",
                                "rollout_summary": "Remembered pipeline.",
                                "rollout_slug": "Pipeline",
                            }
                        )
                    ),
                    message("phase2 complete"),
                ]
            )

            result = run_memory_startup_pipeline_once(
                codex_home=codex_home,
                model_client=model,
                state_store=store,
                base_config=CodexConfig(codex_home=codex_home, skip_git_repo_check=True),
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
                run_phase2=True,
            )

            self.assertEqual(result.status, "completed")
            self.assertEqual(len(result.records), 1)
            self.assertIsNotNone(result.phase2_result)
            assert result.phase2_result is not None
            self.assertEqual(result.phase2_result.status, "succeeded")
            self.assertIn("Pipeline memory.", raw_memories_file(result.memory_root).read_text(encoding="utf-8"))
            self.assertFalse((result.memory_root / "phase2_workspace_diff.md").exists())
            store.close()

    def test_memory_startup_background_task_uses_own_state_store_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            sessions = codex_home / "sessions"
            sessions.mkdir(parents=True)
            rollout_path = sessions / "0194f5a6-89ab-7cde-8123-456789abcdef.jsonl"
            rollout_path.write_text(
                json.dumps(
                    {
                        "ts": datetime.now(timezone.utc).timestamp(),
                        "type": "item.completed",
                        "thread_id": "0194f5a6-89ab-7cde-8123-456789abcdef",
                        "item": {
                            "type": "message",
                            "role": "user",
                            "content": [{"type": "input_text", "text": "remember background"}],
                        },
                    }
                ),
                encoding="utf-8",
            )
            state_store_path = Path(tmp) / "memory-state.sqlite3"
            model = ScriptedResponsesModel(
                [
                    message(
                        json.dumps(
                            {
                                "raw_memory": "Background memory.",
                                "rollout_summary": "Remembered background.",
                                "rollout_slug": "Background",
                            }
                        )
                    )
                ]
            )

            task = start_memory_startup_task(
                codex_home=codex_home,
                model_client=model,
                state_store_path=state_store_path,
                max_rollouts=1,
                max_unused_days=36_500,
                max_rollout_age_days=36_500,
                min_rollout_idle_hours=0,
                run_phase2=False,
            )
            result = task.join(timeout=5)

            self.assertEqual(task.status, "completed")
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result.records[0].raw_memory, "Background memory.")
            store = MemoryStateStore(state_store_path)
            self.assertIsNotNone(store.get_stage1_output("0194f5a6-89ab-7cde-8123-456789abcdef"))
            store.close()

    def test_memory_rollout_summary_file_stem_matches_upstream_v7_and_slug_rules(self) -> None:
        fixed = MemoryStageOneRecord(
            thread_id="0194f5a6-89ab-7cde-8123-456789abcdef",
            source_updated_at=datetime.fromtimestamp(123, tz=timezone.utc),
            raw_memory="raw memory",
            rollout_summary="summary",
            rollout_slug=None,
            rollout_path=Path("/tmp/rollout.jsonl"),
            cwd=Path("/tmp/workspace"),
        )
        self.assertEqual(rollout_summary_file_stem(fixed), "2025-02-11T15-35-19-jqmb")

        slugged = MemoryStageOneRecord(
            thread_id=fixed.thread_id,
            source_updated_at=fixed.source_updated_at,
            raw_memory=fixed.raw_memory,
            rollout_summary=fixed.rollout_summary,
            rollout_slug="Unsafe Slug/With Spaces & Symbols + EXTRA_LONG_12345_67890_ABCDE_fghij_klmno",
            rollout_path=fixed.rollout_path,
            cwd=fixed.cwd,
        )
        slug = rollout_summary_file_stem(slugged).removeprefix("2025-02-11T15-35-19-jqmb-")
        self.assertEqual(len(slug), 60)
        self.assertEqual(slug, "unsafe_slug_with_spaces___symbols___extra_long_12345_67890_a")

    def test_memory_storage_rebuilds_raw_memories_and_rollout_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "memory"
            rollout_summaries_dir(root).mkdir(parents=True)
            stale = rollout_summaries_dir(root) / "stale.md"
            stale.write_text("old", encoding="utf-8")

            record = MemoryStageOneRecord(
                thread_id="0194f5a6-89ab-7cde-8123-456789abcdef",
                source_updated_at=datetime.fromtimestamp(100, tz=timezone.utc),
                raw_memory="raw memory",
                rollout_summary="short summary",
                rollout_slug=None,
                rollout_path=Path("/tmp/rollout-100.jsonl"),
                cwd=Path("/tmp/workspace"),
            )

            sync_rollout_summaries_from_memories(root, [record], 100, max_unused_days=36_500)
            rebuild_raw_memories_file_from_memories(root, [record], 100, max_unused_days=36_500)

            self.assertFalse(stale.exists())
            summary_files = sorted(path.name for path in rollout_summaries_dir(root).iterdir())
            self.assertEqual(summary_files, [f"{rollout_summary_file_stem(record)}.md"])
            raw_memories = raw_memories_file(root).read_text(encoding="utf-8")
            self.assertIn("# Raw Memories", raw_memories)
            self.assertIn("raw memory", raw_memories)
            self.assertIn("cwd: /tmp/workspace", raw_memories)
            self.assertIn("rollout_path: /tmp/rollout-100.jsonl", raw_memories)
            self.assertIn(f"rollout_summary_file: {summary_files[0]}", raw_memories)

    def test_memory_phase2_input_selection_matches_upstream_retention_rules(self) -> None:
        now = datetime(2026, 5, 15, tzinfo=timezone.utc)

        def record(
            thread_id: str,
            days_ago: int,
            *,
            usage_count: int,
            last_usage_days_ago: int | None = None,
            raw_memory: str = "raw memory",
            rollout_summary: str | None = None,
        ) -> MemoryStageOneRecord:
            last_usage = (
                now - timedelta(days=last_usage_days_ago)
                if last_usage_days_ago is not None
                else None
            )
            return MemoryStageOneRecord(
                thread_id=thread_id,
                source_updated_at=now - timedelta(days=days_ago),
                raw_memory=raw_memory,
                rollout_summary=rollout_summary if rollout_summary is not None else f"summary {thread_id}",
                rollout_slug=None,
                rollout_path=Path(f"/tmp/{thread_id}.jsonl"),
                cwd=Path("/tmp/workspace"),
                usage_count=usage_count,
                last_usage=last_usage,
            )

        high_usage = record("b-high", 25, usage_count=4, last_usage_days_ago=20)
        newest = record("c-new", 2, usage_count=1)
        lower_usage = record("a-low", 1, usage_count=0)
        stale_used = record("d-stale-used", 1, usage_count=99, last_usage_days_ago=40)
        empty = record("e-empty", 1, usage_count=99, raw_memory="", rollout_summary="")

        selected = select_phase2_memory_inputs(
            [lower_usage, stale_used, high_usage, newest, empty],
            2,
            max_unused_days=30,
            now=now,
        )

        self.assertEqual([memory.thread_id for memory in selected], ["b-high", "c-new"])

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "memory"
            sync_rollout_summaries_from_memories(
                root,
                [lower_usage, stale_used, high_usage, newest, empty],
                2,
                max_unused_days=30,
                now=now,
            )
            rebuild_raw_memories_file_from_memories(
                root,
                [lower_usage, stale_used, high_usage, newest, empty],
                2,
                max_unused_days=30,
                now=now,
            )
            raw_memories = raw_memories_file(root).read_text(encoding="utf-8")
            self.assertIn("## Thread `b-high`", raw_memories)
            self.assertIn("## Thread `c-new`", raw_memories)
            self.assertNotIn("d-stale-used", raw_memories)
            self.assertNotIn("e-empty", raw_memories)

    def test_memory_stage1_retention_prunes_stale_unselected_records_by_batch(self) -> None:
        now = datetime(2026, 5, 15, tzinfo=timezone.utc)

        def record(
            thread_id: str,
            days_ago: int,
            *,
            selected_for_phase2: bool = False,
            last_usage_days_ago: int | None = None,
        ) -> MemoryStageOneRecord:
            last_usage = (
                now - timedelta(days=last_usage_days_ago)
                if last_usage_days_ago is not None
                else None
            )
            return MemoryStageOneRecord(
                thread_id=thread_id,
                source_updated_at=now - timedelta(days=days_ago),
                raw_memory=f"raw {thread_id}",
                rollout_summary=f"summary {thread_id}",
                rollout_slug=None,
                rollout_path=Path(f"/tmp/{thread_id}.jsonl"),
                cwd=Path("/tmp/workspace"),
                last_usage=last_usage,
                selected_for_phase2=selected_for_phase2,
            )

        stale_oldest = record("a-stale-oldest", 60)
        stale_selected = record("b-stale-selected", 80, selected_for_phase2=True)
        fresh_used = record("c-fresh-used", 90, last_usage_days_ago=2)
        stale_newer = record("d-stale-newer", 40)

        kept, pruned = prune_stage1_records_for_retention(
            [stale_newer, fresh_used, stale_selected, stale_oldest],
            max_unused_days=30,
            limit=1,
            now=now,
        )

        self.assertEqual([memory.thread_id for memory in pruned], ["a-stale-oldest"])
        self.assertEqual(
            sorted(memory.thread_id for memory in kept),
            ["b-stale-selected", "c-fresh-used", "d-stale-newer"],
        )

    def test_memory_state_store_stage1_claim_success_usage_and_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStateStore(Path(tmp) / "memory.sqlite3")
            now = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
            source_updated_at = now - timedelta(hours=2)
            store.upsert_thread(
                MemoryThreadRecord(
                    thread_id="thread-a",
                    rollout_path=Path("/tmp/thread-a.jsonl"),
                    cwd=Path("/tmp/workspace-a"),
                    updated_at=source_updated_at,
                    git_branch="main",
                )
            )

            claim = store.try_claim_stage1_job(
                thread_id="thread-a",
                worker_id="worker",
                source_updated_at=source_updated_at,
                lease_seconds=60,
                max_running_jobs=4,
                now=now,
            )
            self.assertEqual(claim.outcome, "claimed")
            assert claim.ownership_token is not None

            self.assertTrue(
                store.mark_stage1_job_succeeded(
                    thread_id="thread-a",
                    ownership_token=claim.ownership_token,
                    source_updated_at=source_updated_at,
                    raw_memory="remember alpha",
                    rollout_summary="summary alpha",
                    rollout_slug="alpha",
                    now=now + timedelta(seconds=1),
                )
            )
            phase2_job = store.get_job("memory_consolidate_global", "global")
            self.assertIsNotNone(phase2_job)
            assert phase2_job is not None
            self.assertEqual(phase2_job["status"], "pending")

            self.assertEqual(store.record_stage1_output_usage(["thread-a"], now=now + timedelta(seconds=2)), 1)
            selected = store.get_phase2_input_selection(n=1, max_unused_days=30, now=now + timedelta(seconds=3))
            self.assertEqual([memory.thread_id for memory in selected], ["thread-a"])
            self.assertEqual(selected[0].usage_count, 1)
            self.assertEqual(selected[0].last_usage, now + timedelta(seconds=2))
            self.assertEqual(selected[0].rollout_path, "/tmp/thread-a.jsonl")
            store.close()

    def test_memory_state_store_stage1_retry_backoff_and_advanced_source_reset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStateStore(Path(tmp) / "memory.sqlite3")
            now = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
            source_updated_at = now - timedelta(hours=2)
            store.upsert_thread(
                MemoryThreadRecord(
                    thread_id="thread-retry",
                    rollout_path=Path("/tmp/thread-retry.jsonl"),
                    cwd=Path("/tmp/workspace"),
                    updated_at=source_updated_at,
                )
            )
            claim = store.try_claim_stage1_job(
                thread_id="thread-retry",
                worker_id="worker",
                source_updated_at=source_updated_at,
                lease_seconds=60,
                max_running_jobs=4,
                now=now,
            )
            assert claim.ownership_token is not None
            self.assertTrue(
                store.mark_stage1_job_failed(
                    thread_id="thread-retry",
                    ownership_token=claim.ownership_token,
                    failure_reason="failed_extract",
                    retry_delay_seconds=600,
                    now=now + timedelta(seconds=1),
                )
            )

            retry = store.try_claim_stage1_job(
                thread_id="thread-retry",
                worker_id="worker",
                source_updated_at=source_updated_at,
                lease_seconds=60,
                max_running_jobs=4,
                now=now + timedelta(seconds=2),
            )
            self.assertEqual(retry.outcome, "skipped_retry_backoff")

            advanced = store.try_claim_stage1_job(
                thread_id="thread-retry",
                worker_id="worker",
                source_updated_at=source_updated_at + timedelta(seconds=1),
                lease_seconds=60,
                max_running_jobs=4,
                now=now + timedelta(seconds=3),
            )
            self.assertEqual(advanced.outcome, "claimed")
            store.close()

    def test_memory_state_store_phase2_claim_heartbeat_success_and_pollution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStateStore(Path(tmp) / "memory.sqlite3")
            now = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
            records = [
                ("thread-a", now - timedelta(hours=3), "raw a"),
                ("thread-b", now - timedelta(hours=2), "raw b"),
            ]
            for thread_id, source_updated_at, raw_memory in records:
                store.upsert_thread(
                    MemoryThreadRecord(
                        thread_id=thread_id,
                        rollout_path=Path(f"/tmp/{thread_id}.jsonl"),
                        cwd=Path("/tmp/workspace"),
                        updated_at=source_updated_at,
                    )
                )
                claim = store.try_claim_stage1_job(
                    thread_id=thread_id,
                    worker_id="worker",
                    source_updated_at=source_updated_at,
                    lease_seconds=60,
                    max_running_jobs=4,
                    now=now,
                )
                assert claim.ownership_token is not None
                self.assertTrue(
                    store.mark_stage1_job_succeeded(
                        thread_id=thread_id,
                        ownership_token=claim.ownership_token,
                        source_updated_at=source_updated_at,
                        raw_memory=raw_memory,
                        rollout_summary=f"summary {thread_id}",
                        rollout_slug=None,
                        now=now + timedelta(seconds=1),
                    )
                )

            selected = store.get_phase2_input_selection(n=2, max_unused_days=30, now=now + timedelta(seconds=2))
            phase2_claim = store.try_claim_global_phase2_job(
                worker_id="worker",
                lease_seconds=60,
                now=now + timedelta(seconds=3),
            )
            self.assertEqual(phase2_claim.outcome, "claimed")
            assert phase2_claim.ownership_token is not None
            self.assertTrue(
                store.heartbeat_global_phase2_job(
                    ownership_token=phase2_claim.ownership_token,
                    lease_seconds=60,
                    now=now + timedelta(seconds=4),
                )
            )
            self.assertTrue(
                store.mark_global_phase2_job_succeeded(
                    ownership_token=phase2_claim.ownership_token,
                    completed_watermark=max(record.source_updated_at for record in selected),
                    selected_outputs=[selected[0]],
                    now=now + timedelta(seconds=5),
                )
            )

            selected_row = store.get_stage1_output(selected[0].thread_id)
            unselected_row = store.get_stage1_output(selected[1].thread_id)
            assert selected_row is not None and unselected_row is not None
            self.assertEqual(selected_row["selected_for_phase2"], 1)
            self.assertEqual(unselected_row["selected_for_phase2"], 0)
            self.assertTrue(store.mark_thread_memory_mode_polluted(selected[0].thread_id, now=now + timedelta(seconds=6)))
            self.assertEqual(store.try_claim_global_phase2_job(worker_id="worker", lease_seconds=60, now=now + timedelta(seconds=7)).outcome, "skipped_cooldown")
            store.close()

    @unittest.skipUnless(shutil.which("git"), "git CLI is required for memory phase2 runner tests")
    def test_memory_phase2_once_runs_consolidation_and_marks_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            codex_home = Path(tmp) / "codex-home"
            store = MemoryStateStore(Path(tmp) / "memory.sqlite3")
            now = datetime.now(timezone.utc) - timedelta(hours=1)
            store.upsert_thread(
                MemoryThreadRecord(
                    thread_id="phase2-thread",
                    rollout_path=Path("/tmp/phase2-thread.jsonl"),
                    cwd=Path("/tmp/workspace"),
                    updated_at=now,
                )
            )
            claim = store.try_claim_stage1_job(
                thread_id="phase2-thread",
                worker_id="worker",
                source_updated_at=now,
                lease_seconds=60,
                max_running_jobs=4,
            )
            assert claim.ownership_token is not None
            store.mark_stage1_job_succeeded(
                thread_id="phase2-thread",
                ownership_token=claim.ownership_token,
                source_updated_at=now,
                raw_memory="phase2 raw memory",
                rollout_summary="phase2 summary",
                rollout_slug=None,
            )

            result = run_memory_phase2_once(
                codex_home=codex_home,
                state_store=store,
                base_config=CodexConfig(skip_git_repo_check=True, ephemeral=True),
                model_client=ScriptedResponsesModel([message("phase2 complete")]),
                max_unused_days=30,
            )

            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.final_message, "phase2 complete")
            self.assertIn("phase2 raw memory", raw_memories_file(result.memory_root).read_text(encoding="utf-8"))
            self.assertFalse((result.memory_root / "phase2_workspace_diff.md").exists())
            job = store.get_job("memory_consolidate_global", "global")
            assert job is not None
            self.assertEqual(job["status"], "done")
            stored = store.get_stage1_output("phase2-thread")
            assert stored is not None
            self.assertEqual(stored["selected_for_phase2"], 1)
            self.assertEqual(
                store.try_claim_global_phase2_job(worker_id="worker", lease_seconds=60).outcome,
                "skipped_cooldown",
            )
            store.close()

    def test_memory_phase2_prunes_old_extension_resources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            memory_root = Path(tmp) / "memories"
            extensions = memory_extensions_root(memory_root)
            resources = extensions / "chronicle" / "resources"
            resources.mkdir(parents=True)
            (extensions / "chronicle" / "instructions.md").write_text("instructions", encoding="utf-8")

            old_file = resources / "2026-04-06T11-59-59-abcd-10min-old.md"
            cutoff_file = resources / "2026-04-07T12-00-00-abcd-10min-cutoff.md"
            recent_file = resources / "2026-04-08T12-00-00-abcd-10min-recent.md"
            invalid_file = resources / "not-a-timestamp.md"
            for path in [old_file, cutoff_file, recent_file, invalid_file]:
                path.write_text("resource", encoding="utf-8")

            ignored_resources = extensions / "ignored" / "resources"
            ignored_resources.mkdir(parents=True)
            ignored_old_file = ignored_resources / "2026-04-06T11-59-59-abcd-10min-old.md"
            ignored_old_file.write_text("ignored", encoding="utf-8")

            now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
            prune_old_extension_resources(memory_root, now=now)

            self.assertFalse(old_file.exists())
            self.assertFalse(cutoff_file.exists())
            self.assertTrue(recent_file.exists())
            self.assertTrue(invalid_file.exists())
            self.assertTrue(ignored_old_file.exists())

    def test_memory_startup_seeds_ad_hoc_extension_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            memory_root = Path(tmp) / "memories"
            instructions_path = memory_extensions_root(memory_root) / "ad_hoc" / "instructions.md"

            seed_extension_instructions(memory_root)
            self.assertIn("Ad-hoc notes", instructions_path.read_text(encoding="utf-8"))

            instructions_path.write_text("custom instructions", encoding="utf-8")
            seed_extension_instructions(memory_root)
            self.assertEqual(instructions_path.read_text(encoding="utf-8"), "custom instructions")

    def test_memory_phase2_workspace_sync_writes_inputs_and_prunes_extensions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 5, 15, tzinfo=timezone.utc)
            memory_root = Path(tmp) / "memories"
            resources = memory_extensions_root(memory_root) / "ad_hoc" / "resources"
            resources.mkdir(parents=True)
            (resources.parent / "instructions.md").write_text("instructions", encoding="utf-8")
            stale_resource = resources / "2026-05-01T00-00-00-abcd-stale.md"
            stale_resource.write_text("old resource", encoding="utf-8")
            record = MemoryStageOneRecord(
                thread_id="sync-thread",
                source_updated_at=now,
                raw_memory="synced raw memory",
                rollout_summary="synced summary",
                rollout_slug=None,
                rollout_path=Path("/tmp/sync-thread.jsonl"),
                cwd=Path("/tmp/workspace"),
            )

            sync_phase2_workspace_inputs(
                memory_root,
                [record],
                1,
                max_unused_days=30,
                now=now,
            )

            self.assertIn("synced raw memory", raw_memories_file(memory_root).read_text(encoding="utf-8"))
            self.assertEqual(len(list(rollout_summaries_dir(memory_root).glob("*.md"))), 1)
            self.assertFalse(stale_resource.exists())

    def test_memory_workspace_diff_file_matches_upstream_render_shape(self) -> None:
        rendered = render_memory_workspace_diff_file(
            [MemoryWorkspaceChange(status="M", path="MEMORY.md")],
            "a" * (4 * 1024 * 1024 + 128),
        )
        self.assertIn("# Memory Workspace Diff", rendered)
        self.assertIn("- M MEMORY.md", rendered)
        self.assertIn("[workspace diff truncated at 4194304 bytes]", rendered)
        self.assertTrue(rendered.endswith("```\n"))

        with tempfile.TemporaryDirectory() as tmp:
            path = write_memory_workspace_diff(
                Path(tmp),
                [MemoryWorkspaceChange(status="A", path="raw_memories.md")],
                "+raw\n",
            )
            self.assertEqual(path.name, "phase2_workspace_diff.md")
            self.assertIn("- A raw_memories.md", path.read_text(encoding="utf-8"))

    @unittest.skipUnless(shutil.which("git"), "git CLI is required for memory workspace baseline tests")
    def test_memory_workspace_git_baseline_diff_and_reset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "memories"
            root.mkdir()
            (root / "MEMORY.md").write_text("baseline\n", encoding="utf-8")

            prepare_memory_workspace(root)
            changes, diff = memory_workspace_diff(root)
            self.assertEqual(changes, [])
            self.assertEqual(diff, "")

            (root / "MEMORY.md").write_text("changed\n", encoding="utf-8")
            (root / "raw_memories.md").write_text("new raw\n", encoding="utf-8")
            changes, diff = memory_workspace_diff(root)
            self.assertEqual([(change.status, change.path) for change in changes], [("M", "MEMORY.md"), ("A", "raw_memories.md")])
            self.assertIn("MEMORY.md", diff)
            self.assertIn("+new raw", diff)

            diff_path = write_current_memory_workspace_diff(root)
            self.assertEqual(diff_path.name, "phase2_workspace_diff.md")
            self.assertIn("- M MEMORY.md", diff_path.read_text(encoding="utf-8"))

            reset_memory_workspace_baseline(root)
            self.assertFalse(diff_path.exists())
            changes, diff = memory_workspace_diff(root)
            self.assertEqual(changes, [])
            self.assertEqual(diff, "")

    def test_memory_consolidation_session_uses_locked_down_agent_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            memory_root = Path(tmp) / "memories"
            memory_root.mkdir()
            config = build_memory_consolidation_config(
                memory_root=memory_root,
                base_config=CodexConfig(skip_git_repo_check=True, ephemeral=True),
            )

            self.assertEqual(config.model, "gpt-5.4")
            self.assertEqual(config.cwd, memory_root)
            self.assertEqual(config.session_source, "internal_memory_consolidation")
            self.assertEqual(config.approval_policy, "never")
            self.assertEqual(config.network_access, "restricted")
            self.assertEqual(config.writable_roots, (memory_root,))
            self.assertTrue(config.ephemeral)
            self.assertFalse(config.use_memories)
            self.assertFalse(config.memory_tool_enabled)
            self.assertFalse(config.memory_generate_memories)
            self.assertFalse(config.include_multi_agent_tools)
            self.assertFalse(config.include_web_search_tool)
            self.assertFalse(config.include_request_user_input_tool)
            self.assertEqual(config.model_reasoning_effort, "medium")

            model = ScriptedResponsesModel([message("consolidated")])
            result = run_memory_consolidation_session(
                memory_root=memory_root,
                base_config=CodexConfig(skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            )

            self.assertEqual(result.final_message, "consolidated")
            request = model.requests[0]
            self.assertEqual(request.model, "gpt-5.4")
            self.assertEqual(request.reasoning, {"effort": "medium"})
            self.assertEqual(request.verbosity, "low")
            request_text = "\n".join(part["text"] for item in request.input for part in item["content"])
            self.assertIn("Memory Writing Agent: Phase 2", request_text)
            tool_names = {tool.get("name", tool["type"]) for tool in request.tools}
            self.assertNotIn("web_search", tool_names)
            self.assertNotIn("spawn_agent", tool_names)

    def test_assistant_only_final_response(self) -> None:
        model = ScriptedResponsesModel([message("done")])
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        result = session.run("hello")
        self.assertEqual(result.final_message, "done")
        self.assertTrue(any(event.type == "turn.completed" for event in result.events))
        self.assertEqual(model.requests[0].prompt_cache_key, result.thread_id)
        self.assertEqual(model.requests[0].reasoning, {"effort": "medium"})
        self.assertEqual(model.requests[0].verbosity, "low")
        self.assertTrue(model.requests[0].parallel_tool_calls)

    def test_non_reasoning_model_does_not_default_reasoning(self) -> None:
        model = ScriptedResponsesModel([message("done")])
        session = CodexSession(
            CodexConfig(model="gpt-4.1", skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        session.run("hello")
        self.assertIsNone(model.requests[0].reasoning)
        self.assertEqual(model.requests[0].include, [])

    def test_assistant_memory_citation_is_hidden_and_recorded(self) -> None:
        model = ScriptedResponsesModel(
            [
                message(
                    "visible answer<oai-mem-citation><citation_entries>\n"
                    "MEMORY.md:1-3|note=[answer source]\n"
                    "</citation_entries>\n<rollout_ids>\n019cc2ea-1dff-7902-8d40-c8f6e5d83cc4\n"
                    "</rollout_ids></oai-mem-citation>"
                )
            ]
        )
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("use memory")
        self.assertEqual(result.final_message, "visible answer")
        self.assertNotIn("oai-mem-citation", result.history[-1]["content"][0]["text"])
        self.assertEqual(result.memory_citations[0]["entries"][0]["path"], "MEMORY.md")

    def test_assistant_memory_citation_updates_state_store_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            thread_id = "019cc2ea-1dff-7902-8d40-c8f6e5d83cc4"
            now = datetime(2026, 5, 15, tzinfo=timezone.utc)
            store = MemoryStateStore(Path(tmp) / "memory.sqlite3")
            store.upsert_thread(
                MemoryThreadRecord(
                    thread_id=thread_id,
                    rollout_path=Path("/tmp/thread.jsonl"),
                    cwd=Path("/tmp/workspace"),
                    updated_at=now,
                )
            )
            claim = store.try_claim_stage1_job(
                thread_id=thread_id,
                worker_id="worker",
                source_updated_at=now,
                lease_seconds=60,
                max_running_jobs=4,
                now=now,
            )
            assert claim.ownership_token is not None
            store.mark_stage1_job_succeeded(
                thread_id=thread_id,
                ownership_token=claim.ownership_token,
                source_updated_at=now,
                raw_memory="memory",
                rollout_summary="summary",
                rollout_slug=None,
                now=now,
            )
            model = ScriptedResponsesModel(
                [
                    message(
                        "visible<oai-mem-citation><thread_ids>\n"
                        f"{thread_id}\n"
                        "</thread_ids></oai-mem-citation>"
                    )
                ]
            )

            result = CodexSession(
                CodexConfig(skip_git_repo_check=True, ephemeral=True, memory_state_store=store),
                model_client=model,
            ).run("use memory")

            self.assertEqual(result.final_message, "visible")
            stored = store.get_stage1_output(thread_id)
            assert stored is not None
            self.assertEqual(stored["usage_count"], 1)
            self.assertIsNotNone(stored["last_usage"])
            store.close()

    def test_shell_tool_call_then_final_response(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "exec_command",
                            "call_id": "call-1",
                            "arguments": json.dumps({"cmd": "printf hi"}),
                        }
                    ]
                },
                message("saw tool output"),
            ]
        )
        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        )
        result = session.run("run a command")
        self.assertEqual(result.final_message, "saw tool output")
        self.assertTrue(any(item.get("type") == "function_call_output" for item in result.history))

    def test_parallel_tool_calls_dispatch_concurrently_and_drain_in_order(self) -> None:
        class SleepingToolRuntime:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def specs(self) -> list[dict[str, Any]]:
                return [{"type": "function", "name": "slow_a"}, {"type": "function", "name": "slow_b"}]

            def supports_parallel(self, name: str) -> bool:
                return name in {"slow_a", "slow_b"}

            def dispatch(self, name: str, arguments: Any, *, call_id: str | None = None) -> ToolResult:
                self.calls.append(name)
                time.sleep(0.35)
                return ToolResult(True, name, {"name": name, "call_id": call_id})

        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {"type": "function_call", "name": "slow_a", "call_id": "call-a", "arguments": "{}"},
                        {"type": "function_call", "name": "slow_b", "call_id": "call-b", "arguments": "{}"},
                    ]
                },
                message("done"),
            ]
        )
        session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True), model_client=model)
        session.tools = SleepingToolRuntime()  # type: ignore[assignment]

        started = time.monotonic()
        result = session.run("run both")
        elapsed = time.monotonic() - started

        self.assertEqual(result.final_message, "done")
        self.assertLess(elapsed, 0.65)
        output_items = [item for item in result.history if item.get("type") == "function_call_output"]
        self.assertEqual([item["call_id"] for item in output_items[-2:]], ["call-a", "call-b"])
        first_completed = next(index for index, event in enumerate(result.events) if event.type == "tool.completed")
        started_before_completion = [event for event in result.events[:first_completed] if event.type == "tool.started"]
        self.assertEqual([event.payload["call_id"] for event in started_before_completion[-2:]], ["call-a", "call-b"])

    def test_stream_events_are_covered_by_lifecycle_catalog(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "exec_command",
                            "call_id": "call-1",
                            "arguments": json.dumps({"cmd": "printf hi"}),
                        }
                    ]
                },
                message("done"),
            ]
        )
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("run a command")
        event_types = {event.type for event in result.events}
        self.assertTrue(event_types <= KNOWN_EVENT_TYPES, sorted(event_types - KNOWN_EVENT_TYPES))
        self.assertTrue(event_types & TERMINAL_TURN_EVENT_TYPES)

    def test_persistent_rollout_uses_upstream_jsonl_item_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel([message("persisted answer")])
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                    model_auto_compact_token_limit=100_000,
                ),
                model_client=model,
            ).run("persist this")

            matches = list((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            self.assertEqual(len(matches), 1)
            rollout_path = matches[0]
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            record_types = [record["type"] for record in records]

            self.assertEqual(record_types[0], "session_meta")
            self.assertRegex(
                str(rollout_path.relative_to(codex_home)),
                rf"^sessions/\d{{4}}/\d{{2}}/\d{{2}}/rollout-\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}-\d{{2}}-\d{{2}}-{result.thread_id}\.jsonl$",
            )
            self.assertTrue(set(record_types) <= CODEX_ROLLOUT_ITEM_TYPES)
            self.assertIn("turn_context", record_types)
            self.assertIn("response_item", record_types)
            self.assertIn("event_msg", record_types)
            self.assertNotIn("item.completed", record_types)
            session_meta = records[0]["payload"]["meta"]
            self.assertEqual(session_meta["id"], result.thread_id)
            self.assertEqual(session_meta["source"], "cli")
            self.assertEqual(session_meta["memory_mode"], "disabled")
            self.assertIn("base_instructions", session_meta)
            self.assertIn("You are Codex", session_meta["base_instructions"]["text"])
            turn_context = next(record["payload"] for record in records if record["type"] == "turn_context")
            self.assertEqual(turn_context["approval_policy"], "never")
            self.assertNotIn("collaboration_mode", turn_context)
            self.assertEqual(turn_context["summary"], "none")
            self.assertFalse(turn_context["realtime_active"])
            self.assertEqual(turn_context["sandbox_policy"]["writable_roots"], [])
            self.assertEqual(turn_context["permission_profile"]["type"], "managed")
            self.assertEqual(turn_context["permission_profile"]["network"], "restricted")
            self.assertEqual(turn_context["file_system_sandbox_policy"]["kind"], "restricted")
            self.assertEqual(turn_context["truncation_policy"], {"mode": "tokens", "limit": 100_000})

            event_msg_types = [record["payload"]["type"] for record in records if record["type"] == "event_msg"]
            self.assertIn("task_started", event_msg_types)
            turn_started = next(
                record["payload"] for record in records
                if record["type"] == "event_msg" and record["payload"]["type"] == "task_started"
            )
            self.assertEqual(turn_started["collaboration_mode_kind"], "default")
            self.assertIn("user_message", event_msg_types)
            self.assertIn("agent_message", event_msg_types)
            self.assertIn("task_complete", event_msg_types)

            rollout = load_memory_rollout(rollout_path)
            serialized = json.loads(rollout.serialized_contents)
            self.assertEqual([item["role"] for item in serialized], ["user", "user", "assistant"])
            self.assertIn("<environment_context>", serialized[0]["content"][0]["text"])
            self.assertEqual(rollout.thread_id, result.thread_id)
            reconstructed = reconstruct_history_from_rollout(rollout_path)
            self.assertEqual([item["role"] for item in reconstructed.history], ["developer", "user", "user", "assistant"])
            self.assertEqual(reconstructed.session_meta["id"], result.thread_id)
            self.assertEqual(reconstructed.previous_turn_settings["model"], CodexConfig().model)

    def test_persistent_rollout_records_token_count_event_msg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            response = message("counted answer")
            response["usage"] = {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3}

            result = CodexSession(
                CodexConfig(cwd=root, codex_home=codex_home, skip_git_repo_check=True, ephemeral=False),
                model_client=ScriptedResponsesModel([response]),
            ).run("persist token count")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            token_count = next(payload for payload in event_msgs if payload["type"] == "token_count")
            self.assertEqual(token_count["info"]["total_token_usage"], 3)
            self.assertEqual(token_count["info"]["last_token_usage"]["total_tokens"], 3)

    def test_resume_and_fork_from_rollout_seed_session_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            first_model = ScriptedResponsesModel([message("first answer")])
            first = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=first_model,
            ).run("first")
            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{first.thread_id}.jsonl"))

            resumed = CodexSession.resume_from_rollout(
                rollout_path,
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=ScriptedResponsesModel([message("second answer")]),
            )
            resumed_result = resumed.run("second")
            self.assertEqual(resumed_result.thread_id, first.thread_id)
            self.assertEqual(resumed_result.final_message, "second answer")
            resumed_records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(sum(1 for record in resumed_records if record["type"] == "session_meta"), 1)
            self.assertEqual(
                [item["content"][0]["text"] for item in resumed_result.history if item.get("role") == "assistant"],
                ["first answer", "second answer"],
            )

            forked = CodexSession.fork_from_rollout(
                rollout_path,
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=ScriptedResponsesModel([message("fork answer")]),
            )
            forked_result = forked.run("fork prompt")
            self.assertNotEqual(forked_result.thread_id, first.thread_id)
            fork_rollout = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{forked_result.thread_id}.jsonl"))
            fork_records = [json.loads(line) for line in fork_rollout.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(fork_records[0]["payload"]["meta"]["forked_from_id"], first.thread_id)
            self.assertEqual(forked_result.final_message, "fork answer")

    def test_persistent_rollout_records_exec_command_event_msgs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "call-1",
                                "arguments": json.dumps({"cmd": "printf hi"}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("run command")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            begin = next(payload for payload in event_msgs if payload["type"] == "exec_command_begin")
            end = next(payload for payload in event_msgs if payload["type"] == "exec_command_end")

            self.assertEqual(begin["call_id"], "call-1")
            self.assertEqual(begin["turn_id"], result.turn_id)
            self.assertEqual(begin["command"], ["printf hi"])
            self.assertEqual(begin["cwd"], str(root.resolve()))
            self.assertEqual(begin["parsed_cmd"], [{"type": "unknown", "cmd": "printf hi"}])
            self.assertEqual(begin["source"], "agent")
            self.assertEqual(end["status"], "completed")
            self.assertEqual(end["exit_code"], 0)
            self.assertIn("hi", end["stdout"])
            self.assertIn("hi", end["aggregated_output"])
            self.assertEqual(set(end["duration"]), {"secs", "nanos"})

    def test_persistent_rollout_records_write_stdin_terminal_interaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            command = (
                f"{sys.executable!r} -c "
                "\"import sys; print('ready', flush=True); print(sys.stdin.readline().strip(), flush=True)\""
            )
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "exec-1",
                                "arguments": json.dumps({"cmd": command, "tty": True, "yield_time_ms": 250}),
                            }
                        ]
                    },
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "write_stdin",
                                "call_id": "stdin-1",
                                "arguments": json.dumps({"session_id": 1, "chars": "hello\n", "yield_time_ms": 1500}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("run interactive command")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            interaction = next(payload for payload in event_msgs if payload["type"] == "terminal_interaction")
            end = [payload for payload in event_msgs if payload["type"] == "exec_command_end"][-1]

            self.assertEqual(interaction["call_id"], "exec-1")
            self.assertEqual(interaction["process_id"], "1")
            self.assertEqual(interaction["stdin"], "hello\n")
            self.assertEqual(end["call_id"], "exec-1")
            self.assertEqual(end["interaction_input"], "hello\n")
            self.assertIn("ready", end["aggregated_output"])
            self.assertIn("hello", end["aggregated_output"])

    def test_persistent_rollout_records_update_plan_event_msg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            plan = [
                {"step": "Inspect", "status": "completed"},
                {"step": "Patch", "status": "in_progress"},
            ]
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "update_plan",
                                "call_id": "plan-1",
                                "arguments": json.dumps({"explanation": "Working", "plan": plan}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("make a plan")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            plan_update = next(payload for payload in event_msgs if payload["type"] == "plan_update")

            self.assertEqual(plan_update["explanation"], "Working")
            self.assertEqual(plan_update["plan"], plan)

    def test_persistent_rollout_records_view_image_event_msg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "image.png"
            image_path.write_bytes(b"not really a png, but enough for the file handler")
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "view_image",
                                "call_id": "view-1",
                                "arguments": json.dumps({"path": "image.png"}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("view image")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            image_event = next(payload for payload in event_msgs if payload["type"] == "view_image_tool_call")

            self.assertEqual(image_event["call_id"], "view-1")
            self.assertEqual(image_event["path"], str(image_path.resolve()))

    def test_persistent_rollout_records_request_user_input_event_msg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            questions = [
                {
                    "id": "choice",
                    "header": "Choice",
                    "question": "Pick one.",
                    "options": [{"label": "A (Recommended)", "description": "Choose A."}],
                }
            ]
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "request_user_input",
                                "call_id": "input-1",
                                "arguments": json.dumps({"questions": questions}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                    collaboration_mode="Plan",
                    request_user_input_answers={"choice": {"answers": ["A"]}},
                ),
                model_client=model,
            ).run("ask")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            input_event = next(payload for payload in event_msgs if payload["type"] == "request_user_input")

            self.assertEqual(input_event["call_id"], "input-1")
            self.assertEqual(input_event["turn_id"], result.turn_id)
            self.assertEqual(input_event["questions"][0]["id"], "choice")
            self.assertTrue(input_event["questions"][0]["isOther"])

    def test_persistent_rollout_records_apply_patch_event_msgs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.txt").write_text("before\n", encoding="utf-8")
            codex_home = root / "codex-home"
            patch = """diff --git a/file.txt b/file.txt
--- a/file.txt
+++ b/file.txt
@@ -1 +1 @@
-before
+after
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "custom_tool_call",
                                "name": "apply_patch",
                                "call_id": "patch-1",
                                "input": patch,
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("patch")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            begin = next(payload for payload in event_msgs if payload["type"] == "patch_apply_begin")
            end = next(payload for payload in event_msgs if payload["type"] == "patch_apply_end")
            changes = {
                "file.txt": {
                    "type": "update",
                    "unified_diff": "@@ -1 +1 @@\n-before\n+after\n",
                    "move_path": None,
                }
            }

            self.assertEqual((root / "file.txt").read_text(encoding="utf-8"), "after\n")
            self.assertEqual(begin["call_id"], "patch-1")
            self.assertEqual(begin["turn_id"], result.turn_id)
            self.assertTrue(begin["auto_approved"])
            self.assertEqual(begin["changes"], changes)
            self.assertEqual(end["status"], "completed")
            self.assertTrue(end["success"])
            self.assertEqual(end["changes"], changes)

    def test_apply_patch_emits_turn_diff_for_committed_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.txt").write_text("before\n", encoding="utf-8")
            patch = """*** Begin Patch
*** Update File: file.txt
@@
-before
+after
*** End Patch
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "apply_patch",
                                "call_id": "patch-1",
                                "arguments": patch,
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            ).run("patch")

            turn_diffs = [event.payload["unified_diff"] for event in result.events if event.type == "turn_diff"]
            self.assertEqual(len(turn_diffs), 1)
            self.assertIn("diff --git a/file.txt b/file.txt", turn_diffs[0])
            self.assertIn("-before", turn_diffs[0])
            self.assertIn("+after", turn_diffs[0])

    def test_apply_patch_verification_failure_does_not_emit_committed_turn_diff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            patch = """*** Begin Patch
*** Add File: created.txt
+hello
*** Update File: missing.txt
@@
-old
+new
*** End Patch
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "apply_patch",
                                "call_id": "patch-1",
                                "arguments": patch,
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("patch")

            self.assertFalse((root / "created.txt").exists())
            completed = next(event for event in result.events if event.type == "tool.completed")
            self.assertFalse(completed.payload["ok"])
            self.assertIn("Failed to read file to update", completed.payload["output"])
            self.assertNotIn("partial_failure", completed.payload["metadata"])
            self.assertFalse(any(event.type == "turn_diff" for event in result.events))

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            self.assertFalse(any(payload["type"] == "patch_apply_begin" for payload in event_msgs))
            self.assertFalse(any(payload["type"] == "patch_apply_end" for payload in event_msgs))
            self.assertFalse(any(payload["type"] == "turn_diff" for payload in event_msgs))

    def test_exec_command_apply_patch_heredoc_is_normalized_to_apply_patch_tool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            command = """apply_patch <<'PATCH'
*** Begin Patch
*** Add File: created.txt
+hello
*** End Patch
PATCH
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "patch-1",
                                "arguments": json.dumps({"cmd": command}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            ).run("patch through shell")

            self.assertEqual((root / "created.txt").read_text(encoding="utf-8"), "hello\n")
            started = [event for event in result.events if event.type == "tool.started"]
            self.assertEqual([event.payload["name"] for event in started], ["apply_patch"])
            output_items = [item for item in result.history if item.get("type") == "function_call_output"]
            self.assertEqual(output_items[0]["call_id"], "patch-1")
            self.assertIn("Success. Updated the following files:", output_items[0]["output"])
            self.assertTrue(any(event.type == "turn_diff" for event in result.events))

    def test_exec_command_apply_patch_heredoc_with_cd_uses_effective_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sub").mkdir()
            command = """cd sub && apply_patch <<'PATCH'
*** Begin Patch
*** Add File: nested.txt
+inside
*** End Patch
PATCH
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "patch-1",
                                "arguments": json.dumps({"cmd": command}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            ).run("patch in subdir")

            self.assertEqual((root / "sub" / "nested.txt").read_text(encoding="utf-8"), "inside\n")
            begin = next(event for event in result.events if event.type == "tool.started")
            self.assertEqual(begin.payload["name"], "apply_patch")
            self.assertEqual(begin.payload["arguments"]["workdir"], str((root / "sub").resolve()))
            turn_diff = next(event.payload["unified_diff"] for event in result.events if event.type == "turn_diff")
            self.assertIn("diff --git a/nested.txt b/nested.txt", turn_diff)

    def test_exec_command_apply_patch_heredoc_requires_supported_shell(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            command = """apply_patch <<'PATCH'
*** Begin Patch
*** Add File: created.txt
+hello
*** End Patch
PATCH
"""
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "exec_command",
                                "call_id": "patch-1",
                                "arguments": json.dumps({"cmd": command, "shell": sys.executable}),
                            }
                        ]
                    },
                    message("done"),
                ]
            )
            result = CodexSession(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            ).run("patch through unsupported shell")

            self.assertFalse((root / "created.txt").exists())
            started = [event for event in result.events if event.type == "tool.started"]
            self.assertEqual([event.payload["name"] for event in started], ["exec_command"])
            self.assertFalse(any(event.type == "turn_diff" for event in result.events))

    def test_search_files_is_not_a_default_model_visible_tool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "note.txt").write_text("alpha\nneedle here\n", encoding="utf-8")
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "function_call",
                                "name": "search_files",
                                "call_id": "search-1",
                                "arguments": json.dumps({"query": "needle", "path": "."}),
                            }
                        ]
                    },
                    message("found needle"),
                ]
            )
            result = CodexSession(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True),
                model_client=model,
            ).run("search")
            outputs = [item for item in result.history if item.get("type") == "function_call_output"]
            self.assertEqual(result.final_message, "found needle")
            self.assertIn("unknown tool: search_files", outputs[0]["output"])

    def test_apply_patch_tool(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.txt").write_text("before\n", encoding="utf-8")
            patch = """diff --git a/file.txt b/file.txt
--- a/file.txt
+++ b/file.txt
@@ -1 +1 @@
-before
+after
"""
            runtime = ToolRuntime(CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True))
            result = runtime.apply_patch(patch)
            self.assertTrue(result.ok, result.output)
            self.assertEqual((root / "file.txt").read_text(encoding="utf-8"), "after\n")

    def test_apply_patch_tool_supports_codex_freeform_patch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.txt").write_text("before\n", encoding="utf-8")
            patch = """*** Begin Patch
*** Update File: file.txt
@@
-before
+after
*** Add File: added.txt
+new file
*** End Patch
"""
            runtime = ToolRuntime(CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True))
            result = runtime.apply_patch(patch)

            self.assertTrue(result.ok, result.output)
            self.assertIn("Success. Updated the following files:", result.output)
            self.assertEqual((root / "file.txt").read_text(encoding="utf-8"), "after\n")
            self.assertEqual((root / "added.txt").read_text(encoding="utf-8"), "new file\n")

    def test_apply_patch_freeform_matches_upstream_overwrite_move_and_verification_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "duplicate.txt").write_text("old content\n", encoding="utf-8")
            runtime = ToolRuntime(CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True))
            overwrite = runtime.apply_patch(
                "*** Begin Patch\n*** Add File: duplicate.txt\n+new content\n*** End Patch"
            )
            self.assertTrue(overwrite.ok, overwrite.output)
            self.assertEqual(overwrite.output, "Success. Updated the following files:\nA duplicate.txt\n")
            self.assertEqual((root / "duplicate.txt").read_text(encoding="utf-8"), "new content\n")

            (root / "old").mkdir()
            (root / "renamed").mkdir()
            (root / "old" / "name.txt").write_text("from\n", encoding="utf-8")
            (root / "renamed" / "name.txt").write_text("existing\n", encoding="utf-8")
            moved = runtime.apply_patch(
                "*** Begin Patch\n*** Update File: old/name.txt\n*** Move to: renamed/name.txt\n@@\n-from\n+new\n*** End Patch"
            )
            self.assertTrue(moved.ok, moved.output)
            self.assertEqual(moved.output, "Success. Updated the following files:\nM renamed/name.txt\n")
            self.assertFalse((root / "old" / "name.txt").exists())
            self.assertEqual((root / "renamed" / "name.txt").read_text(encoding="utf-8"), "new\n")

            verification_failure = runtime.apply_patch(
                "*** Begin Patch\n*** Add File: created.txt\n+hello\n*** Update File: missing.txt\n@@\n-old\n+new\n*** End Patch"
            )
            self.assertFalse(verification_failure.ok)
            self.assertFalse((root / "created.txt").exists())
            self.assertIn("Failed to read file to update", verification_failure.output)
            self.assertNotIn("partial_failure", verification_failure.metadata)

            empty = runtime.apply_patch("*** Begin Patch\n*** Update File: duplicate.txt\n*** End Patch")
            self.assertFalse(empty.ok)
            self.assertIn("Update file hunk for path 'duplicate.txt' is empty", empty.output)

    def test_apply_patch_freeform_matches_upstream_fixture_scenarios(self) -> None:
        upstream_env = os.environ.get("CODEX_UPSTREAM_DIR")
        if upstream_env:
            upstream_root = Path(upstream_env).expanduser()
        else:
            upstream_root = (
                Path(__file__).resolve().parents[1]
                / "agents"
                / "codex"
                / "upstream"
                / "openai-codex"
            )
        scenarios = upstream_root / "codex-rs" / "apply-patch" / "tests" / "fixtures" / "scenarios"
        expected_failures = {
            "005_rejects_empty_patch",
            "006_rejects_missing_context",
            "007_rejects_missing_file_delete",
            "008_rejects_empty_update_hunk",
            "009_requires_existing_file_for_update",
            "012_delete_directory_fails",
            "013_rejects_invalid_hunk_header",
        }

        def snapshot(root: Path) -> dict[str, bytes]:
            if not root.exists():
                return {}
            return {
                str(path.relative_to(root)): path.read_bytes()
                for path in sorted(root.rglob("*"))
                if path.is_file()
            }

        for scenario in sorted(path for path in scenarios.iterdir() if path.is_dir()):
            if scenario.name == "015_failure_after_partial_success_leaves_changes":
                continue
            with self.subTest(scenario=scenario.name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                input_dir = scenario / "input"
                if input_dir.exists():
                    shutil.copytree(input_dir, root, dirs_exist_ok=True)
                runtime = ToolRuntime(CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True))
                result = runtime.apply_patch((scenario / "patch.txt").read_text(encoding="utf-8"))

                if scenario.name in expected_failures:
                    self.assertFalse(result.ok, result.output)
                else:
                    self.assertTrue(result.ok, result.output)
                self.assertEqual(snapshot(root), snapshot(scenario / "expected"), result.output)

    def test_apply_patch_freeform_accepts_upstream_lenient_heredoc_argument(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.txt").write_text("before\n", encoding="utf-8")
            runtime = ToolRuntime(CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True))
            result = runtime.apply_patch(
                "<<'EOF'\n"
                "*** Begin Patch\n"
                "*** Update File: file.txt\n"
                "@@\n"
                "-before\n"
                "+after\n"
                "*** End Patch\n"
                "EOF\n"
            )

            self.assertTrue(result.ok, result.output)
            self.assertEqual((root / "file.txt").read_text(encoding="utf-8"), "after\n")

    def test_apply_patch_denies_workspace_write_escape_for_git_diff(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            root.mkdir()
            outside = Path(tmp) / "outside.txt"
            patch = """diff --git a/../outside.txt b/../outside.txt
new file mode 100644
--- /dev/null
+++ b/../outside.txt
@@ -0,0 +1 @@
+outside
"""
            runtime = ToolRuntime(
                CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True, sandbox="workspace-write")
            )
            result = runtime.apply_patch(patch)

            self.assertFalse(result.ok)
            self.assertIn("path escapes writable workspace", result.output)
            self.assertFalse(outside.exists())
            self.assertTrue(result.metadata["denied"])

    def test_apply_patch_allows_configured_writable_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            allowed = Path(tmp) / "allowed"
            root.mkdir()
            allowed.mkdir()
            runtime = ToolRuntime(
                CodexConfig(
                    cwd=root,
                    writable_roots=(allowed,),
                    skip_git_repo_check=True,
                    ephemeral=True,
                    sandbox="workspace-write",
                )
            )
            result = runtime.apply_patch(
                "*** Begin Patch\n*** Add File: ../allowed/created.txt\n+ok\n*** End Patch"
            )

            self.assertTrue(result.ok, result.output)
            self.assertEqual((allowed / "created.txt").read_text(encoding="utf-8"), "ok\n")

    def test_apply_patch_requests_approval_for_workspace_write_escape_and_caches_session_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            outside = Path(tmp) / "outside"
            root.mkdir()
            outside.mkdir()
            approvals: list[dict] = []

            def approve(request: dict) -> dict:
                approvals.append(request)
                return {"approved": True, "approved_for_session": True}

            runtime = ToolRuntime(
                CodexConfig(
                    cwd=root,
                    skip_git_repo_check=True,
                    ephemeral=True,
                    sandbox="workspace-write",
                    approval_policy="on-request",
                    approval_provider=approve,
                )
            )
            added = runtime.apply_patch(
                "*** Begin Patch\n*** Add File: ../outside/created.txt\n+hello\n*** End Patch"
            )
            updated = runtime.apply_patch(
                "*** Begin Patch\n*** Update File: ../outside/created.txt\n@@\n-hello\n+bonjour\n*** End Patch"
            )

            self.assertTrue(added.ok, added.output)
            self.assertTrue(updated.ok, updated.output)
            self.assertEqual((outside / "created.txt").read_text(encoding="utf-8"), "bonjour\n")
            self.assertEqual(len(approvals), 1)
            self.assertEqual(approvals[0]["tool"], "apply_patch")
            self.assertEqual(approvals[0]["files"], ["../outside/created.txt"])
            self.assertEqual(added.metadata["approval"], "approved_without_sandbox")

    def test_view_image_uses_upstream_model_catalog_for_detail_capability(self) -> None:
        gpt52 = next(
            spec for spec in ToolRuntime(CodexConfig(model="gpt-5.2")).specs() if spec.get("name") == "view_image"
        )
        gpt54 = next(
            spec for spec in ToolRuntime(CodexConfig(model="gpt-5.4")).specs() if spec.get("name") == "view_image"
        )

        self.assertNotIn("detail", gpt52["parameters"]["properties"])
        self.assertIn("detail", gpt54["parameters"]["properties"])

        runtime = ToolRuntime(CodexConfig(model_supports_image_input=False, skip_git_repo_check=True, ephemeral=True))
        result = runtime.view_image({"path": "missing.png"})
        self.assertFalse(result.ok)
        self.assertIn("you do not support image inputs", result.output)

    def test_view_image_resizes_and_respects_original_detail_capability(self) -> None:
        try:
            from PIL import Image
        except ImportError:
            self.skipTest("Pillow is required for image processing parity tests")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            image_path = root / "large.png"
            Image.new("RGBA", (3000, 1000), (10, 20, 30, 255)).save(image_path)

            runtime = ToolRuntime(CodexConfig(model="gpt-5.2", cwd=root, skip_git_repo_check=True, ephemeral=True))
            resized = runtime.view_image({"path": "large.png"})
            self.assertTrue(resized.ok, resized.output)
            self.assertEqual(json.loads(resized.output)["detail"], "high")
            self.assertLessEqual(resized.metadata["width"], 2048)
            self.assertTrue(resized.metadata["resized"])

            downgraded = runtime.view_image({"path": "large.png", "detail": "original"})
            self.assertTrue(downgraded.ok, downgraded.output)
            self.assertEqual(json.loads(downgraded.output)["detail"], "high")
            self.assertTrue(downgraded.metadata["resized"])

            original_runtime = ToolRuntime(
                CodexConfig(
                    cwd=root,
                    skip_git_repo_check=True,
                    ephemeral=True,
                    model_supports_image_detail_original=True,
                )
            )
            original = original_runtime.view_image({"path": "large.png", "detail": "original"})
            self.assertTrue(original.ok, original.output)
            self.assertEqual(json.loads(original.output)["detail"], "original")
            data_url = original.metadata["image_url"]
            image_bytes = base64.b64decode(data_url.split(",", 1)[1])
            with Image.open(io.BytesIO(image_bytes)) as decoded:
                self.assertEqual(decoded.size, (3000, 1000))

    def test_long_running_exec_and_write_stdin(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        command = (
            f"{sys.executable!r} -c "
            "\"import sys; print('ready', flush=True); print(sys.stdin.readline().strip(), flush=True)\""
        )
        first = runtime.exec_command({"cmd": command, "tty": True, "yield_time_ms": 100})
        payload = first.metadata
        self.assertIn("session_id", payload)
        self.assertIn("Process running with session ID", first.output)
        second = runtime.write_stdin({"session_id": payload["session_id"], "chars": "hello\n", "yield_time_ms": 1500})
        self.assertIn("hello", second.output)
        self.assertIn("ready", second.metadata["aggregated_output"])

    def test_exec_command_captures_partial_output_without_newline(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        command = (
            f"{sys.executable!r} -c "
            "\"import sys,time; sys.stdout.write('partial'); sys.stdout.flush(); time.sleep(0.4)\""
        )
        first = runtime.exec_command({"cmd": command, "yield_time_ms": 250})
        self.assertIn("partial", first.output)
        self.assertIn("session_id", first.metadata)

        final = runtime.write_stdin({"session_id": first.metadata["session_id"], "yield_time_ms": 250})
        self.assertEqual(final.metadata["exit_code"], 0)

    @unittest.skipIf(sys.platform == "win32", "PTY semantics are not used by the Windows shell_command path")
    def test_exec_command_tty_allocates_terminal(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        command = (
            f"{sys.executable!r} -c "
            "\"import os,sys; print(str(os.isatty(0))+' '+str(os.isatty(1)), flush=True); "
            "print('got:'+sys.stdin.readline().strip(), flush=True)\""
        )
        first = runtime.exec_command({"cmd": command, "tty": True, "yield_time_ms": 1000})
        self.assertIn("True True", first.output)

        second = runtime.write_stdin({"session_id": first.metadata["session_id"], "chars": "hello\n", "yield_time_ms": 1500})
        self.assertIn("got:hello", second.output)

    @unittest.skipIf(sys.platform == "win32", "PTY semantics are not used by the Windows shell_command path")
    def test_exec_command_tty_handles_sleep_then_input(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        command = (
            f"{sys.executable!r} -c "
            "\"import time; print('program started; sleeping 0.2s...', flush=True); "
            "time.sleep(0.2); text = input('please input a sentence: '); "
            "print('completed; got:', text, flush=True)\""
        )
        first = runtime.exec_command({"cmd": command, "tty": True, "yield_time_ms": 100})
        self.assertIn("session_id", first.metadata)

        second = runtime.write_stdin(
            {
                "session_id": first.metadata["session_id"],
                "chars": "这是一句从 Codex 写入的测试输入。\n",
                "yield_time_ms": 1500,
            }
        )

        self.assertIn("please input a sentence:", second.output)
        self.assertIn("completed; got: 这是一句从 Codex 写入的测试输入。", second.output)

    def test_write_stdin_requires_tty_for_non_empty_input(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True))
        command = f"{sys.executable!r} -c \"import time; time.sleep(1)\""
        first = runtime.exec_command({"cmd": command, "yield_time_ms": 250})
        result = runtime.write_stdin({"session_id": first.metadata["session_id"], "chars": "hello\n"})

        self.assertFalse(result.ok)
        self.assertIn("stdin is closed", result.output)

    def test_exec_command_rejects_escalation_without_on_request_approval(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True, approval_policy="never"))
        result = runtime.exec_command({"cmd": "echo no", "sandbox_permissions": "require_escalated"})

        self.assertFalse(result.ok)
        self.assertIn("cannot ask for escalated permissions", result.output)

    def test_exec_command_records_platform_sandbox_metadata(self) -> None:
        runtime = ToolRuntime(CodexConfig(skip_git_repo_check=True, ephemeral=True, sandbox="workspace-write"))
        result = runtime.exec_command({"cmd": f"{sys.executable!r} -c \"print('sandbox-meta')\""})

        self.assertTrue(result.ok, result.output)
        self.assertIn("sandbox-meta", result.output)
        self.assertEqual(result.metadata["sandbox_policy"], "workspace-write")
        self.assertIn("sandbox_enforced", result.metadata)
        if sys.platform == "darwin" and not codex_tools._platform_sandbox_available():
            self.assertTrue(result.metadata["sandbox_unavailable"])

    @unittest.skipIf(sys.platform != "darwin", "macOS seatbelt argv is Darwin-specific")
    def test_macos_sandbox_argv_wraps_command_with_seatbelt_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            writable = root / "writable-extra"
            writable.mkdir()
            previous = codex_tools._PLATFORM_SANDBOX_AVAILABLE_CACHE
            codex_tools._PLATFORM_SANDBOX_AVAILABLE_CACHE = True
            try:
                sandboxed = codex_tools._sandboxed_process_argv(
                    ["/bin/echo", "ok"],
                    config=CodexConfig(
                        cwd=root,
                        skip_git_repo_check=True,
                        ephemeral=True,
                        sandbox="workspace-write",
                        writable_roots=(writable,),
                    ),
                    cwd=root,
                    workdir=root,
                    bypass_sandbox=False,
                )
            finally:
                codex_tools._PLATFORM_SANDBOX_AVAILABLE_CACHE = previous

            self.assertEqual(sandboxed.argv[:2], [codex_tools.MACOS_SANDBOX_EXEC, "-p"])
            self.assertEqual(sandboxed.argv[-2:], ["/bin/echo", "ok"])
            policy = sandboxed.argv[2]
            self.assertIn("(deny default)", policy)
            self.assertIn("(allow file-read*)", policy)
            self.assertIn(str(root), policy)
            self.assertIn(str(writable), policy)
            self.assertTrue(sandboxed.metadata["sandbox_enforced"])

    def test_escalated_exec_bypasses_platform_sandbox_after_approval(self) -> None:
        approvals: list[dict] = []

        def approve(request: dict) -> dict:
            approvals.append(request)
            return {"approved": True}

        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                sandbox="workspace-write",
                approval_policy="on-request",
                approval_provider=approve,
            )
        )
        result = runtime.exec_command(
            {
                "cmd": f"{sys.executable!r} -c \"print('unsandboxed-approved')\"",
                "sandbox_permissions": "require_escalated",
                "justification": "Need unsandboxed execution",
            }
        )

        self.assertTrue(result.ok, result.output)
        self.assertIn("unsandboxed-approved", result.output)
        self.assertTrue(result.metadata["sandbox_bypassed"])
        self.assertFalse(result.metadata["sandbox_enforced"])
        self.assertEqual(approvals[0]["sandbox_permissions"], "require_escalated")

    def test_exec_command_uses_approval_provider_for_escalated_request(self) -> None:
        approvals: list[dict] = []

        def approve(request: dict) -> dict:
            approvals.append(request)
            return {"approved": True}

        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                approval_policy="on-request",
                approval_provider=approve,
            )
        )
        result = runtime.exec_command(
            {
                "cmd": f"{sys.executable!r} -c \"print('approved')\"",
                "sandbox_permissions": "require_escalated",
                "justification": "Need to run a test command",
                "prefix_rule": [sys.executable],
            }
        )

        self.assertTrue(result.ok)
        self.assertIn("approved", result.output)
        self.assertEqual(approvals[0]["tool"], "exec_command")
        self.assertEqual(approvals[0]["sandbox_permissions"], "require_escalated")
        self.assertEqual(approvals[0]["justification"], "Need to run a test command")

    def test_exec_command_uses_permission_request_hook_for_escalated_request(self) -> None:
        hooks: list[dict] = []

        def hook_provider(request: dict) -> dict:
            hooks.append(request)
            if request["event"] == "permission_request":
                return {"decision": "approved_for_session"}
            return {}

        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                approval_policy="on-request",
                hook_provider=hook_provider,
            )
        )
        result = runtime.exec_command(
            {
                "cmd": f"{sys.executable!r} -c \"print('hook-approved')\"",
                "sandbox_permissions": "require_escalated",
                "justification": "Need hook approval",
            }
        )

        self.assertTrue(result.ok)
        self.assertIn("hook-approved", result.output)
        self.assertEqual(hooks[0]["event"], "permission_request")
        self.assertEqual(hooks[0]["tool_name"], "Bash")
        self.assertEqual(hooks[0]["tool_input"]["tool"], "exec_command")

    def test_exec_command_stops_when_approval_provider_denies_escalation(self) -> None:
        runtime = ToolRuntime(
            CodexConfig(
                skip_git_repo_check=True,
                ephemeral=True,
                approval_policy="on-request",
                approval_provider=lambda _request: False,
            )
        )
        result = runtime.exec_command({"cmd": "echo denied", "sandbox_permissions": "require_escalated"})

        self.assertFalse(result.ok)
        self.assertIn("approval denied", result.output)

    def test_exec_command_on_failure_retries_likely_sandbox_denial_after_approval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approvals: list[dict] = []

            def approve(request: dict) -> dict:
                approvals.append(request)
                return {"approved": True}

            script = (
                "from pathlib import Path\n"
                "marker = Path('retry-marker')\n"
                "if not marker.exists():\n"
                "    marker.write_text('seen', encoding='utf-8')\n"
                "    print('sandbox denied: Operation not permitted')\n"
                "    raise SystemExit(1)\n"
                "print('retry-ok')\n"
            )
            runtime = ToolRuntime(
                CodexConfig(
                    cwd=root,
                    skip_git_repo_check=True,
                    ephemeral=True,
                    approval_policy="on-failure",
                    approval_provider=approve,
                )
            )
            result = runtime.exec_command({"cmd": f"{sys.executable!r} -c {script!r}"})

            self.assertTrue(result.ok, result.output)
            self.assertIn("retry-ok", result.output)
            self.assertTrue(result.metadata["retry_without_sandbox"])
            self.assertEqual(approvals[0]["tool"], "exec_command")
            self.assertTrue(approvals[0]["retry_without_sandbox"])

    def test_exec_command_on_request_requires_default_sandbox_approval_provider(self) -> None:
        runtime = ToolRuntime(
            CodexConfig(skip_git_repo_check=True, ephemeral=True, approval_policy="on-request")
        )
        result = runtime.exec_command({"cmd": "echo needs approval"})

        self.assertFalse(result.ok)
        self.assertIn("approval required for sandboxed execution", result.output)

    def test_tool_error_is_returned_to_model(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "exec_command",
                            "call_id": "bad",
                            "arguments": json.dumps({"cmd": "exit 7"}),
                        }
                    ]
                },
                message("handled failure"),
            ]
        )
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("fail")
        outputs = [item for item in result.history if item.get("type") == "function_call_output"]
        self.assertTrue(outputs)
        self.assertIn("Process exited with code 7", outputs[0]["output"])
        self.assertEqual(result.final_message, "handled failure")

    def test_web_search_call_is_recorded_without_local_dispatch(self) -> None:
        model = ScriptedResponsesModel(
            [
                {
                    "output": [
                        {
                            "type": "web_search_call",
                            "id": "ws-1",
                            "status": "completed",
                            "action": {"type": "search", "query": "Codex"},
                        },
                        {
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": "searched"}],
                        },
                    ]
                }
            ]
        )
        result = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=model,
        ).run("search the web")
        self.assertEqual(result.final_message, "searched")
        self.assertTrue(any(item.get("type") == "web_search_call" for item in result.history))
        self.assertFalse(any(event.type == "tool.started" for event in result.events))

    def test_external_context_marks_memory_mode_polluted_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "web_search_call",
                                "id": "ws-1",
                                "status": "completed",
                                "action": {"type": "search", "query": "Codex"},
                            },
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "searched"}],
                            },
                        ]
                    }
                ]
            )
            session = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    memory_tool_enabled=True,
                    memory_disable_on_external_context=True,
                    memory_startup_background=False,
                    memory_run_phase2_on_startup=False,
                ),
                model_client=model,
            )

            result = session.run("search the web")

            self.assertEqual(result.final_message, "searched")
            store = session.config.memory_state_store
            self.assertIsNotNone(store)
            assert store is not None
            row = store.conn.execute("SELECT memory_mode FROM threads WHERE id = ?", (result.thread_id,)).fetchone()
            self.assertEqual(row["memory_mode"], "polluted")
            store.close()

    def test_persistent_rollout_records_web_search_event_msg(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            model = ScriptedResponsesModel(
                [
                    {
                        "output": [
                            {
                                "type": "web_search_call",
                                "id": "ws-1",
                                "status": "completed",
                                "action": {"type": "search", "query": "Codex"},
                            },
                            {
                                "type": "message",
                                "role": "assistant",
                                "content": [{"type": "output_text", "text": "searched"}],
                            },
                        ]
                    }
                ]
            )
            result = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=model,
            ).run("search the web")

            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{result.thread_id}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            event_msgs = [record["payload"] for record in records if record["type"] == "event_msg"]
            web_search = next(payload for payload in event_msgs if payload["type"] == "web_search_end")

            self.assertEqual(web_search["call_id"], "ws-1")
            self.assertEqual(web_search["query"], "Codex")
            self.assertEqual(web_search["action"], {"type": "search", "query": "Codex"})

    def test_cli_json_and_output_last_message_with_fake_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            last = Path(tmp) / "last.txt"
            env = {
                **os.environ,
                "PY_CODEX_FAKE_RESPONSES": json.dumps([message("cli done")]),
                "PYTHONPATH": os.getcwd(),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--json",
                    "--skip-git-repo-check",
                    "--ephemeral",
                    "--output-last-message",
                    str(last),
                    "hello",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(last.read_text(encoding="utf-8"), "cli done")
            events = [json.loads(line) for line in completed.stdout.splitlines()]
            self.assertTrue(any(event["type"] == "turn.completed" for event in events))

    def test_cli_human_output_renders_tool_progress_with_fake_model(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf hello", "yield_time_ms": 250}),
                    }
                ]
            },
            message("human done"),
        ]
        env = {
            **os.environ,
            "COLUMNS": "64",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "human done")
        self.assertIn("• Ran printf hello", completed.stderr)
        self.assertIn("  └ hello", completed.stderr)
        self.assertIn("hello", completed.stderr)

    def test_cli_human_output_supports_color_always(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf hello", "yield_time_ms": 250}),
                    }
                ]
            },
            message("color done"),
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--color",
                "always",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("\033[32m•\033[0m \033[1mRan\033[0m printf hello", completed.stderr)
        self.assertEqual(completed.stdout.strip(), "color done")

    def test_cli_human_output_wraps_long_command_with_official_gutter(self) -> None:
        long_command = "printf " + ("abcdef" * 12)
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": long_command, "yield_time_ms": 250}),
                    }
                ]
            },
            message("wrapped command done"),
        ]
        env = {
            **os.environ,
            "COLUMNS": "44",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("• Ran printf", completed.stderr)
        self.assertIn("  │ ", completed.stderr)
        self.assertIn("  └ ", completed.stderr)

    def test_cli_human_output_separates_tool_cells(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf one", "yield_time_ms": 250}),
                    }
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-2",
                        "arguments": json.dumps({"cmd": "printf two", "yield_time_ms": 250}),
                    }
                ]
            },
            message("done"),
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("• Ran printf one\n  └ one\n\n• Ran printf two", completed.stderr)

    def test_cli_human_output_renders_markdown_and_work_separator(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "exec-1",
                        "arguments": json.dumps({"cmd": "printf hello", "yield_time_ms": 250}),
                    }
                ]
            },
            message("## 结果\n这是 **重点** 和 `code`。"),
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--color",
                "always",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("─" * 20, completed.stderr)
        self.assertNotIn("**重点**", completed.stderr)
        self.assertNotIn("`code`", completed.stderr)
        self.assertIn("\033[1m重点\033[0m", completed.stderr)
        self.assertIn("\033[36mcode\033[0m", completed.stderr)
        self.assertIn("\033[1m结果\033[0m", completed.stderr)

    def test_cli_markdown_code_fences_use_syntax_highlighting_and_info_token(self) -> None:
        import codex.cli as cli
        from codex.cli import _ANSI_RE
        from codex.cli import _AnsiStyle
        from codex.cli import _render_markdown_for_terminal

        old_theme = cli._CLI_SYNTAX_THEME
        try:
            self.assertTrue(cli._set_cli_syntax_theme("monokai-extended"))
            self.assertEqual(cli._pygments_style_name(), "monokai")
            rendered = _render_markdown_for_terminal(
                "```python title=\"demo\"\ndef hello():\n    return 'ok'\n```",
                _AnsiStyle(True),
                terminal_width=80,
            )
            joined = "\n".join(rendered)
            plain = _ANSI_RE.sub("", joined)
        finally:
            cli._CLI_SYNTAX_THEME = old_theme

        self.assertNotIn("```", plain)
        self.assertIn("def hello", plain)
        self.assertIn("return 'ok'", plain)
        self.assertRegex(joined, r"\x1b\[[0-9;]*m")

    def test_cli_ansi_wrapping_preserves_color_boundaries(self) -> None:
        from codex.cli import _ANSI_RE, _visible_len, _wrap_ansi_line

        wrapped = _wrap_ansi_line("\033[31m" + ("x" * 24) + "\033[0m", 8)

        self.assertEqual([_visible_len(line) for line in wrapped], [8, 8, 8])
        self.assertTrue(all(line.startswith("\033[31m") for line in wrapped))
        self.assertTrue(all(line.endswith("\033[0m") for line in wrapped))
        self.assertEqual(_ANSI_RE.sub("", "".join(wrapped)), "x" * 24)

    def test_cli_human_output_preserves_intraword_underscores(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("Use snake_case and temp_print_test.py in markdown.")]),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("snake_case", completed.stderr)
        self.assertIn("temp_print_test.py", completed.stderr)
        self.assertNotIn("snakecase", completed.stderr)
        self.assertNotIn("tempprinttest.py", completed.stderr)

    def test_cli_human_output_indents_multiline_agent_message(self) -> None:
        responses = [
            message(
                "第一行\n\n"
                "第二段第一行很长很长很长很长很长很长很长很长很长很长很长很长很长。\n"
                "- 列表项"
            )
        ]
        env = {
            **os.environ,
            "COLUMNS": "48",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        rendered_lines = [line for line in completed.stderr.splitlines() if line.strip()]
        self.assertTrue(rendered_lines[0].startswith("• "), completed.stderr)
        self.assertTrue(all(line.startswith(("• ", "  ")) for line in rendered_lines), completed.stderr)

    def test_cli_human_output_renders_markdown_tables(self) -> None:
        responses = [
            message(
                "| 模块 | 状态 |\n"
                "| --- | --- |\n"
                "| core | done |\n"
                "| tools | partial |"
            )
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("• ┌", completed.stderr)
        self.assertIn("│ 模块", completed.stderr)
        self.assertIn("│ core", completed.stderr)
        self.assertIn("│ tools", completed.stderr)
        self.assertIn("└", completed.stderr)
        self.assertNotIn("| --- | --- |", completed.stderr)

    def test_cli_human_output_wraps_long_markdown_table_cells(self) -> None:
        responses = [
            message(
                "| 模块 | 说明 |\n"
                "| --- | --- |\n"
                "| core | 这一列是一段很长很长的中文说明，用来验证表格单元格会在盒线内换行。 |\n"
                "| tools | short |"
            )
        ]
        env = {
            **os.environ,
            "COLUMNS": "46",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        table_lines = [line for line in completed.stderr.splitlines() if "│" in line or "┌" in line or "└" in line]
        self.assertGreaterEqual(len(table_lines), 6, completed.stderr)
        self.assertTrue(all(line.startswith(("• ", "  ")) for line in table_lines), completed.stderr)
        self.assertIn("验证表格", completed.stderr)

    def test_cli_human_output_renders_reasoning_as_indented_cell(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "reasoning",
                        "summary": [
                            {
                                "text": (
                                    "**Inspecting memory and commands**\n\n"
                                    "I think I need to inspect the state.\n"
                                    "Maybe I should run a command."
                                )
                            }
                        ],
                    },
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "done"}],
                    },
                ]
            }
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("• Inspecting memory and commands: I think I need to inspect the state.", completed.stderr)
        self.assertIn("  Maybe I should run a command.", completed.stderr)
        self.assertNotIn("**Inspecting memory and commands**", completed.stderr)

    def test_cli_human_output_wraps_text_with_continuation_indent(self) -> None:
        long_text = (
            "**Inspecting memory and commands**\n\n"
            "I need to inspect the state of prompts in memory and then compare command output "
            "carefully so the rendered transcript keeps every continuation line aligned."
        )
        responses = [{"output": [{"type": "reasoning", "summary": [{"text": long_text}]}]}]
        env = {
            **os.environ,
            "COLUMNS": "72",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        reasoning_lines = [line for line in completed.stderr.splitlines() if line.strip()]
        self.assertGreaterEqual(len(reasoning_lines), 3)
        self.assertTrue(reasoning_lines[0].startswith("• "), completed.stderr)
        self.assertTrue(all(line.startswith("  ") for line in reasoning_lines[1:]), completed.stderr)

    def test_cli_human_output_renders_escaped_model_errors_without_traceback(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": "[]",
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertIn("ERROR: scripted model exhausted", completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)

    def test_cli_human_output_renders_git_check_errors_without_traceback(self) -> None:
        non_git_dir = Path(os.getcwd()).anchor or os.path.abspath(os.sep)
        self.assertFalse((Path(non_git_dir) / ".git").exists())
        with tempfile.TemporaryDirectory():
            env = {
                **os.environ,
                "PYTHONPATH": os.getcwd(),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--ephemeral",
                    "--cd",
                    non_git_dir,
                    "hello",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertIn("ERROR: not inside a Git repository", completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)

    def test_cli_top_level_errors_do_not_show_runpy_traceback(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": "not-json",
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertIn("ERROR: JSONDecodeError:", completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)
        self.assertNotIn("<frozen runpy>", completed.stderr)
        self.assertNotIn("agents/codex/__main__.py", completed.stderr)

    def test_cli_module_entrypoint_errors_do_not_show_traceback(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": "not-json",
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex.cli",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertIn("ERROR: JSONDecodeError:", completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)
        self.assertNotIn("<frozen runpy>", completed.stderr)
        self.assertNotIn("agents/codex/cli.py", completed.stderr)

    def test_cli_chat_route_errors_do_not_show_traceback(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": "[]",
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertIn("ERROR: scripted model exhausted", completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)
        self.assertNotIn("return _main_chat(raw_argv)", completed.stderr)

    def test_main_chat_direct_call_handles_run_chat_errors(self) -> None:
        from codex import cli

        class RaisingSession:
            def __init__(self, config):
                pass

        original_session = cli.CodexSession
        try:
            cli.CodexSession = RaisingSession
            with tempfile.TemporaryFile(mode="w+", encoding="utf-8") as stderr:
                original_stderr = sys.stderr
                sys.stderr = stderr
                try:
                    status = cli._main_chat(["--skip-git-repo-check", "--ephemeral", "hello"])
                finally:
                    sys.stderr = original_stderr
                stderr.seek(0)
                output = stderr.read()
        finally:
            cli.CodexSession = original_session

        self.assertEqual(status, 1)
        self.assertIn("ERROR:", output)
        self.assertNotIn("Traceback (most recent call last)", output)

    def test_cli_json_output_emits_failed_turn_for_escaped_model_errors(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": "[]",
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--json",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        self.assertNotIn("Traceback (most recent call last)", completed.stderr)
        events = [json.loads(line) for line in completed.stdout.splitlines()]
        failed_events = [event for event in events if event["type"] == "turn.failed"]
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0]["error"], "scripted model exhausted")

    def test_cli_human_output_does_not_duplicate_final_on_stderr(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("single final")]),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "single final")
        self.assertEqual(completed.stderr.count("single final"), 1)

    def test_cli_human_output_renders_official_style_explored_group(self) -> None:
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "list-1",
                        "arguments": json.dumps({"cmd": "rg --files | head -n 2", "yield_time_ms": 250}),
                    }
                ]
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "read-1",
                        "arguments": json.dumps({"cmd": "cat main.py", "yield_time_ms": 250}),
                    }
                ]
            },
            message("explored done"),
        ]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("• Explored", completed.stderr)
        self.assertIn("  └ List", completed.stderr)
        self.assertIn("    Read main.py", completed.stderr)

    def test_cli_human_output_renders_apply_patch_as_file_change(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "temp_print_test.py").write_text('print("hello")\n', encoding="utf-8")
            patch = """*** Begin Patch
*** Update File: temp_print_test.py
@@
-print("hello")
+print("bonjour")
*** End Patch
"""
            responses = [
                {
                    "output": [
                        {
                            "type": "function_call",
                            "name": "apply_patch",
                            "call_id": "patch-1",
                            "arguments": patch,
                        }
                    ]
                },
                message("patch done"),
            ]
            env = {
                **os.environ,
                "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
                "PYTHONPATH": os.getcwd(),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--skip-git-repo-check",
                    "--ephemeral",
                    "--cd",
                    str(root),
                    "change the file",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual((root / "temp_print_test.py").read_text(encoding="utf-8"), 'print("bonjour")\n')
            self.assertIn("• Edited temp_print_test.py (+1 -1)", completed.stderr)
            self.assertIn('    1 -print("hello")', completed.stderr)
            self.assertIn('    1 +print("bonjour")', completed.stderr)
            self.assertNotIn("apply patch", completed.stderr)
            self.assertNotIn("patch: completed", completed.stderr)
            self.assertNotIn("Success. Updated the following files", completed.stderr)

    def test_cli_chat_reuses_session_for_multiple_prompts(self) -> None:
        responses = [message("first answer"), message("second answer")]
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "chat",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            input="first prompt\nsecond prompt\n/exit\n",
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertNotIn("Ask Codex to do anything", completed.stderr)
        self.assertIn("› first prompt", completed.stderr)
        self.assertIn("› second prompt", completed.stderr)
        self.assertIn("• first answer", completed.stderr)
        self.assertIn("• second answer", completed.stderr)

    def test_cli_no_subcommand_routes_to_chat_with_initial_prompt(self) -> None:
        env = {
            **os.environ,
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("top level answer")]),
            "PYTHONPATH": os.getcwd(),
        }
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "hello",
            ],
            input="/exit\n",
            text=True,
            capture_output=True,
            env=env,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "")
        self.assertIn("› hello", completed.stderr)
        self.assertIn("• top level answer", completed.stderr)
        self.assertNotIn("Ask Codex to do anything", completed.stderr)

    def test_cli_tty_chat_bracketed_multiline_paste_submits_one_message(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("paste response")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("\x1b[200~第一行\n第二行\x1b[201~\r", "paste response"),
                ("/exit\r", None),
            ],
        )
        plain = _plain_terminal_output(output)

        self.assertIn("› 第一行", plain)
        self.assertIn("  第二行", plain)
        self.assertIn("• paste response", plain)
        self.assertNotIn("scripted model exhausted", plain)

    def test_cli_tty_chat_direct_chinese_input_decodes_utf8(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("中文 response")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("帮我看一下结构\r", "中文 response"),
                ("/exit\r", None),
            ],
        )
        plain = _plain_terminal_output(output)

        self.assertIn("› 帮我看一下结构", plain)
        self.assertIn("• 中文 response", plain)
        self.assertNotIn("�", plain)

    def test_cli_tty_chat_prompt_supports_left_arrow_editing(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("done")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("helo\x1b[Dl\r", "• done"),
                ("/exit\r", None),
            ],
            timeout=8.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("› hello", plain)
        self.assertIn("• done", plain)

    def test_prompt_escape_sequences_edit_cursor_like_basic_textarea(self) -> None:
        from codex.cli import _apply_prompt_escape_sequence

        self.assertEqual(_apply_prompt_escape_sequence("abc", 2, b"\x1b[D"), ("abc", 1))
        self.assertEqual(_apply_prompt_escape_sequence("abc", 2, b"\x1b[C"), ("abc", 3))
        self.assertEqual(_apply_prompt_escape_sequence("ab\ncd", 4, b"\x1b[H"), ("ab\ncd", 3))
        self.assertEqual(_apply_prompt_escape_sequence("ab\ncd", 3, b"\x1b[F"), ("ab\ncd", 5))
        self.assertEqual(_apply_prompt_escape_sequence("abc", 1, b"\x1b[3~"), ("ac", 1))
        self.assertEqual(_apply_prompt_escape_sequence("abc\ndefgh", 6, b"\x1b[A"), ("abc\ndefgh", 2))
        self.assertEqual(_apply_prompt_escape_sequence("abc\ndefgh", 2, b"\x1b[B"), ("abc\ndefgh", 6))

    def test_live_status_format_matches_upstream_elapsed_and_adds_context_metrics(self) -> None:
        from codex.cli import _AnsiStyle
        from codex.cli import _LiveTurnStatusSnapshot
        from codex.cli import _format_elapsed_compact
        from codex.cli import _format_tokens_compact
        from codex.cli import _live_status_display_lines

        self.assertEqual(_format_elapsed_compact(0), "0s")
        self.assertEqual(_format_elapsed_compact(61), "1m 01s")
        self.assertEqual(_format_elapsed_compact(3661), "1h 01m 01s")
        self.assertEqual(_format_tokens_compact(999), "999")
        self.assertEqual(_format_tokens_compact(12_700), "12.7K")

        lines = _live_status_display_lines(
            _LiveTurnStatusSnapshot(
                header="Working",
                elapsed_seconds=2,
                active_context_tokens=12_700,
                active_context_estimated=True,
                session_context_tokens=18_200,
                session_context_estimated=True,
                session_reasoning_tokens=1_500,
                context_window=400_000,
            ),
            _AnsiStyle(False),
        )

        rendered = "\n".join(lines)
        self.assertIn("• Working (2s • esc to interrupt)", rendered)
        self.assertIn("ctx 12.7K/400K", rendered)
        self.assertIn("session 18.2K", rendered)
        self.assertIn("reasoning", rendered)
        self.assertIn("1.5K", rendered)
        self.assertNotIn("~", rendered)

    def test_cli_tty_chat_esc_interrupts_long_running_tool(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "sleep-1",
                        "arguments": json.dumps({"cmd": "sleep 10", "yield_time_ms": 30000}),
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
            }
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "run sleep",
            ],
            env=env,
            interactions=[
                ("", "run sleep"),
                ("\x1b", "Conversation interrupted"),
                ("/exit\r", None),
            ],
            timeout=12.0,
        )
        plain = _plain_terminal_output(output)

        self.assertRegex(plain, r"Working \([0-9]+s • esc to interrupt\)")
        self.assertIn("ctx ", plain)
        self.assertIn("session ", plain)
        self.assertNotIn("Ask Codex to do anything", plain)
        self.assertIn("Conversation interrupted", plain)
        self.assertIn("Process exited with code", plain)

    def test_cli_tty_chat_pending_input_submits_during_running_turn(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "sleep-1",
                        "arguments": json.dumps({"cmd": "sleep 1.5", "yield_time_ms": 3000}),
                    }
                ]
            },
            message("saw queued input"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "start slow turn",
            ],
            env=env,
            interactions=[
                ("", "start slow turn"),
                ("请继续看 tests\n第二行\r", "Messages to be submitted after next tool call"),
                ("", "saw queued input"),
                ("/exit\r", None),
            ],
            timeout=12.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("› start slow turn", plain)
        self.assertIn("Messages to be submitted after next tool call", plain)
        self.assertIn("› 请继续看 tests", plain)
        self.assertIn("  第二行", plain)
        self.assertIn("• saw queued input", plain)
        self.assertNotIn("�", plain)

    def test_cli_tty_chat_answers_request_user_input_tool(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        questions = [
            {
                "id": "choice",
                "header": "Choice",
                "question": "Pick one.",
                "options": [{"label": "A (Recommended)", "description": "Choose A."}],
            }
        ]
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "request_user_input",
                        "call_id": "input-1",
                        "arguments": json.dumps({"questions": questions}),
                    }
                ]
            },
            message("got answer"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "-c",
                "collaboration_mode=Plan",
            ],
            env=env,
            interactions=[
                ("start\r", "Pick one."),
                ("1\r", "got answer"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Questions 1/1 answered", plain)
        self.assertIn("answer: A (Recommended)", plain)
        self.assertIn("• got answer", plain)
        self.assertNotIn("request_user_input was cancelled", plain)

    def test_cli_tty_request_user_input_supports_arrow_selection(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        questions = [
            {
                "id": "choice",
                "header": "Choice",
                "question": "Pick one.",
                "options": [
                    {"label": "A (Recommended)", "description": "Choose A."},
                    {"label": "B", "description": "Choose B."},
                ],
            }
        ]
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "request_user_input",
                        "call_id": "input-1",
                        "arguments": json.dumps({"questions": questions}),
                    }
                ]
            },
            message("arrow answer accepted"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "-c",
                "collaboration_mode=Plan",
            ],
            env=env,
            interactions=[
                ("start\r", "Pick one."),
                ("\x1b[B\r", "arrow answer accepted"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Questions 1/1 answered", plain)
        self.assertIn("answer: B", plain)
        self.assertIn("• arrow answer accepted", plain)
        self.assertNotIn("request_user_input was cancelled", plain)

    def test_cli_tty_request_user_input_supports_other_freeform_answer(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        questions = [
            {
                "id": "choice",
                "header": "Choice",
                "question": "Pick one.",
                "options": [
                    {"label": "A (Recommended)", "description": "Choose A."},
                    {"label": "B", "description": "Choose B."},
                ],
            }
        ]
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "request_user_input",
                        "call_id": "input-1",
                        "arguments": json.dumps({"questions": questions}),
                    }
                ]
            },
            message("other answer accepted"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "-c",
                "collaboration_mode=Plan",
            ],
            env=env,
            interactions=[
                ("start\r", "None of the above"),
                ("\x1b[B\x1b[B\r", "Other:"),
                ("我想自己填\r", "other answer accepted"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Questions 1/1 answered", plain)
        self.assertIn("answer: None of the above", plain)
        self.assertIn("note: 我想自己填", plain)
        self.assertIn("• other answer accepted", plain)
        self.assertNotIn("request_user_input was cancelled", plain)

    def test_cli_tty_chat_plan_command_enables_request_user_input_tool(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        questions = [
            {
                "id": "choice",
                "header": "Choice",
                "question": "Pick one.",
                "options": [{"label": "A (Recommended)", "description": "Choose A."}],
            }
        ]
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "request_user_input",
                        "call_id": "input-1",
                        "arguments": json.dumps({"questions": questions}),
                    }
                ]
            },
            message("plan answer accepted"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/plan\r", "Switched to Plan mode."),
                ("ask\r", "Pick one."),
                ("1\r", "plan answer accepted"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Switched to Plan mode.", plain)
        self.assertIn("Questions 1/1 answered", plain)
        self.assertIn("• plan answer accepted", plain)
        self.assertNotIn("unavailable in Default mode", plain)

    def test_cli_tty_chat_inline_plan_command_submits_remainder_in_plan_mode(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        questions = [
            {
                "id": "choice",
                "header": "Choice",
                "question": "Pick one.",
                "options": [{"label": "A (Recommended)", "description": "Choose A."}],
            }
        ]
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "request_user_input",
                        "call_id": "input-1",
                        "arguments": json.dumps({"questions": questions}),
                    }
                ]
            },
            message("inline plan answer accepted"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/plan ask with input\r", "Pick one."),
                ("1\r", "inline plan answer accepted"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Switched to Plan mode.", plain)
        self.assertIn("› ask with input", plain)
        self.assertIn("Questions 1/1 answered", plain)
        self.assertIn("• inline plan answer accepted", plain)
        self.assertNotIn("unavailable in Default mode", plain)

    def test_cli_tty_known_unsupported_slash_command_is_not_sent_to_model(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("model should not be called")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/permissions\r", "recognized as a Codex command"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("'/permissions' is recognized as a Codex command", plain)
        self.assertNotIn("model should not be called", plain)

    def test_cli_tty_unknown_slash_command_is_not_sent_to_model(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("model should not be called")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/definitely-not-real\r", "Unrecognized command"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Unrecognized command '/definitely-not-real'", plain)
        self.assertNotIn("model should not be called", plain)

    def test_cli_tty_first_prompt_is_not_rendered_as_queued_follow_up(self) -> None:
        # An idle REPL receiving its first prompt must flow straight into the
        # normal turn renderer. Previous solutions were also echoing the input
        # under a "Queued follow-up inputs" banner before the turn started,
        # which both duplicated the prompt and falsely suggested it was being
        # deferred.
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("hi back")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("hello there\r", "hi back"),
                ("/exit\r", None),
            ],
            timeout=15.0,
        )
        plain = _plain_terminal_output(output)

        prelude = plain.split("hi back", 1)[0]
        self.assertNotIn("Queued follow-up inputs", prelude)
        self.assertNotIn("Messages to be submitted after next tool call", prelude)
        self.assertLessEqual(
            prelude.count("hello there"),
            2,
            msg=f"prompt should not be echoed more than twice; got:\n{prelude}",
        )

    def test_cli_tty_chat_accepts_input_after_esc_interrupt(self) -> None:
        # After the user presses ESC to abort a running turn, the REPL must
        # still accept and dispatch the next prompt. A previous regression
        # tore down the keyboard reader thread on ESC, leaving the REPL
        # blocked on an empty input queue forever.
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "sleep-1",
                        "arguments": json.dumps({"cmd": "sleep 10", "yield_time_ms": 30000}),
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
            },
            message("post-interrupt-reply"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("run slow\r", "Working"),
                ("\x1b", "Conversation interrupted"),
                ("ping again\r", "post-interrupt-reply"),
                ("/exit\r", None),
            ],
            timeout=20.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Conversation interrupted", plain)
        self.assertIn("post-interrupt-reply", plain)

    def test_cli_tty_render_uses_crlf_line_endings_in_raw_mode(self) -> None:
        # In interactive TUI mode the tty is in raw mode (no OPOST/ONLCR
        # translation), so a bare LF only advances the cursor row, leaving
        # the column wherever the previous line ended. Successive tool
        # cells therefore drift right until they fall off-screen. The
        # renderer must emit CR+LF for every line break, either by
        # leaving OPOST on or by writing "\r\n" explicitly.
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "cmd-1",
                        "arguments": json.dumps({"cmd": "echo first", "yield_time_ms": 2000}),
                    }
                ],
                "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
            },
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "cmd-2",
                        "arguments": json.dumps({"cmd": "echo second", "yield_time_ms": 2000}),
                    }
                ],
                "usage": {"input_tokens": 12, "output_tokens": 2, "total_tokens": 14},
            },
            message("done"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("run two\r", "done"),
                ("/exit\r", None),
            ],
            timeout=20.0,
        )

        crlf_count = output.count("\r\n")
        bare_lf_count = len(re.findall(r"(?<!\r)\n", output))
        self.assertGreater(
            crlf_count,
            5,
            msg=f"expected many CRLF line endings in raw-mode TUI output; "
                f"saw crlf={crlf_count} bare_lf={bare_lf_count}",
        )
        self.assertLess(
            bare_lf_count,
            crlf_count,
            msg=f"raw-mode TUI emitted more bare LFs than CRLFs "
                f"(crlf={crlf_count} bare_lf={bare_lf_count}); "
                f"successive cells will drift right",
        )

    def test_cli_tty_model_slash_command_is_recognized(self) -> None:
        # `/model` is part of the upstream slash-command surface. Solutions
        # have shipped without it, falling through to "Unrecognized command",
        # which both breaks the upstream workflow and confuses the user.
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("model should not be called")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/model\r", None),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertNotIn("Unrecognized command '/model'", plain)
        self.assertNotIn("model should not be called", plain)

    def test_cli_tty_status_slash_command_renders_local_status(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/status\r", "Session status"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("model:", plain)
        self.assertIn("mode:", plain)
        self.assertIn("rollout:", plain)

    def test_cli_tty_compact_slash_command_renders_context_compacted_not_summary(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps([message("hidden compact summary")]),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
            ],
            env=env,
            interactions=[
                ("/compact\r", "Context compacted"),
                ("/exit\r", None),
            ],
            timeout=10.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Context compacted", plain)
        self.assertNotIn("hidden compact summary", plain)

    def test_cli_tty_compact_slash_command_queues_while_turn_runs(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        responses = [
            {
                "output": [
                    {
                        "type": "function_call",
                        "name": "exec_command",
                        "call_id": "sleep-1",
                        "arguments": json.dumps({"cmd": "sleep 1.5", "yield_time_ms": 3000}),
                    }
                ]
            },
            message("regular turn done"),
            message("hidden compact summary"),
        ]
        env = {
            **os.environ,
            "TERM": "xterm-256color",
            "PY_CODEX_FAKE_RESPONSES": json.dumps(responses),
            "PYTHONPATH": os.getcwd(),
        }
        output = self._run_cli_pty(
            [
                sys.executable,
                "-m",
                "codex",
                "--skip-git-repo-check",
                "--ephemeral",
                "--color",
                "never",
                "start slow turn",
            ],
            env=env,
            interactions=[
                ("", "start slow turn"),
                ("/compact\r", "Queued follow-up inputs"),
                ("", "regular turn done"),
                ("", "Context compacted"),
                ("/exit\r", None),
            ],
            timeout=12.0,
        )
        plain = _plain_terminal_output(output)

        self.assertIn("Queued follow-up inputs", plain)
        self.assertIn("↳ /compact", plain)
        self.assertIn("Context compacted", plain)
        self.assertNotIn("hidden compact summary", plain)

    def test_chat_user_history_lines_use_upstream_gutter_and_wrapping(self) -> None:
        from codex.cli import _HumanEventRenderer

        lines: list[str] = []
        renderer = _HumanEventRenderer(color_mode="never", line_sink=lines.append)
        renderer.render_user_message("这是一个很长很长的用户输入，用来测试换行之后是不是继续缩进")

        self.assertTrue(lines[0].startswith("› "), lines)
        self.assertTrue(all(line.startswith("  ") for line in lines[1:]), lines)

    def test_cli_human_renderer_matches_upstream_plan_update_cell(self) -> None:
        from codex.cli import _HumanEventRenderer

        lines: list[str] = []
        renderer = _HumanEventRenderer(color_mode="never", line_sink=lines.append)
        renderer.render(
            codex_types.CodexEvent(
                "tool.completed",
                {
                    "name": "update_plan",
                    "call_id": "plan-1",
                    "ok": True,
                    "metadata": {
                        "explanation": "Adjust the UI renderer.",
                        "plan": [
                            {"step": "Audit upstream cells", "status": "completed"},
                            {"step": "Port renderer", "status": "in_progress"},
                            {"step": "Verify", "status": "pending"},
                        ],
                    },
                },
            )
        )

        rendered = "\n".join(lines)
        self.assertIn("• Updated Plan", rendered)
        self.assertIn("  └ Adjust the UI renderer.", rendered)
        self.assertIn("✔ Audit upstream cells", rendered)
        self.assertIn("□ Port renderer", rendered)
        self.assertIn("□ Verify", rendered)
        self.assertNotIn("> Port renderer", rendered)

    def test_cli_human_renderer_matches_upstream_web_search_cell(self) -> None:
        from codex.cli import _HumanEventRenderer

        lines: list[str] = []
        renderer = _HumanEventRenderer(color_mode="never", line_sink=lines.append)
        renderer.render(
            codex_types.CodexEvent(
                "item.completed",
                {
                    "item": {
                        "type": "web_search_call",
                        "id": "ws-1",
                        "status": "completed",
                        "query": "short query",
                        "action": {"type": "search", "query": "short query"},
                    }
                },
            )
        )

        self.assertEqual(lines, ["• Searched short query"])

    def test_cli_human_renderer_matches_upstream_request_user_input_result_cell(self) -> None:
        from codex.cli import _HumanEventRenderer

        lines: list[str] = []
        renderer = _HumanEventRenderer(color_mode="never", line_sink=lines.append)
        renderer.render(
            codex_types.CodexEvent(
                "tool.completed",
                {
                    "name": "request_user_input",
                    "call_id": "input-1",
                    "ok": True,
                    "metadata": {
                        "questions": [
                            {
                                "id": "choice",
                                "header": "Choice",
                                "question": "Pick one.",
                                "isOther": True,
                                "options": [{"label": "A (Recommended)", "description": "Choose A."}],
                            }
                        ],
                        "answers": {"choice": {"answers": ["A (Recommended)"]}},
                    },
                },
            )
        )

        rendered = "\n".join(lines)
        self.assertIn("• Questions 1/1 answered", rendered)
        self.assertIn("  • Pick one.", rendered)
        self.assertIn("    answer: A (Recommended)", rendered)

    def _run_cli_pty(
        self,
        argv: list[str],
        *,
        env: dict[str, str],
        interactions: list[tuple[str, str | None]],
        timeout: float = 30.0,
    ) -> str:
        import pty
        import select

        master_fd, slave_fd = pty.openpty()
        proc = subprocess.Popen(
            argv,
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            env=env,
            cwd=os.getcwd(),
            close_fds=True,
        )
        os.close(slave_fd)
        output = bytearray()

        def read_available(wait: float) -> None:
            deadline = time.monotonic() + wait
            while time.monotonic() < deadline:
                readable, _, _ = select.select([master_fd], [], [], 0.05)
                if not readable:
                    if proc.poll() is not None:
                        return
                    continue
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    return
                if not chunk:
                    return
                output.extend(chunk)

        def read_until(needle: str) -> None:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                text = output.decode("utf-8", errors="replace")
                plain = _plain_terminal_output(text)
                if needle in plain:
                    return
                read_available(0.1)
            text = output.decode("utf-8", errors="replace")
            plain = _plain_terminal_output(text)
            self.fail(f"timed out waiting for {needle!r}; output was:\n{plain}")

        try:
            read_available(0.5)
            for chars, expected in interactions:
                if chars:
                    os.write(master_fd, chars.encode("utf-8"))
                if expected is not None:
                    read_until(expected)
                else:
                    read_available(0.5)
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(timeout=2)
            read_available(0.2)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
            os.close(master_fd)
        return output.decode("utf-8", errors="replace")

    def test_command_actions_classify_common_exploration_commands(self) -> None:
        self.assertEqual(parse_command_actions("rg --files | head -n 50")[0]["type"], "list_files")
        self.assertEqual(parse_command_actions("rg -n foo agents/codex")[0]["type"], "search")
        self.assertEqual(parse_command_actions("cat agents/codex/cli.py")[0]["type"], "read")

    def test_cli_exec_accepts_upstream_short_aliases_and_compat_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            last = root / "last.txt"
            schema = root / "schema.json"
            schema.write_text(
                json.dumps(
                    {
                        "type": "object",
                        "properties": {"answer": {"type": "string"}},
                        "required": ["answer"],
                        "additionalProperties": False,
                    }
                ),
                encoding="utf-8",
            )
            extra = root / "extra"
            extra.mkdir()
            env = {
                **os.environ,
                "PY_CODEX_FAKE_RESPONSES": json.dumps([message("alias done")]),
                "PYTHONPATH": os.getcwd(),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--experimental-json",
                    "-m",
                    "gpt-5.2",
                    "-C",
                    str(root),
                    "-s",
                    "workspace-write",
                    "--add-dir",
                    str(extra),
                    "--ignore-user-config",
                    "--ignore-rules",
                    "--skip-git-repo-check",
                    "--ephemeral",
                    "--output-schema",
                    str(schema),
                    "-o",
                    str(last),
                    "hello",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(last.read_text(encoding="utf-8"), "alias done")
            self.assertTrue(completed.stdout.strip())

    def test_cli_exec_loads_config_profile_and_dotted_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            codex_home.mkdir()
            (codex_home / "config.toml").write_text(
                """
model = "base-model"
web_search = "disabled"

[profiles.dev]
model = "profile-model"
web_search = "disabled"
model_reasoning_effort = "high"
""",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "CODEX_HOME": str(codex_home),
                "PY_CODEX_FAKE_RESPONSES": json.dumps([message("config done")]),
                "PYTHONPATH": os.getcwd(),
            }
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--json",
                    "--profile",
                    "dev",
                    "-c",
                    'profiles.dev.model="override-model"',
                    "-c",
                    'profiles.dev.web_search="live"',
                    "--skip-git-repo-check",
                    "--ephemeral",
                    "hello",
                ],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            events = [json.loads(line) for line in completed.stdout.splitlines()]
            thread_started = next(event for event in events if event["type"] == "thread.started")
            model_request = next(event for event in events if event["type"] == "model.request")
            self.assertEqual(thread_started["model"], "override-model")
            self.assertIn("web_search", model_request["tool_names"])

    def test_cli_exec_reads_official_codex_config_before_python_home_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            official_home = home / ".codex"
            python_home = home / ".codex-python"
            official_home.mkdir()
            (official_home / "config.toml").write_text(
                """
model = "gpt-5.5"
model_reasoning_effort = "high"
""",
                encoding="utf-8",
            )
            python_home.mkdir()
            (python_home / "config.toml").write_text(
                """
model = "gpt-5.5"
model_reasoning_effort = "low"
""",
                encoding="utf-8",
            )
            env = {
                **os.environ,
                "HOME": str(home),
                "PY_CODEX_FAKE_RESPONSES": json.dumps([message("official config done")]),
                "PYTHONPATH": os.getcwd(),
            }
            env.pop("CODEX_HOME", None)
            env.pop("CODEX_PY_HOME", None)
            env.pop("OPENAI_REASONING_EFFORT", None)
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--json",
                    "--skip-git-repo-check",
                    "hello",
                ],
                cwd=Path(__file__).resolve().parents[1],
                text=True,
                capture_output=True,
                env=env,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            events = [json.loads(line) for line in completed.stdout.splitlines()]
            thread_started = next(event for event in events if event["type"] == "thread.started")
            self.assertEqual(thread_started["model"], "gpt-5.5")

            rollout_path = next(
                (python_home / "sessions").glob(f"????/??/??/rollout-*-{thread_started['thread_id']}.jsonl")
            )
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            turn_context = next(record["payload"] for record in records if record["type"] == "turn_context")
            self.assertEqual(turn_context["effort"], "high")
            self.assertFalse((official_home / "sessions").exists())

    def test_cli_exec_oss_flags_and_provider_config_with_fake_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            codex_home.mkdir()
            env = {
                **os.environ,
                "CODEX_HOME": str(codex_home),
                "PYTHONPATH": os.getcwd(),
            }
            local_provider = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--json",
                    "--oss",
                    "--local-provider",
                    "lmstudio",
                    "--full-auto",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    "hello",
                ],
                text=True,
                capture_output=True,
                env={**env, "PY_CODEX_FAKE_RESPONSES": json.dumps([message("oss done")])},
                check=False,
            )
            self.assertEqual(local_provider.returncode, 0, local_provider.stderr)
            self.assertIn("`--full-auto` is deprecated", local_provider.stderr)
            events = [json.loads(line) for line in local_provider.stdout.splitlines()]
            thread_started = next(event for event in events if event["type"] == "thread.started")
            self.assertEqual(thread_started["model"], "openai/gpt-oss-20b")
            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{thread_started['thread_id']}.jsonl"))
            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(records[0]["payload"]["meta"]["model_provider"], "lmstudio")

            (codex_home / "config.toml").write_text('oss_provider = "ollama"\n', encoding="utf-8")
            configured_provider = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "--json",
                    "--oss",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    "--ephemeral",
                    "hello",
                ],
                text=True,
                capture_output=True,
                env={**env, "PY_CODEX_FAKE_RESPONSES": json.dumps([message("oss config done")])},
                check=False,
            )
            self.assertEqual(configured_provider.returncode, 0, configured_provider.stderr)
            configured_events = [json.loads(line) for line in configured_provider.stdout.splitlines()]
            configured_thread = next(event for event in configured_events if event["type"] == "thread.started")
            self.assertEqual(configured_thread["model"], "gpt-oss:20b")

    def test_cli_exec_resume_rollout_path_and_last_with_fake_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            first = CodexSession(
                CodexConfig(
                    cwd=root,
                    codex_home=codex_home,
                    skip_git_repo_check=True,
                    ephemeral=False,
                ),
                model_client=ScriptedResponsesModel([message("first answer")]),
            ).run("first")
            rollout_path = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{first.thread_id}.jsonl"))

            env = {
                **os.environ,
                "CODEX_HOME": str(codex_home),
                "PYTHONPATH": os.getcwd(),
            }
            path_completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "resume",
                    "--json",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    str(rollout_path),
                    "second",
                ],
                text=True,
                capture_output=True,
                env={**env, "PY_CODEX_FAKE_RESPONSES": json.dumps([message("second answer")])},
                check=False,
            )
            self.assertEqual(path_completed.returncode, 0, path_completed.stderr)
            path_events = [json.loads(line) for line in path_completed.stdout.splitlines()]
            path_thread = next(event for event in path_events if event["type"] == "thread.started")
            self.assertEqual(path_thread["thread_id"], first.thread_id)

            last_completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "resume",
                    "--last",
                    "--json",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    "third",
                ],
                text=True,
                capture_output=True,
                env={**env, "PY_CODEX_FAKE_RESPONSES": json.dumps([message("third answer")])},
                check=False,
            )
            self.assertEqual(last_completed.returncode, 0, last_completed.stderr)
            last_events = [json.loads(line) for line in last_completed.stdout.splitlines()]
            last_thread = next(event for event in last_events if event["type"] == "thread.started")
            last_turn = next(event for event in last_events if event["type"] == "turn.completed")
            self.assertEqual(last_thread["thread_id"], first.thread_id)
            self.assertEqual(last_turn["final_message"], "third answer")

            records = [json.loads(line) for line in rollout_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(sum(1 for record in records if record["type"] == "session_meta"), 1)
            reconstructed = reconstruct_history_from_rollout(rollout_path)
            assistant_texts = [
                item["content"][0]["text"]
                for item in reconstructed.history
                if item.get("role") == "assistant"
            ]
            self.assertEqual(assistant_texts, ["first answer", "second answer", "third answer"])

            fork_completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "exec",
                    "fork",
                    "--json",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    str(rollout_path),
                    "forked",
                ],
                text=True,
                capture_output=True,
                env={**env, "PY_CODEX_FAKE_RESPONSES": json.dumps([message("fork answer")])},
                check=False,
            )
            self.assertEqual(fork_completed.returncode, 0, fork_completed.stderr)
            fork_events = [json.loads(line) for line in fork_completed.stdout.splitlines()]
            fork_thread = next(event for event in fork_events if event["type"] == "thread.started")
            self.assertNotEqual(fork_thread["thread_id"], first.thread_id)
            fork_rollout = next((codex_home / "sessions").glob(f"????/??/??/rollout-*-{fork_thread['thread_id']}.jsonl"))
            fork_records = [json.loads(line) for line in fork_rollout.read_text(encoding="utf-8").splitlines()]
            fork_meta = next(record["payload"] for record in fork_records if record["type"] == "session_meta")
            self.assertEqual(fork_meta["meta"]["forked_from_id"], first.thread_id)
            fork_reconstructed = reconstruct_history_from_rollout(fork_rollout)
            fork_assistant_texts = [
                item["content"][0]["text"]
                for item in fork_reconstructed.history
                if item.get("role") == "assistant"
            ]
            self.assertEqual(fork_assistant_texts, ["first answer", "second answer", "third answer", "fork answer"])

    def test_interactive_slash_resume_and_fork_replace_chat_session(self) -> None:
        from codex import cli

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = CodexConfig(
                cwd=root,
                codex_home=codex_home,
                skip_git_repo_check=True,
                ephemeral=False,
            )
            first = CodexSession(
                config,
                model_client=ScriptedResponsesModel([message("saved answer")]),
            ).run("saved")
            current = CodexSession(config, model_client=ScriptedResponsesModel([]))

            with redirect_stderr(io.StringIO()):
                resumed = cli._handle_interactive_slash_command(current, "/resume --last")

            self.assertTrue(resumed.handled)
            self.assertIsNotNone(resumed.session)
            assert resumed.session is not None
            self.assertEqual(resumed.session.state.thread_id, first.thread_id)
            self.assertEqual(
                [
                    item["content"][0]["text"]
                    for item in resumed.session.state.history
                    if item.get("role") == "assistant"
                ],
                ["saved answer"],
            )

            with redirect_stderr(io.StringIO()):
                forked = cli._handle_interactive_slash_command(resumed.session, "/fork")

            self.assertTrue(forked.handled)
            self.assertIsNotNone(forked.session)
            assert forked.session is not None
            self.assertNotEqual(forked.session.state.thread_id, first.thread_id)
            self.assertEqual(forked.session.state.forked_from_id, first.thread_id)
            self.assertEqual(forked.session.state.history, resumed.session.state.history)

    def test_interactive_new_and_clear_start_fresh_chat_session(self) -> None:
        from codex import cli

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = CodexConfig(cwd=root, skip_git_repo_check=True, ephemeral=True)
            current = CodexSession(
                config,
                model_client=ScriptedResponsesModel([message("old answer")]),
            )
            current.run("old prompt")

            with redirect_stderr(io.StringIO()):
                new_result = cli._handle_interactive_slash_command(current, "/new")

            self.assertTrue(new_result.handled)
            self.assertIsNotNone(new_result.session)
            assert new_result.session is not None
            self.assertNotEqual(new_result.session.state.thread_id, current.state.thread_id)
            self.assertEqual(new_result.session.state.history, [])
            self.assertIs(new_result.session.model_client, current.model_client)

            with redirect_stderr(io.StringIO()):
                clear_result = cli._handle_interactive_slash_command(current, "/clear")

            self.assertTrue(clear_result.handled)
            self.assertIsNotNone(clear_result.session)
            assert clear_result.session is not None
            self.assertNotEqual(clear_result.session.state.thread_id, current.state.thread_id)
            self.assertEqual(clear_result.session.state.history, [])

    def test_interactive_ps_and_stop_manage_background_terminals(self) -> None:
        from codex import cli

        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=ScriptedResponsesModel([]),
        )
        command = f"{sys.executable!r} -c \"import time; time.sleep(5)\""
        result = session.tools.exec_command({"cmd": command, "yield_time_ms": 10})
        self.assertTrue(result.ok)
        self.assertIn("session_id", result.metadata)

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            ps_result = cli._handle_interactive_slash_command(session, "/ps")

        self.assertTrue(ps_result.handled)
        self.assertIn("Background terminals:", stderr.getvalue())
        self.assertIn(str(result.metadata["session_id"]), stderr.getvalue())

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            stop_result = cli._handle_interactive_slash_command(session, "/stop")

        self.assertTrue(stop_result.handled)
        self.assertIn("Stopped 1 background terminal", stderr.getvalue())
        self.assertEqual(cli._background_terminal_rows(session), [])

    def test_cli_top_level_help_lists_resume_and_fork(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "codex", "--help"],
            text=True,
            capture_output=True,
            env={**os.environ, "PYTHONPATH": os.getcwd()},
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("resume", completed.stdout)
        self.assertIn("fork", completed.stdout)

    def test_rollout_picker_rows_use_first_user_message_preview_and_upstream_controls(self) -> None:
        from codex import cli

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = CodexConfig(
                cwd=root,
                codex_home=codex_home,
                skip_git_repo_check=True,
                ephemeral=False,
            )
            first = CodexSession(
                config,
                model_client=ScriptedResponsesModel([message("saved answer")]),
            ).run("first saved prompt\nwith detail")

            rows = cli._rollout_picker_rows(config)
            row = next(item for item in rows if item.thread_id == first.thread_id)
            self.assertEqual(row.preview, "first saved prompt with detail")

            lines = cli._rollout_picker_display_lines(
                [row],
                title="Resume a previous session",
                style=cli._AnsiStyle(False),
                cwd=root,
                show_all=False,
                query="",
                sort_key="updated",
                selected=0,
                offset=0,
                density="comfortable",
                toolbar_focus="filter",
                expanded=True,
            )
            rendered = "\n".join(lines)
            self.assertIn("Resume a previous session", rendered)
            self.assertIn("Type to search", rendered)
            self.assertIn("Filter:[Cwd]", rendered)
            self.assertIn("Sort:[Updated]", rendered)
            self.assertIn("❯ first saved prompt with detail", rendered)
            self.assertIn("id: " + first.thread_id, rendered)
            self.assertIn("enter resume", rendered)
            self.assertIn("ctrl+o dense/comfortable", rendered)

            many_rows = [
                cli._RolloutPickerRow(
                    path=root / f"rollout-{index}.jsonl",
                    preview=f"session preview {index}",
                    thread_id=f"thread-{index}",
                    created_at=time.time() - index,
                    updated_at=time.time() - index,
                    cwd=str(root),
                )
                for index in range(30)
            ]
            many_lines = cli._rollout_picker_display_lines(
                many_rows,
                title="Resume a previous session",
                style=cli._AnsiStyle(False),
                cwd=root,
                show_all=False,
                query="",
                sort_key="updated",
                selected=0,
                offset=0,
                density="comfortable",
                toolbar_focus="filter",
                expanded=False,
            )
            many_rendered = "\n".join(many_lines)
            self.assertIn("session preview 0", many_rendered)
            self.assertIn("↓", many_rendered)
            self.assertNotIn("session preview 29", many_rendered)

    def test_cli_tty_top_level_resume_opens_picker_and_enters_chat(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = CodexConfig(
                cwd=root,
                codex_home=codex_home,
                skip_git_repo_check=True,
                ephemeral=False,
            )
            CodexSession(
                config,
                model_client=ScriptedResponsesModel([message("saved answer")]),
            ).run("resume picker prompt")
            env = {
                **os.environ,
                "TERM": "xterm-256color",
                "CODEX_HOME": str(codex_home),
                "PYTHONPATH": os.getcwd(),
            }

            output = self._run_cli_pty(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "resume",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    "--color",
                    "never",
                ],
                env=env,
                interactions=[
                    ("", "resume picker prompt"),
                    ("\r", "› "),
                    ("/exit\r", None),
                ],
                timeout=10.0,
            )
            plain = _plain_terminal_output(output)

            self.assertIn("Resume a previous session", plain)
            self.assertIn("resume picker prompt", plain)
            self.assertIn("saved answer", plain)
            self.assertIn("Filter:[Cwd]", plain)
            self.assertIn("Sort:[Updated]", plain)
            self.assertNotIn("Select a chat number", plain)

    def test_cli_tty_top_level_fork_uses_same_picker_surface(self) -> None:
        if sys.platform == "win32":
            self.skipTest("PTY smoke is Unix-only")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            codex_home = root / "codex-home"
            config = CodexConfig(
                cwd=root,
                codex_home=codex_home,
                skip_git_repo_check=True,
                ephemeral=False,
            )
            CodexSession(
                config,
                model_client=ScriptedResponsesModel([message("fork source answer")]),
            ).run("fork picker prompt")
            env = {
                **os.environ,
                "TERM": "xterm-256color",
                "CODEX_HOME": str(codex_home),
                "PYTHONPATH": os.getcwd(),
            }

            output = self._run_cli_pty(
                [
                    sys.executable,
                    "-m",
                    "codex",
                    "fork",
                    "-C",
                    str(root),
                    "--skip-git-repo-check",
                    "--color",
                    "never",
                ],
                env=env,
                interactions=[
                    ("", "fork picker prompt"),
                    ("\r", "› "),
                    ("/exit\r", None),
                ],
                timeout=10.0,
            )
            plain = _plain_terminal_output(output)

            self.assertIn("Fork a previous session", plain)
            self.assertIn("fork picker prompt", plain)
            self.assertIn("fork source answer", plain)
            self.assertIn("Filter:[Cwd]", plain)
            self.assertIn("Sort:[Updated]", plain)
            self.assertNotIn("Select a chat number", plain)

    def test_interactive_theme_slash_sets_python_cli_syntax_theme(self) -> None:
        from codex import cli

        old_theme = cli._CLI_SYNTAX_THEME
        try:
            session = CodexSession(
                CodexConfig(skip_git_repo_check=True, ephemeral=True),
                model_client=ScriptedResponsesModel([]),
            )
            with redirect_stderr(io.StringIO()):
                result = cli._handle_interactive_slash_command(session, "/theme dracula")

            self.assertTrue(result.handled)
            self.assertEqual(cli._CLI_SYNTAX_THEME, "dracula")
            self.assertEqual(cli._pygments_style_name(), "dracula")
        finally:
            cli._CLI_SYNTAX_THEME = old_theme


    # ------------------------------------------------------------------
    # Sanity / smoke tests for the basics agents have shipped broken.
    # ------------------------------------------------------------------

    def test_codex_package_imports_cleanly(self) -> None:
        """`python3 -c "import codex; import codex.X..."` must succeed for every
        submodule the eval reaches into. Catches NameErrors / missing typing
        imports / circular-import explosions that prevent the suite from
        starting at all."""
        env = dict(os.environ)
        env.pop("PY_CODEX_FAKE_RESPONSES", None)
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import codex; "
                "import codex.types; import codex.state; import codex.prompts; "
                "import codex.tools; import codex.model; import codex.memory; "
                "import codex.cli",
            ],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_python_m_codex_help_lists_known_subcommands(self) -> None:
        """`python3 -m codex --help` must succeed and mention `exec`. Catches
        agents that ship without a top-level CLI (no `__main__`, no parser)."""
        env = dict(os.environ)
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [sys.executable, "-m", "codex", "--help"],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("exec", completed.stdout)
        completed = subprocess.run(
            [sys.executable, "-m", "codex", "exec", "--help"],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_default_model_client_is_openai_responses_when_no_fake_env(self) -> None:
        """Without `PY_CODEX_FAKE_RESPONSES`, `default_model_client()` must
        return an `OpenAIResponsesModel` instance — the live API client.
        Catches agents that ship hardcoded stubs as the default."""
        env = dict(os.environ)
        env.pop("PY_CODEX_FAKE_RESPONSES", None)
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os\n"
                "os.environ.pop('PY_CODEX_FAKE_RESPONSES', None)\n"
                "from codex.model import default_model_client, OpenAIResponsesModel\n"
                "client = default_model_client()\n"
                "assert isinstance(client, OpenAIResponsesModel), "
                "'default_model_client() must return OpenAIResponsesModel when "
                "no PY_CODEX_FAKE_RESPONSES is set; got %s' % type(client).__name__\n",
            ],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_codex_session_default_model_client_is_openai_responses(self) -> None:
        """A bare `CodexSession()` (no explicit model_client) must wire to
        `OpenAIResponsesModel`. Catches agents whose CodexSession constructor
        silently picks a stub."""
        env = dict(os.environ)
        env.pop("PY_CODEX_FAKE_RESPONSES", None)
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os\n"
                "os.environ.pop('PY_CODEX_FAKE_RESPONSES', None)\n"
                "from codex import CodexConfig, CodexSession\n"
                "from codex.model import OpenAIResponsesModel\n"
                "session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True))\n"
                "assert isinstance(session.model_client, OpenAIResponsesModel), "
                "'CodexSession.model_client must default to OpenAIResponsesModel; "
                "got %s' % type(session.model_client).__name__\n",
            ],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_py_codex_fake_responses_env_is_honored_by_default_client(self) -> None:
        """Inverse guard: when `PY_CODEX_FAKE_RESPONSES` IS set,
        `default_model_client()` must return a `ScriptedResponsesModel` —
        catching agents that ignore the env var and unconditionally call
        OpenAI even in CI / test runs."""
        env = dict(os.environ)
        env["PY_CODEX_FAKE_RESPONSES"] = json.dumps([
            {"output": [{"type": "message", "role": "assistant",
                         "content": [{"type": "output_text", "text": "ok"}]}]}
        ])
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                "from codex.model import default_model_client, ScriptedResponsesModel\n"
                "client = default_model_client()\n"
                "assert isinstance(client, ScriptedResponsesModel), "
                "'PY_CODEX_FAKE_RESPONSES set must yield ScriptedResponsesModel; "
                "got %s' % type(client).__name__\n",
            ],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_codex_session_stream_routes_through_model_client(self) -> None:
        """`session.run(prompt)` must invoke the configured `model_client`
        (via `.stream(...)` or `.create(...)`) at least once. Catches agents
        whose `session.stream()` yields hardcoded events and bypasses the
        client entirely (teamwork-style fake REPL output)."""
        calls: list[Any] = []

        class _TrackingModel:
            def stream(self, request: PromptRequest):
                calls.append(("stream", request))
                yield ModelStreamEvent(
                    type="response.completed",
                    payload={"id": "track-1", "output": [
                        {"type": "message", "role": "assistant",
                         "content": [{"type": "output_text", "text": "ok"}]}
                    ]},
                )

            def create(self, request: PromptRequest):
                calls.append(("create", request))
                return type("R", (), {"id": "track-1", "output": [
                    {"type": "message", "role": "assistant",
                     "content": [{"type": "output_text", "text": "ok"}]}
                ], "raw": {}})()

        session = CodexSession(
            CodexConfig(skip_git_repo_check=True, ephemeral=True),
            model_client=_TrackingModel(),
        )
        session.run("hello")
        self.assertGreaterEqual(
            len(calls), 1,
            "session.run() did not invoke model_client.stream/create — "
            "implementation is yielding hardcoded events instead of using "
            "the configured model client.",
        )

    def test_session_run_raises_when_no_api_key_and_no_fake_responses(self) -> None:
        """With no `OPENAI_API_KEY` and no `PY_CODEX_FAKE_RESPONSES`, calling
        `session.run("hi")` must raise an error (not silently emit placeholder
        events). Catches agents that swallow the missing-credentials case."""
        env = dict(os.environ)
        env.pop("OPENAI_API_KEY", None)
        env.pop("PY_CODEX_FAKE_RESPONSES", None)
        env["PYTHONPATH"] = os.getcwd() + os.pathsep + env.get("PYTHONPATH", "")
        script = (
            "import os\n"
            "os.environ.pop('OPENAI_API_KEY', None)\n"
            "os.environ.pop('PY_CODEX_FAKE_RESPONSES', None)\n"
            "from codex import CodexConfig, CodexSession\n"
            "session = CodexSession(CodexConfig(skip_git_repo_check=True, ephemeral=True))\n"
            "raised = False\n"
            "try:\n"
            "    session.run('hi')\n"
            "except Exception:\n"
            "    raised = True\n"
            "assert raised, 'session.run() did not raise when neither "
            "OPENAI_API_KEY nor PY_CODEX_FAKE_RESPONSES was set'\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            capture_output=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_cli_exec_exit_codes_reflect_outcome(self) -> None:
        """`python3 -m codex exec` must exit 0 on success and non-zero on
        failure. Catches CLIs that always exit 0 (downstream CI cannot tell
        success from failure)."""
        env_ok = dict(os.environ)
        env_ok["PY_CODEX_FAKE_RESPONSES"] = json.dumps([
            {"output": [{"type": "message", "role": "assistant",
                         "content": [{"type": "output_text", "text": "ok"}]}]}
        ])
        env_ok["PYTHONPATH"] = os.getcwd() + os.pathsep + env_ok.get("PYTHONPATH", "")
        ok = subprocess.run(
            [sys.executable, "-m", "codex", "exec",
             "--skip-git-repo-check", "--ephemeral", "say hi"],
            env=env_ok,
            text=True,
            capture_output=True,
        )
        self.assertEqual(ok.returncode, 0, ok.stderr)

        env_bad = dict(os.environ)
        env_bad["PY_CODEX_FAKE_RESPONSES"] = "not-json"
        env_bad["PYTHONPATH"] = os.getcwd() + os.pathsep + env_bad.get("PYTHONPATH", "")
        bad = subprocess.run(
            [sys.executable, "-m", "codex", "exec",
             "--skip-git-repo-check", "--ephemeral", "say hi"],
            env=env_bad,
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(bad.returncode, 0,
                            "exec must exit non-zero when the model layer fails")

    def test_apply_patch_rejects_stale_context_without_modifying_files(self) -> None:
        """`apply_patch` must reject a hunk whose context line doesn't match
        the file, AND must not leave any partial modifications behind.
        Atomicity matters — half-applied patches corrupt the workspace."""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "file.txt"
            target.write_text("real line A\nreal line B\nreal line C\n",
                              encoding="utf-8")
            original = target.read_text(encoding="utf-8")
            runtime = ToolRuntime(CodexConfig(
                cwd=tmp,
                writable_roots=(tmp,),
                skip_git_repo_check=True,
                ephemeral=True,
            ))
            stale_patch = (
                "*** Begin Patch\n"
                "*** Update File: file.txt\n"
                "@@\n"
                " WRONG CONTEXT LINE\n"
                "-real line B\n"
                "+real line B!!!\n"
                "*** End Patch\n"
            )
            result = runtime.apply_patch({"patch": stale_patch})
            self.assertFalse(result.ok,
                             f"apply_patch should fail on stale context; got ok=True, output={result.output!r}")
            self.assertEqual(target.read_text(encoding="utf-8"), original,
                             "apply_patch must leave the file untouched when "
                             "the patch is rejected")


if __name__ == "__main__":
    unittest.main()
