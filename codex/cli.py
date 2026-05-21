from __future__ import annotations

import argparse
import codecs
import json
import os
import queue
import re
import shlex
import shutil
import select
import sys
import termios
import textwrap
import threading
import time
import tomllib
import unicodedata

from collections import deque
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from .core import CodexSession, SteerInputError, TurnInterrupted


def _set_raw_keep_opost(fd: int) -> None:
    """Like tty.setraw, but keep OPOST so output \\n is still translated to \\r\\n.

    Without OPOST, lines printed while a bottom-anchored prompt is rendered
    leave the cursor at an arbitrary column, causing the prompt to "jump"
    to the middle of the screen on the next redraw.
    """
    mode = termios.tcgetattr(fd)
    # iflag
    mode[0] &= ~(
        termios.BRKINT
        | termios.ICRNL
        | termios.INPCK
        | termios.ISTRIP
        | termios.IXON
    )
    # oflag — intentionally leave OPOST set
    # cflag
    mode[2] &= ~(termios.CSIZE | termios.PARENB)
    mode[2] |= termios.CS8
    # lflag
    mode[3] &= ~(termios.ECHO | termios.ICANON | termios.IEXTEN | termios.ISIG)
    mode[6][termios.VMIN] = 1
    mode[6][termios.VTIME] = 0
    termios.tcsetattr(fd, termios.TCSAFLUSH, mode)
from .state import parse_command_actions, reconstruct_history_from_rollout
from .types import CodexConfig, CodexEvent, _model_catalog_info
from . import types as _types


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        _print_cli_error(exc)
        return 1


def _main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    if len(raw_argv) >= 2 and raw_argv[0] == "exec" and raw_argv[1] == "resume":
        return _main_exec_resume(raw_argv[2:])
    if len(raw_argv) >= 2 and raw_argv[0] == "exec" and raw_argv[1] == "fork":
        return _main_exec_fork(raw_argv[2:])
    if raw_argv and raw_argv[0] == "chat":
        return _main_chat(raw_argv[1:], prog="python -m codex chat")
    if _should_route_to_chat(raw_argv):
        return _main_chat(raw_argv)

    parser = argparse.ArgumentParser(prog="python -m codex")
    subparsers = parser.add_subparsers(dest="command")
    chat_parser = subparsers.add_parser("chat")
    chat_parser.add_argument("prompt", nargs="?")
    _add_exec_options(chat_parser)
    exec_parser = subparsers.add_parser("exec")
    exec_parser.add_argument("prompt", nargs="?")
    _add_exec_options(exec_parser)

    args = parser.parse_args(raw_argv)

    if args.command == "chat":
        return _main_chat(raw_argv[1:], prog="python -m codex chat")

    if args.command != "exec":
        parser.print_help(sys.stderr)
        return 2

    try:
        config = _build_exec_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return _run_session(
        CodexSession(config),
        _read_prompt(args.prompt),
        json_events=args.json_events,
        color_mode=args.color,
    )


def _should_route_to_chat(raw_argv: list[str]) -> bool:
    if not raw_argv:
        return True
    if raw_argv[0] in {"-h", "--help", "exec"}:
        return False
    return True


def _main_exec_resume(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m codex exec resume")
    parser.add_argument("session_id", nargs="?")
    parser.add_argument("prompt", nargs="?")
    parser.add_argument("--last", action="store_true")
    parser.add_argument("--all", action="store_true", dest="all_cwds")
    _add_exec_options(parser)
    args = parser.parse_args(argv)

    if args.last and args.prompt is None:
        args.prompt = args.session_id
        args.session_id = None

    try:
        config = _build_exec_config(args)
        rollout_path = _resolve_resume_rollout(args, config)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if rollout_path is None:
        if args.session_id:
            print(f"No Codex rollout found for `{args.session_id}`", file=sys.stderr)
            return 1
        session = CodexSession(config)
    else:
        session = CodexSession.resume_from_rollout(rollout_path, config)
    return _run_session(session, _read_prompt(args.prompt), json_events=args.json_events, color_mode=args.color)


def _main_exec_fork(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m codex exec fork")
    parser.add_argument("session_id", nargs="?")
    parser.add_argument("prompt", nargs="?")
    parser.add_argument("--last", action="store_true")
    parser.add_argument("--all", action="store_true", dest="all_cwds")
    _add_exec_options(parser)
    args = parser.parse_args(argv)

    if args.last and args.prompt is None:
        args.prompt = args.session_id
        args.session_id = None

    try:
        config = _build_exec_config(args)
        rollout_path = _resolve_resume_rollout(args, config)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if rollout_path is None:
        selector = args.session_id or "--last"
        print(f"No Codex rollout found for `{selector}`", file=sys.stderr)
        return 1
    session = CodexSession.fork_from_rollout(rollout_path, config)
    return _run_session(session, _read_prompt(args.prompt), json_events=args.json_events, color_mode=args.color)


def _main_chat(argv: list[str], *, prog: str = "python -m codex") -> int:
    parser = argparse.ArgumentParser(prog=prog)
    parser.add_argument("prompt", nargs="?")
    _add_exec_options(parser)
    args = parser.parse_args(argv)
    if args.json_events:
        print("`--json` is only supported for `exec`, not interactive chat.", file=sys.stderr)
        return 2

    try:
        config = _build_exec_config(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        session = CodexSession(config)
        initial_prompt = _normalize_optional_prompt(args.prompt)
        return _run_chat(session, initial_prompt, color_mode=args.color)
    except Exception as exc:
        _print_cli_error(exc)
        return 1


def _add_exec_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", "--experimental-json", action="store_true", dest="json_events")
    parser.add_argument("--model", "-m")
    parser.add_argument("--oss", action="store_true")
    parser.add_argument("--local-provider", dest="local_provider")
    parser.add_argument("--profile", "-p")
    parser.add_argument("--config", "-c", action="append", default=[], dest="config_overrides")
    parser.add_argument("--cd", "-C", dest="cwd")
    parser.add_argument("--sandbox", "-s", choices=["read-only", "workspace-write", "danger-full-access"])
    parser.add_argument("--add-dir", action="append", default=[], dest="add_dirs")
    parser.add_argument("--ask-for-approval", choices=["untrusted", "on-failure", "on-request", "never"])
    bypass_group = parser.add_mutually_exclusive_group()
    bypass_group.add_argument("--dangerously-bypass-approvals-and-sandbox", "--yolo", action="store_true")
    bypass_group.add_argument("--full-auto", action="store_true", dest="removed_full_auto", help=argparse.SUPPRESS)
    parser.add_argument("--dangerously-bypass-hook-trust", action="store_true", dest="bypass_hook_trust")
    parser.add_argument("--ignore-user-config", action="store_true")
    parser.add_argument("--ignore-rules", action="store_true")
    parser.add_argument("--skip-git-repo-check", action="store_true")
    parser.add_argument("--ephemeral", action="store_true")
    parser.add_argument("--color", choices=["auto", "always", "never"], default="auto")
    parser.add_argument("--output-last-message", "-o")
    parser.add_argument("--output-schema")
    parser.add_argument("--image", "-i", action="append", default=[], dest="images")


def _build_exec_config(args: argparse.Namespace) -> CodexConfig:
    if getattr(args, "removed_full_auto", False):
        print("warning: `--full-auto` is deprecated; use `--sandbox workspace-write` instead.", file=sys.stderr)
    output_schema = _load_output_schema(args.output_schema)
    cli_config = _load_cli_config(args)
    oss_provider = _resolve_oss_provider(args, cli_config)
    model = _exec_model(args, cli_config, oss_provider)
    model_provider_id = oss_provider or _string_config(cli_config, "model_provider") or "openai"
    model_provider_config = _model_provider_config(cli_config, model_provider_id)
    sandbox = "danger-full-access" if args.dangerously_bypass_approvals_and_sandbox else args.sandbox
    approval_policy = "never" if args.dangerously_bypass_approvals_and_sandbox else args.ask_for_approval
    web_search = _web_search_settings(cli_config)
    config = CodexConfig(
        model=model,
        model_provider_id=model_provider_id,
        session_source="exec" if getattr(args, "command", None) == "exec" else "cli",
        cwd=Path(args.cwd or _string_config(cli_config, "cwd") or "."),
        sandbox=sandbox or _sandbox_config(cli_config) or "workspace-write",
        approval_policy=approval_policy or _approval_config(cli_config) or "never",
        writable_roots=tuple(Path(path) for path in [*_path_list_config(cli_config, "writable_roots"), *args.add_dirs]),
        codex_home=_default_codex_home(),
        json_events=args.json_events,
        output_last_message=args.output_last_message,
        skip_git_repo_check=args.skip_git_repo_check,
        ephemeral=args.ephemeral,
        include_web_search_tool=web_search[0],
        web_search_external_web_access=web_search[1],
        memory_tool_enabled=_bool_config(cli_config, "memory_tool_enabled", False)
        or _bool_nested_config(cli_config, ("features", "memory_tool"), False),
        memory_generate_memories=_bool_nested_config(cli_config, ("memories", "generate_memories"), True),
        memory_disable_on_external_context=_bool_nested_config(
            cli_config,
            ("memories", "disable_on_external_context"),
            _bool_nested_config(cli_config, ("memories", "no_memories_if_mcp_or_web_search"), False),
        ),
        use_memories=_bool_nested_config(cli_config, ("memories", "use_memories"), True),
        memory_max_raw_memories_for_consolidation=_int_nested_config(
            cli_config, ("memories", "max_raw_memories_for_consolidation"), 256
        ),
        memory_max_unused_days=_int_nested_config(cli_config, ("memories", "max_unused_days"), 30),
        memory_max_rollout_age_days=_int_nested_config(cli_config, ("memories", "max_rollout_age_days"), 10),
        memory_max_rollouts_per_startup=_int_nested_config(cli_config, ("memories", "max_rollouts_per_startup"), 2),
        memory_min_rollout_idle_hours=_int_nested_config(cli_config, ("memories", "min_rollout_idle_hours"), 6),
        model_reasoning_effort=_string_config(cli_config, "model_reasoning_effort"),
        model_reasoning_summary=_string_config(cli_config, "model_reasoning_summary"),
        model_verbosity=_string_config(cli_config, "model_verbosity"),
        service_tier=_string_config(cli_config, "service_tier"),
        model_stream_max_retries=_int_config(model_provider_config, "stream_max_retries"),
        show_raw_agent_reasoning=bool(oss_provider) or _bool_config(cli_config, "show_raw_agent_reasoning", False),
        bypass_hook_trust=args.bypass_hook_trust or _bool_config(cli_config, "bypass_hook_trust", False),
        include_environment_context=_bool_config(cli_config, "include_environment_context", True),
        include_permissions_instructions=_bool_config(cli_config, "include_permissions_instructions", True),
        collaboration_mode=_collaboration_mode_config(cli_config),
        request_user_input_available_modes=_request_user_input_available_modes(cli_config),
        output_schema=output_schema,
        input_images=tuple(Path(path) for path in _parse_image_args(args.images)),
        remote_compaction=_remote_compaction_config(cli_config),
    )
    return config


def _run_session(session: CodexSession, prompt: str, *, json_events: bool, color_mode: str = "auto") -> int:
    if json_events:
        final = ""
        failed = False
        try:
            for event in session.stream(prompt):
                if event.type == "turn.completed":
                    final = str(event.payload.get("final_message", ""))
                elif event.type == "turn.failed":
                    failed = True
                print(event.to_json(), flush=True)
        except Exception as exc:
            if not failed:
                print(_session_failure_event(session, exc).to_json(), flush=True)
            return 1
        return 0 if final else 1

    return _run_session_human(session, prompt, color_mode=color_mode, print_final_to_stdout=True)


def _run_session_human(
    session: CodexSession,
    prompt: str,
    *,
    color_mode: str = "auto",
    print_final_to_stdout: bool,
    renderer: "_HumanEventRenderer | None" = None,
    install_request_user_input_provider: bool = True,
) -> int:
    if install_request_user_input_provider:
        _install_request_user_input_provider(
            session,
            _make_cli_request_user_input_provider(color_mode=color_mode),
        )
    renderer = renderer or _HumanEventRenderer(color_mode=color_mode)
    final = ""
    failed = False
    try:
        for event in session.stream(prompt):
            renderer.render(event)
            if event.type == "turn.completed":
                final = str(event.payload.get("final_message", ""))
            elif event.type == "turn.aborted":
                failed = True
            elif event.type == "turn.failed":
                failed = True
                renderer.render_error(str(event.payload.get("error") or "turn failed"))
    except TurnInterrupted:
        failed = True
        renderer.render_interrupted()
    except Exception as exc:
        if not failed:
            event = _session_failure_event(session, exc)
            renderer.render_error(str(event.payload.get("error") or "turn failed"))
    renderer.finish(final, print_to_stdout=print_final_to_stdout)
    return 0 if final else 1


def _run_chat(session: CodexSession, initial_prompt: str | None, *, color_mode: str = "auto") -> int:
    prompt = initial_prompt
    printed_transcript = False
    exit_status = 0
    queued_prompts: deque[str] = deque()
    while True:
        if prompt is None and queued_prompts:
            prompt = queued_prompts.popleft()
        if prompt is None:
            if printed_transcript:
                print(file=sys.stderr, flush=True)
            prompt = _read_interactive_prompt(color_mode=color_mode)
        if prompt is None:
            return exit_status
        slash_result = _handle_interactive_slash_command(
            session,
            prompt,
            color_mode=color_mode,
            queued_prompts=queued_prompts,
        )
        if slash_result.handled:
            if slash_result.session is not None:
                session = slash_result.session
            if slash_result.exit:
                return slash_result.status
            prompt = slash_result.prompt
            continue
        if prompt.strip():
            if _interactive_turn_controls_available():
                status = _run_session_human_interactive(
                    session,
                    prompt,
                    color_mode=color_mode,
                    queued_prompts=queued_prompts,
                )
            else:
                renderer = _HumanEventRenderer(color_mode=color_mode)
                renderer.render_user_message(prompt)
                status = _run_session_human(
                    session,
                    prompt,
                    color_mode=color_mode,
                    print_final_to_stdout=False,
                    renderer=renderer,
                )
            if status != 0:
                exit_status = status
            printed_transcript = True
        prompt = None


def _run_session_human_interactive(
    session: CodexSession,
    prompt: str,
    *,
    color_mode: str = "auto",
    queued_prompts: deque[str] | None = None,
) -> int:
    output: "queue.Queue[str]" = queue.Queue()
    status_tracker = _LiveTurnStatus()
    renderer = _HumanEventRenderer(color_mode=color_mode, line_sink=output.put, status_tracker=status_tracker)
    renderer.render_user_message(prompt)
    request_user_input_bridge = _RequestUserInputBridge()
    _install_request_user_input_provider(
        session,
        _make_cli_request_user_input_provider(
            color_mode=color_mode,
            bridge=request_user_input_bridge,
        ),
    )

    status: dict[str, int] = {"code": 1}

    def worker() -> None:
        status["code"] = _run_session_human(
            session,
            prompt,
            color_mode=color_mode,
            print_final_to_stdout=False,
            renderer=renderer,
            install_request_user_input_provider=False,
        )

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    _drain_output_queue(output)
    interrupted = False
    with _TurnInputReader(enabled=sys.stdin.isatty() and sys.stderr.isatty(), color_mode=color_mode) as reader:
        reader.set_status(status_tracker.snapshot(session))
        while thread.is_alive():
            _drain_output_queue(output, input_reader=reader)
            reader.set_status(status_tracker.snapshot(session))
            request = request_user_input_bridge.take_pending()
            if request is not None:
                reader.suspend()
                try:
                    response = _prompt_request_user_input(request.questions, color_mode=color_mode)
                finally:
                    reader.resume()
                request.resolve(response)
            for action in reader.poll():
                if action.kind == "interrupt" and not interrupted:
                    interrupted = True
                    session.interrupt()
                elif action.kind == "submit" and action.text.strip():
                    accepted = _submit_turn_input(session, action.text, queued_prompts=queued_prompts)
                    renderer.render_pending_input_preview(action.text, active=accepted)
                    _drain_output_queue(output, input_reader=reader)
            thread.join(timeout=0.03)
    thread.join(timeout=0.1)
    _drain_output_queue(output)
    return status["code"]


def _run_compact_human_interactive(
    session: CodexSession,
    *,
    color_mode: str = "auto",
    queued_prompts: deque[str] | None = None,
) -> int:
    output: "queue.Queue[str]" = queue.Queue()
    status_tracker = _LiveTurnStatus(header="Compacting")
    renderer = _HumanEventRenderer(color_mode=color_mode, line_sink=output.put, status_tracker=status_tracker)
    completed = {"value": False}
    failed = {"value": False}

    def worker() -> None:
        try:
            for event in session.stream_compact():
                if event.type == "item.completed" and event.payload.get("compact"):
                    status_tracker.update(event)
                    continue
                renderer.render(event)
                if event.type == "context_compaction.completed":
                    completed["value"] = True
                    renderer.render_info_message("Context compacted")
                elif event.type == "turn.failed":
                    failed["value"] = True
                    renderer.render_error(str(event.payload.get("error") or "turn failed"))
        except TurnInterrupted:
            failed["value"] = True
            renderer.render_interrupted()
        except Exception as exc:
            if not failed["value"]:
                event = _session_failure_event(session, exc)
                renderer.render_error(str(event.payload.get("error") or "turn failed"))

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    _drain_output_queue(output)
    interrupted = False
    with _TurnInputReader(enabled=sys.stdin.isatty() and sys.stderr.isatty(), color_mode=color_mode) as reader:
        reader.set_status(status_tracker.snapshot(session))
        while thread.is_alive():
            _drain_output_queue(output, input_reader=reader)
            reader.set_status(status_tracker.snapshot(session))
            for action in reader.poll():
                if action.kind == "interrupt" and not interrupted:
                    interrupted = True
                    session.interrupt()
                elif action.kind == "submit" and action.text.strip():
                    accepted = _submit_turn_input(session, action.text, queued_prompts=queued_prompts)
                    renderer.render_pending_input_preview(action.text, active=accepted)
                    _drain_output_queue(output, input_reader=reader)
            thread.join(timeout=0.03)
    thread.join(timeout=0.1)
    _drain_output_queue(output)
    return 0 if completed["value"] else 1


def _submit_turn_input(session: CodexSession, text: str, *, queued_prompts: deque[str] | None = None) -> bool:
    slash = _parse_interactive_slash(text)
    if slash is not None and not slash.command.available_during_task:
        if queued_prompts is not None:
            queued_prompts.append(text)
        else:
            session.queue_input_for_next_turn(text)
        return False
    try:
        session.steer_input(text)
        return True
    except SteerInputError:
        if queued_prompts is not None:
            queued_prompts.append(text)
        else:
            session.queue_input_for_next_turn(text)
        return False


def _set_session_collaboration_mode(session: CodexSession, mode: str) -> None:
    config = replace(session.config, collaboration_mode=mode)
    session.config = config
    session.state.config = config
    session.tools.config = config


def _list_known_models() -> list[dict[str, Any]]:
    _model_catalog_info("__warmup__")
    cache = getattr(_types, "_MODEL_CATALOG_CACHE", None) or {}
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for slug, entry in cache.items():
        if slug in seen:
            continue
        seen.add(slug)
        out.append(entry)
    out.sort(key=lambda e: str(e.get("slug", "")))
    return out


def _model_supported_efforts(model: str) -> list[str]:
    info = _model_catalog_info(model)
    raw = info.get("supported_reasoning_levels")
    efforts: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and isinstance(item.get("effort"), str):
                efforts.append(item["effort"])
            elif isinstance(item, str):
                efforts.append(item)
    return efforts


def _set_session_model(session: CodexSession, model: str, effort: str | None) -> None:
    config = replace(session.config, model=model, model_reasoning_effort=effort)
    session.config = config
    session.state.config = config
    session.tools.config = config


def _interactive_model_picker(
    models: list[dict[str, Any]],
    current_model: str,
    current_effort: str,
    *,
    color_mode: str = "auto",
) -> tuple[str, str] | None:
    if not (sys.stdin.isatty() and sys.stderr.isatty()):
        for i, entry in enumerate(models, 1):
            slug = entry.get("slug", "")
            efforts = _model_supported_efforts(slug) or [str(entry.get("default_reasoning_level") or "medium")]
            marker = " *" if slug == current_model else ""
            print(f"  [{i}] {slug}{marker}  efforts: {', '.join(efforts)}", file=sys.stderr, flush=True)
        return None

    try:
        fd = sys.stdin.fileno()
        old_attrs = termios.tcgetattr(fd)
    except Exception:
        return None

    style = _AnsiStyle(_should_use_color(color_mode))
    rows: list[tuple[str, list[str]]] = []
    for entry in models:
        slug = str(entry.get("slug", ""))
        efforts = _model_supported_efforts(slug)
        if not efforts:
            default_level = entry.get("default_reasoning_level")
            efforts = [str(default_level)] if default_level else ["medium"]
        rows.append((slug, efforts))

    selected = next((i for i, (s, _e) in enumerate(rows) if s == current_model), 0)
    effort_idx = [0] * len(rows)
    for i, (slug, efforts) in enumerate(rows):
        if slug == current_model and current_effort in efforts:
            effort_idx[i] = efforts.index(current_effort)
        else:
            default_level = str(_model_catalog_info(slug).get("default_reasoning_level") or "medium")
            if default_level in efforts:
                effort_idx[i] = efforts.index(default_level)

    rendered_rows = 0

    def render(lines: list[str]) -> None:
        nonlocal rendered_rows
        cols = _terminal_columns()
        _clear_prompt_lines(rendered_rows)
        print("\n".join(lines), end="", file=sys.stderr, flush=True)
        rendered_rows = _prompt_screen_rows(lines, cols)

    def clear() -> None:
        nonlocal rendered_rows
        _clear_prompt_lines(rendered_rows)
        rendered_rows = 0

    def lines() -> list[str]:
        out = [
            f"{style.bold('Select model')}  (current: {current_model} / {current_effort})",
            f"{style.dim('  ↑/↓ model  ←/→ reasoning effort  Enter confirm  Esc cancel')}",
            "",
        ]
        for i, (slug, efforts) in enumerate(rows):
            cur_eff = efforts[effort_idx[i]]
            arrows = []
            if len(efforts) > 1:
                arrows.append("◂" if effort_idx[i] > 0 else " ")
                arrows.append("▸" if effort_idx[i] < len(efforts) - 1 else " ")
            else:
                arrows = [" ", " "]
            eff_part = f"{arrows[0]} {cur_eff} {arrows[1]}" if len(efforts) > 1 else f"  {cur_eff}  "
            marker = "*" if slug == current_model else " "
            line = f"  {marker} {slug:<20} {eff_part}"
            if i == selected:
                line = style.bold("> " + line.lstrip())
            else:
                line = "  " + line.lstrip()
            out.append(line)
        return out

    try:
        _set_raw_keep_opost(fd)
        pending = b""
        while True:
            render(lines())
            chunk, pending = _read_tty_chunk(fd, pending)
            if chunk == b"":
                return None
            if chunk == b"\x03":
                return None
            if chunk in {b"\r", b"\n"}:
                slug, efforts = rows[selected]
                return slug, efforts[effort_idx[selected]]
            if chunk == b"\x1b":
                sequence, pending = _read_escape_sequence(fd, pending)
                if sequence == b"\x1b":
                    return None
                if sequence in {b"\x1b[A", b"\x1bOA"}:
                    selected = (selected - 1) % len(rows)
                elif sequence in {b"\x1b[B", b"\x1bOB"}:
                    selected = (selected + 1) % len(rows)
                elif sequence in {b"\x1b[D", b"\x1bOD"}:
                    _slug, efforts = rows[selected]
                    if len(efforts) > 1:
                        effort_idx[selected] = (effort_idx[selected] - 1) % len(efforts)
                elif sequence in {b"\x1b[C", b"\x1bOC"}:
                    _slug, efforts = rows[selected]
                    if len(efforts) > 1:
                        effort_idx[selected] = (effort_idx[selected] + 1) % len(efforts)
                continue
            if chunk == b"q":
                return None
    except KeyboardInterrupt:
        return None
    finally:
        clear()
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
        except Exception:
            pass


def _handle_model_slash(session: CodexSession, rest: str, *, color_mode: str = "auto") -> None:
    tokens = rest.split()
    current_model = session.config.model
    current_effort = session.config.model_reasoning_effort or _model_catalog_info(current_model).get("default_reasoning_level") or "medium"
    models = _list_known_models()

    if not tokens:
        if not models:
            print("No model catalog entries found.", file=sys.stderr, flush=True)
            return
        choice = _interactive_model_picker(models, current_model, current_effort, color_mode=color_mode)
        if choice is None:
            print("Cancelled.", file=sys.stderr, flush=True)
            return
        slug, effort = choice
        _set_session_model(session, slug, effort)
        print(f"Model set to {slug} (reasoning effort: {effort}).", file=sys.stderr, flush=True)
        return

    if tokens[0] in {"effort", "reasoning"} and len(tokens) == 2:
        effort = tokens[1]
        supported = _model_supported_efforts(current_model)
        if supported and effort not in supported:
            print(f"Effort '{effort}' not supported by {current_model}. Supported: {', '.join(supported)}", file=sys.stderr, flush=True)
            return
        _set_session_model(session, current_model, effort)
        print(f"Reasoning effort set to {effort} (model: {current_model}).", file=sys.stderr, flush=True)
        return

    name = tokens[0]
    info = _model_catalog_info(name)
    known_slugs = {str(e.get("slug", "")) for e in models}
    if known_slugs and name not in known_slugs:
        print(f"Unknown model '{name}'. Known: {', '.join(sorted(known_slugs))}", file=sys.stderr, flush=True)
        return
    if len(tokens) == 1:
        supported = _model_supported_efforts(name)
        if current_effort in supported:
            effort = current_effort
        else:
            effort = str(info.get("default_reasoning_level") or "medium")
    elif len(tokens) == 2:
        effort = tokens[1]
        supported = _model_supported_efforts(name)
        if supported and effort not in supported:
            print(f"Effort '{effort}' not supported by {name}. Supported: {', '.join(supported)}", file=sys.stderr, flush=True)
            return
    else:
        print("Usage: /model | /model <name> [<effort>] | /model effort <effort>", file=sys.stderr, flush=True)
        return
    _set_session_model(session, name, effort)
    print(f"Model set to {name} (reasoning effort: {effort}).", file=sys.stderr, flush=True)


class _RequestUserInputRequest:
    def __init__(self, questions: list[dict[str, Any]]) -> None:
        self.questions = questions
        self._event = threading.Event()
        self._response: dict[str, Any] | None = None

    def resolve(self, response: dict[str, Any] | None) -> None:
        self._response = response
        self._event.set()

    def wait(self) -> dict[str, Any] | None:
        self._event.wait()
        return self._response


class _RequestUserInputBridge:
    def __init__(self) -> None:
        self._requests: "queue.Queue[_RequestUserInputRequest]" = queue.Queue()

    def ask(self, questions: list[dict[str, Any]]) -> dict[str, Any] | None:
        request = _RequestUserInputRequest(questions)
        self._requests.put(request)
        return request.wait()

    def take_pending(self) -> _RequestUserInputRequest | None:
        try:
            return self._requests.get_nowait()
        except queue.Empty:
            return None


def _make_cli_request_user_input_provider(
    *,
    color_mode: str,
    bridge: _RequestUserInputBridge | None = None,
) -> Callable[[list[dict[str, Any]]], dict[str, Any] | None]:
    def provider(questions: list[dict[str, Any]]) -> dict[str, Any] | None:
        if bridge is not None:
            return bridge.ask(questions)
        return _prompt_request_user_input(questions, color_mode=color_mode)

    setattr(provider, "_python_codex_cli_request_user_input_provider", True)
    return provider


def _install_request_user_input_provider(
    session: CodexSession,
    provider: Callable[[list[dict[str, Any]]], dict[str, Any] | None],
) -> None:
    current = session.config.request_user_input_provider
    if current is not None and not getattr(current, "_python_codex_cli_request_user_input_provider", False):
        return
    config = replace(session.config, request_user_input_provider=provider)
    session.config = config
    session.state.config = config
    session.tools.config = config


_REQUEST_USER_INPUT_OTHER_LABEL = "None of the above"
_REQUEST_USER_INPUT_OTHER_DESCRIPTION = "Optionally, add details in notes (tab)."


@dataclass(frozen=True)
class _RequestUserInputOption:
    label: str
    description: str
    is_other: bool = False


def _prompt_request_user_input(questions: list[dict[str, Any]], *, color_mode: str = "auto") -> dict[str, Any] | None:
    if not sys.stdin.isatty() or not sys.stderr.isatty():
        return None
    style = _AnsiStyle(_should_use_color(color_mode))
    try:
        fd = sys.stdin.fileno()
        old_attrs = termios.tcgetattr(fd)
    except Exception:
        return None

    answers: dict[str, dict[str, list[str]]] = {}
    rendered_rows = 0

    def render(lines: list[str]) -> None:
        nonlocal rendered_rows
        cols = _terminal_columns()
        _clear_prompt_lines(rendered_rows)
        print("\n".join(lines), end="", file=sys.stderr, flush=True)
        rendered_rows = _prompt_screen_rows(lines, cols)

    def clear() -> None:
        nonlocal rendered_rows
        _clear_prompt_lines(rendered_rows)
        rendered_rows = 0

    try:
        _set_raw_keep_opost(fd)
        sys.stderr.write("\033[?2004h")
        sys.stderr.flush()
        pending = b""
        for index, question in enumerate(questions, start=1):
            if not isinstance(question, dict):
                continue
            question_id = str(question.get("id") or "")
            if not question_id:
                continue
            result, pending = _read_request_user_input_answer(
                question,
                question_index=index,
                question_count=len(questions),
                fd=fd,
                pending=pending,
                style=style,
                render=render,
            )
            if result is None:
                return None
            answers[question_id] = {"answers": result}
        clear()
        return {"answers": answers}
    except KeyboardInterrupt:
        raise
    finally:
        clear()
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
        except Exception:
            pass
        sys.stderr.write("\033[?2004l")
        sys.stderr.flush()


def _read_request_user_input_answer(
    question: dict[str, Any],
    *,
    question_index: int,
    question_count: int,
    fd: int,
    pending: bytes,
    style: "_AnsiStyle",
    render: Callable[[list[str]], None],
) -> tuple[list[str] | None, bytes]:
    options = _request_user_input_options(question)
    selected = 0
    notes = ""
    mode = "options" if options else "notes"
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    while True:
        render(
            _request_user_input_selector_lines(
                question,
                options=options,
                selected=selected,
                notes=notes,
                mode=mode,
                question_index=question_index,
                question_count=question_count,
                style=style,
            )
        )
        chunk, pending = _read_tty_chunk(fd, pending)
        if chunk == b"":
            return None, pending
        if chunk == b"\x03":
            raise KeyboardInterrupt
        if chunk in {b"\x7f", b"\b"}:
            if mode == "notes" and notes:
                notes = notes[:-1]
            elif mode == "notes" and options:
                mode = "options"
            continue
        if chunk == b"\t":
            if options:
                mode = "notes" if mode == "options" else "options"
            continue
        if chunk in {b"\r", b"\n"}:
            tail = decoder.decode(b"", final=True)
            if tail:
                notes += tail.replace("\r\n", "\n").replace("\r", "\n")
            decoder = codecs.getincrementaldecoder("utf-8")("replace")
            if chunk == b"\n" and mode == "notes":
                notes += "\n"
                continue
            if mode == "options" and options:
                if options[selected].is_other:
                    mode = "notes"
                    continue
                return _request_user_input_values_for_selection(options[selected], notes), pending
            if options:
                return _request_user_input_values_for_selection(options[selected], notes), pending
            text = notes.strip()
            return ([text] if text else []), pending
        if chunk == b"\x1b":
            sequence, pending = _read_escape_sequence(fd, pending)
            if sequence.startswith(b"\x1b[200~"):
                pasted, pending = _read_bracketed_paste(fd, sequence, pending)
                notes += pasted.decode("utf-8", errors="replace").replace("\r\n", "\n").replace("\r", "\n")
                mode = "notes"
                continue
            if sequence in {b"\x1b[A", b"\x1bOA"} and options:
                selected = (selected - 1) % len(options)
                continue
            if sequence in {b"\x1b[B", b"\x1bOB"} and options:
                selected = (selected + 1) % len(options)
                continue
            if sequence == b"\x1b":
                if mode == "notes" and notes:
                    notes = ""
                    mode = "options" if options else "notes"
                    continue
                return None, pending
            continue
        text = decoder.decode(chunk, final=False)
        if not text:
            continue
        if mode == "options" and options:
            stripped = text.strip()
            if len(stripped) == 1 and stripped.isdigit():
                index = int(stripped) - 1
                if 0 <= index < len(options):
                    selected = index
                    if options[selected].is_other:
                        mode = "notes"
                    continue
            mode = "notes"
        notes += text.replace("\r\n", "\n").replace("\r", "\n")


def _request_user_input_options(question: dict[str, Any]) -> list[_RequestUserInputOption]:
    options: list[_RequestUserInputOption] = []
    raw_options = question.get("options")
    if isinstance(raw_options, list):
        for option in raw_options:
            if not isinstance(option, dict):
                continue
            label = str(option.get("label") or "").strip()
            if not label:
                continue
            options.append(
                _RequestUserInputOption(
                    label=label,
                    description=str(option.get("description") or "").strip(),
                )
            )
    if options and question.get("isOther"):
        options.append(
            _RequestUserInputOption(
                label=_REQUEST_USER_INPUT_OTHER_LABEL,
                description=_REQUEST_USER_INPUT_OTHER_DESCRIPTION,
                is_other=True,
            )
        )
    return options


def _request_user_input_selector_lines(
    question: dict[str, Any],
    *,
    options: list[_RequestUserInputOption],
    selected: int,
    notes: str,
    mode: str,
    question_index: int,
    question_count: int,
    style: "_AnsiStyle",
) -> list[str]:
    lines = [f"{style.dim('•')} {style.bold('Questions')}"]
    if question_count > 1:
        lines.append(f"  {style.dim(f'Question {question_index}/{question_count}')}")
    header = str(question.get("header") or "").strip()
    text = str(question.get("question") or "").strip()
    if header:
        lines.append(f"  {style.bold(header)}")
    if text:
        wrapped_question = textwrap.wrap(text, width=max(20, _terminal_columns() - 4)) or [text]
        lines.extend(f"  {line}" for line in wrapped_question)
    for index, option in enumerate(options):
        marker = "›" if index == selected else " "
        number = index + 1
        label = style.cyan(option.label) if index == selected else option.label
        prefix = f"  {marker} {number}. "
        row = f"{prefix}{label}"
        if option.description:
            row = f"{row} {style.dim(option.description)}"
        wrapped_row = _wrap_ansi_line(row, max(20, _terminal_columns()))
        if len(wrapped_row) > 1:
            indent = " " * _visible_width(prefix)
            wrapped_row = [wrapped_row[0], *[f"{indent}{line}" for line in wrapped_row[1:]]]
        lines.extend(wrapped_row)
    if mode == "notes":
        label = "Other" if options and options[selected].is_other else "Notes"
        visible_notes = "*" * len(notes) if question.get("isSecret") else notes
        note_lines = visible_notes.split("\n") or [""]
        first = note_lines[0]
        lines.append(f"  {style.bold(label + ':')} {first}")
        lines.extend(f"    {line}" for line in note_lines[1:])
    tips = "↑/↓ select | enter choose | tab notes | esc cancel"
    if mode == "notes":
        tips = "enter submit | tab choices | esc clear/cancel"
    lines.append(f"  {style.dim(tips)}")
    return lines


def _request_user_input_values_for_selection(option: _RequestUserInputOption, notes: str) -> list[str]:
    values = [option.label]
    note = notes.strip()
    if note:
        values.append(f"user_note: {note}")
    return values


def _request_user_input_question_lines(question: dict[str, Any], style: "_AnsiStyle") -> list[str]:
    lines: list[str] = []
    header = str(question.get("header") or "").strip()
    text = str(question.get("question") or "").strip()
    if header:
        lines.append(f"  {style.bold(header)}")
    if text:
        lines.append(f"  {text}")
    for index, option in enumerate(_request_user_input_options(question), start=1):
        if option.description:
            lines.append(f"    {index}. {style.cyan(option.label)} {style.dim(option.description)}")
        else:
            lines.append(f"    {index}. {style.cyan(option.label)}")
    return lines


def _request_user_input_answer_values(question: dict[str, Any], answer_text: str) -> list[str]:
    text = answer_text.strip()
    if not text:
        return []
    labels = [option.label for option in _request_user_input_options(question)]
    if labels and text.isdigit():
        index = int(text) - 1
        if 0 <= index < len(labels):
            return [labels[index]]
    if labels:
        return [f"user_note: {text}"]
    return [text]


def _request_user_input_answer_list(answer: Any) -> list[str]:
    if not isinstance(answer, dict):
        return []
    values = answer.get("answers")
    if not isinstance(values, list):
        return []
    return [str(value) for value in values if isinstance(value, str) and value]


def _split_request_user_input_answer_values(values: list[str]) -> tuple[list[str], str | None]:
    options: list[str] = []
    note: str | None = None
    for value in values:
        if value.startswith("user_note: "):
            note = value[len("user_note: ") :]
        else:
            options.append(value)
    return options, note


def _drain_output_queue(output: "queue.Queue[str]", *, input_reader: "_TurnInputReader | None" = None) -> None:
    printed = False
    while True:
        try:
            line = output.get_nowait()
        except queue.Empty:
            if printed and input_reader is not None:
                input_reader.render()
            return
        if input_reader is not None:
            input_reader.clear()
        print(line, file=sys.stderr, flush=True)
        printed = True


@dataclass(frozen=True)
class _TurnInputAction:
    kind: str
    text: str = ""


@dataclass(frozen=True)
class _LiveTurnStatusSnapshot:
    header: str
    elapsed_seconds: int
    active_context_tokens: int | None
    active_context_estimated: bool
    session_context_tokens: int | None
    session_context_estimated: bool
    session_reasoning_tokens: int | None
    context_window: int | None


class _LiveTurnStatus:
    def __init__(self, *, header: str = "Working") -> None:
        self._started_at = time.monotonic()
        self._header = header
        self._lock = threading.Lock()

    def update(self, event: Any) -> None:
        event_type = getattr(event, "type", "")
        payload = getattr(event, "payload", {})
        payload = payload if isinstance(payload, dict) else {}
        with self._lock:
            if event_type == "context_compaction.started":
                self._header = "Compacting"
            elif event_type == "context_compaction.completed":
                self._header = "Working"
            elif event_type == "model.request":
                self._header = "Compacting" if payload.get("compact") else "Working"
            elif event_type == "stream_error":
                self._header = "Reconnecting"
            elif event_type in {"tool.started", "tool.completed", "item.completed", "model.response"} and not payload.get("compact"):
                if self._header in {"Compacting", "Reconnecting"}:
                    self._header = "Working"

    def snapshot(self, session: CodexSession) -> _LiveTurnStatusSnapshot:
        with self._lock:
            header = self._header
            elapsed = max(0, int(time.monotonic() - self._started_at))
        (
            active_context,
            active_context_estimated,
            session_context,
            session_context_estimated,
            session_reasoning,
            context_window,
        ) = _session_context_status(session)
        return _LiveTurnStatusSnapshot(
            header=header,
            elapsed_seconds=elapsed,
            active_context_tokens=active_context,
            active_context_estimated=active_context_estimated,
            session_context_tokens=session_context,
            session_context_estimated=session_context_estimated,
            session_reasoning_tokens=session_reasoning,
            context_window=context_window,
        )


class _TurnInputReader:
    def __init__(self, *, enabled: bool, color_mode: str = "auto") -> None:
        self.enabled = enabled
        self._fd: int | None = None
        self._old_attrs: list[Any] | None = None
        self._style = _AnsiStyle(_should_use_color(color_mode))
        self._buffer = ""
        self._cursor = 0
        self._pending = b""
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self._rendered_lines = 0
        self._status_lines: list[str] = []
        self._status_key = ""
        self._defer_render = False
        self._dirty = False

    def __enter__(self) -> "_TurnInputReader":
        if not self.enabled:
            return self
        try:
            self._fd = sys.stdin.fileno()
            self._old_attrs = termios.tcgetattr(self._fd)
            _set_raw_keep_opost(self._fd)
            sys.stderr.write("\033[?2004h")
            sys.stderr.flush()
        except Exception:
            self.enabled = False
            self._fd = None
            self._old_attrs = None
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.clear()
        if self._fd is not None and self._old_attrs is not None:
            try:
                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_attrs)
            except Exception:
                pass
        if self.enabled:
            sys.stderr.write("\033[?2004l")
            sys.stderr.flush()

    def poll(self) -> list[_TurnInputAction]:
        if not self.enabled or self._fd is None:
            return []
        actions: list[_TurnInputAction] = []
        self._defer_render = True
        try:
            while True:
                try:
                    readable, _, _ = select.select([self._fd], [], [], 0)
                except Exception:
                    break
                if not readable and not self._pending:
                    break
                chunk, self._pending = _read_tty_chunk(self._fd, self._pending)
                if chunk == b"":
                    break
                action = self._handle_chunk(chunk)
                if action is not None:
                    actions.append(action)
        finally:
            self._defer_render = False
            if self._dirty:
                self.render()
        return actions

    def render(self) -> None:
        if not self.enabled:
            return
        prompt_lines = _prompt_display_lines(self._buffer, self._style)
        lines = [*self._status_lines, *prompt_lines]
        cols = _terminal_columns()
        self.clear()
        print("\n".join(lines), end="", file=sys.stderr, flush=True)
        self._rendered_lines = _prompt_screen_rows(lines, cols)
        status_rows = _prompt_screen_rows(self._status_lines, cols)
        _move_prompt_cursor(self._buffer, self._cursor, self._rendered_lines, cols, prefix_rows=status_rows)
        self._dirty = False

    def set_status(self, snapshot: _LiveTurnStatusSnapshot | None) -> None:
        if not self.enabled:
            return
        lines = _live_status_display_lines(snapshot, self._style) if snapshot is not None else []
        key = "\n".join(lines)
        if key == self._status_key:
            return
        self._status_lines = lines
        self._status_key = key
        self._request_render()

    def clear(self) -> None:
        if not self.enabled:
            return
        _clear_prompt_lines(self._rendered_lines)
        self._rendered_lines = 0
        self._dirty = False

    def suspend(self) -> None:
        if not self.enabled or self._fd is None:
            return
        self.clear()
        if self._old_attrs is not None:
            try:
                termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old_attrs)
            except Exception:
                pass
        sys.stderr.write("\033[?2004l")
        sys.stderr.flush()

    def resume(self) -> None:
        if not self.enabled or self._fd is None:
            return
        try:
            _set_raw_keep_opost(self._fd)
        except Exception:
            return
        sys.stderr.write("\033[?2004h")
        sys.stderr.flush()
        self.render()

    def _request_render(self) -> None:
        self._dirty = True
        if not self._defer_render:
            self.render()

    def _handle_chunk(self, chunk: bytes) -> _TurnInputAction | None:
        if chunk == b"\x03":
            raise KeyboardInterrupt
        if chunk in {b"\x7f", b"\b"}:
            if self._cursor > 0:
                self._buffer = self._buffer[: self._cursor - 1] + self._buffer[self._cursor :]
                self._cursor -= 1
                self._request_render()
            return None
        if chunk == b"\r":
            tail = self._decoder.decode(b"", final=True)
            if tail:
                self._insert(tail)
            text = _normalize_optional_prompt(self._buffer) or ""
            self._buffer = ""
            self._cursor = 0
            self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
            # Force an immediate redraw of the (now empty) prompt so the
            # transcript above remains clean before the caller prints output.
            self.render()
            return _TurnInputAction("submit", text)
        if chunk == b"\n":
            self._insert("\n")
            return None
        if chunk == b"\x1b":
            sequence, self._pending = _read_escape_sequence(self._fd, self._pending)
            if sequence.startswith(b"\x1b[200~"):
                pasted, self._pending = _read_bracketed_paste(self._fd, sequence, self._pending)
                self._insert(pasted.decode("utf-8", errors="replace"))
                return None
            handled = self._handle_escape_sequence(sequence)
            if handled:
                return None
            if sequence == b"\x1b":
                return _TurnInputAction("interrupt")
            return None
        text = self._decoder.decode(chunk, final=False)
        if text:
            self._insert(text)
        return None

    def _insert(self, text: str) -> None:
        if not text:
            return
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        self._buffer = self._buffer[: self._cursor] + normalized + self._buffer[self._cursor :]
        self._cursor += len(normalized)
        self._request_render()

    def _handle_escape_sequence(self, sequence: bytes) -> bool:
        updated = _apply_prompt_escape_sequence(self._buffer, self._cursor, sequence)
        if updated is None:
            return False
        self._buffer, self._cursor = updated
        self._request_render()
        return True


def _interactive_turn_controls_available() -> bool:
    return sys.stdin.isatty() and sys.stderr.isatty()


def _run_compact_human(session: CodexSession, *, color_mode: str = "auto") -> int:
    renderer = _HumanEventRenderer(color_mode=color_mode)
    completed = False
    failed = False
    try:
        for event in session.stream_compact():
            if event.type == "item.completed" and event.payload.get("compact"):
                continue
            if event.type == "context_compaction.completed":
                completed = True
                renderer.render_info_message("Context compacted")
            elif event.type == "turn.failed":
                failed = True
                renderer.render_error(str(event.payload.get("error") or "turn failed"))
            else:
                renderer.render(event)
    except Exception as exc:
        if not failed:
            event = _session_failure_event(session, exc)
            renderer.render_error(str(event.payload.get("error") or "turn failed"))
    return 0 if completed else 1


def _session_failure_event(session: CodexSession, exc: Exception) -> CodexEvent:
    message = _exception_display_message(exc)
    try:
        return session.state.emit("turn.failed", error=message)
    except Exception:
        return CodexEvent("turn.failed", {"error": message})


def _exception_display_message(exc: Exception) -> str:
    message = str(exc).strip()
    if not message:
        return type(exc).__name__
    if isinstance(exc, RuntimeError):
        return message
    return f"{type(exc).__name__}: {message}"


def _print_cli_error(exc: Exception) -> None:
    print(f"ERROR: {_exception_display_message(exc)}", file=sys.stderr)


def _read_interactive_prompt(*, color_mode: str = "auto") -> str | None:
    if sys.stdin.isatty() and sys.stderr.isatty():
        prompt = _read_tty_prompt(color_mode=color_mode)
        if prompt is not None:
            return prompt

    style = _AnsiStyle(_should_use_color(color_mode))
    if sys.stdin.isatty() and sys.stderr.isatty():
        print(f"{style.bold('›')} ", end="", file=sys.stderr, flush=True)
    else:
        print(f"{style.bold('›')} ", end="", file=sys.stderr, flush=True)
    line = sys.stdin.readline()
    if line == "":
        if sys.stdin.isatty() and sys.stderr.isatty():
            print(file=sys.stderr)
        return None
    if sys.stdin.isatty() and sys.stderr.isatty():
        print("\033[1A\033[2K", end="", file=sys.stderr, flush=True)
    return line.rstrip("\n").replace("\r\n", "\n").replace("\r", "\n")


def _read_tty_prompt(*, color_mode: str = "auto") -> str | None:
    style = _AnsiStyle(_should_use_color(color_mode))
    try:
        fd = sys.stdin.fileno()
        old_attrs = termios.tcgetattr(fd)
    except Exception:
        return None

    buffer = ""
    cursor = 0
    rendered_lines = 0
    state = {"dirty": False, "defer": False}

    def render() -> None:
        nonlocal rendered_lines
        lines = _prompt_display_lines(buffer, style)
        cols = _terminal_columns()
        _clear_prompt_lines(rendered_lines)
        print("\n".join(lines), end="", file=sys.stderr, flush=True)
        rendered_lines = _prompt_screen_rows(lines, cols)
        _move_prompt_cursor(buffer, cursor, rendered_lines, cols)
        state["dirty"] = False

    def request_render() -> None:
        state["dirty"] = True
        if not state["defer"]:
            render()

    sys.stderr.write("\033[?2004h")
    sys.stderr.flush()
    try:
        _set_raw_keep_opost(fd)
        render()
        pending = b""
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        while True:
            # Drain everything currently available in one burst before
            # rendering, so a multi-kilobyte paste only redraws once.
            state["defer"] = True
            try:
                while True:
                    chunk, pending = _read_tty_chunk(fd, pending)
                    if chunk == b"":
                        if state["dirty"]:
                            render()
                        _clear_prompt_lines(rendered_lines)
                        print(file=sys.stderr, flush=True)
                        return None
                    if chunk == b"\x03":
                        raise KeyboardInterrupt
                    if chunk == b"\x04" and not buffer:
                        if state["dirty"]:
                            render()
                        _clear_prompt_lines(rendered_lines)
                        return None
                    if chunk in {b"\x7f", b"\b"}:
                        if cursor > 0:
                            buffer = buffer[: cursor - 1] + buffer[cursor:]
                            cursor -= 1
                            state["dirty"] = True
                        if not _has_pending_input(fd, pending):
                            break
                        continue
                    if chunk == b"\r":
                        tail = decoder.decode(b"", final=True)
                        if tail:
                            normalized = tail.replace("\r\n", "\n").replace("\r", "\n")
                            buffer = buffer[:cursor] + normalized + buffer[cursor:]
                            cursor += len(normalized)
                        result = _normalize_optional_prompt(buffer) or ""
                        _clear_prompt_lines(rendered_lines)
                        rendered_lines = 0
                        return result
                    if chunk == b"\n":
                        buffer = buffer[:cursor] + "\n" + buffer[cursor:]
                        cursor += 1
                        state["dirty"] = True
                        if not _has_pending_input(fd, pending):
                            break
                        continue
                    if chunk == b"\x1b":
                        sequence, pending = _read_escape_sequence(fd, pending)
                        if sequence.startswith(b"\x1b[200~"):
                            pasted, pending = _read_bracketed_paste(fd, sequence, pending)
                            decoded = pasted.decode("utf-8", errors="replace")
                            normalized = decoded.replace("\r\n", "\n").replace("\r", "\n")
                            buffer = buffer[:cursor] + normalized + buffer[cursor:]
                            cursor += len(normalized)
                            state["dirty"] = True
                        else:
                            updated = _apply_prompt_escape_sequence(buffer, cursor, sequence)
                            if updated is not None:
                                buffer, cursor = updated
                                state["dirty"] = True
                        if sequence.startswith(b"\x1b[200~"):
                            state["dirty"] = True
                        if not _has_pending_input(fd, pending):
                            break
                        continue
                    decoded = decoder.decode(chunk, final=False)
                    if decoded:
                        normalized = decoded.replace("\r\n", "\n").replace("\r", "\n")
                        buffer = buffer[:cursor] + normalized + buffer[cursor:]
                        cursor += len(normalized)
                        state["dirty"] = True
                    if not _has_pending_input(fd, pending):
                        break
            finally:
                state["defer"] = False
            if state["dirty"]:
                render()
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
        except Exception:
            pass
        sys.stderr.write("\033[?2004l")
        sys.stderr.flush()


def _prompt_display_lines(text: str, style: "_AnsiStyle") -> list[str]:
    if text == "":
        return [f"{style.bold('›')} "]
    lines = text.split("\n")
    rendered = [f"{style.bold('›')} {lines[0]}"]
    rendered.extend(f"  {line}" for line in lines[1:])
    return rendered


def _live_status_display_lines(snapshot: _LiveTurnStatusSnapshot | None, style: "_AnsiStyle") -> list[str]:
    if snapshot is None:
        return []
    elapsed = _format_elapsed_compact(snapshot.elapsed_seconds)
    parts = [
        f"{style.dim('•')} {style.bold(snapshot.header)} "
        f"{style.dim(f'({elapsed} • esc to interrupt)')}",
    ]
    metric_parts: list[str] = []
    if snapshot.active_context_tokens is not None:
        ctx = _format_tokens_compact(snapshot.active_context_tokens)
        if snapshot.context_window:
            ctx = f"{ctx}/{_format_tokens_compact(snapshot.context_window)}"
        metric_parts.append(f"ctx {ctx}")
    if snapshot.session_context_tokens is not None:
        metric_parts.append(
            f"session {_format_tokens_compact(snapshot.session_context_tokens)}"
        )
    if snapshot.session_reasoning_tokens is not None:
        metric_parts.append(f"reasoning {_format_tokens_compact(snapshot.session_reasoning_tokens)}")
    if metric_parts:
        parts.append(style.dim(" · " + " · ".join(metric_parts)))
    line = "".join(parts)
    width = _terminal_columns()
    wrapped = _wrap_ansi_line(line, max(20, width))
    if len(wrapped) <= 1:
        return wrapped
    return [wrapped[0], *[f"  {style.dim(line)}" for line in wrapped[1:]]]


def _session_context_status(session: CodexSession) -> tuple[int | None, bool, int | None, bool, int | None, int | None]:
    active_context: int | None = None
    active_context_estimated = True
    session_context: int | None = None
    session_context_estimated = True
    session_reasoning: int | None = None
    context_window: int | None = None
    try:
        active_context, active_context_estimated = session.state.active_context_token_status()
    except Exception:
        pass
    try:
        session_context, session_context_estimated = session.state.session_context_token_status()
    except Exception:
        pass
    try:
        session_reasoning = session.state.session_reasoning_usage_tokens()
    except Exception:
        pass
    try:
        context_window = session.config.resolved_model_context_window()
    except Exception:
        pass
    return active_context, active_context_estimated, session_context, session_context_estimated, session_reasoning, context_window


def _format_elapsed_compact(elapsed_seconds: int) -> str:
    seconds = max(0, int(elapsed_seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        minutes = seconds // 60
        remainder = seconds % 60
        return f"{minutes}m {remainder:02}s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    remainder = seconds % 60
    return f"{hours}h {minutes:02}m {remainder:02}s"


def _format_tokens_compact(value: int | float) -> str:
    value = max(0, int(value))
    if value == 0:
        return "0"
    if value < 1_000:
        return str(value)
    scaled = float(value)
    suffix = "K"
    if value >= 1_000_000_000_000:
        scaled = scaled / 1_000_000_000_000.0
        suffix = "T"
    elif value >= 1_000_000_000:
        scaled = scaled / 1_000_000_000.0
        suffix = "B"
    elif value >= 1_000_000:
        scaled = scaled / 1_000_000.0
        suffix = "M"
    else:
        scaled = scaled / 1_000.0
    decimals = 2 if scaled < 10.0 else 1 if scaled < 100.0 else 0
    formatted = f"{scaled:.{decimals}f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return f"{formatted}{suffix}"


_ANSI_CSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _visible_width(text: str) -> int:
    """Return the number of terminal columns occupied by `text`.

    Strips ANSI CSI escapes; counts CJK wide / fullwidth chars as 2 columns;
    treats combining marks and other control chars as zero-width.
    """
    cleaned = _ANSI_CSI_RE.sub("", text)
    width = 0
    for ch in cleaned:
        if ch in ("\r", "\n"):
            continue
        cat = unicodedata.category(ch)
        if cat.startswith("C") or cat in ("Mn", "Me"):
            continue
        ea = unicodedata.east_asian_width(ch)
        width += 2 if ea in ("W", "F") else 1
    return width


def _terminal_columns() -> int:
    try:
        cols = shutil.get_terminal_size(fallback=(80, 24)).columns
    except Exception:
        cols = 80
    return cols if cols > 0 else 80


def _prompt_screen_rows(lines: list[str], cols: int) -> int:
    """Total screen rows the rendered prompt will occupy, accounting for wrap."""
    if cols <= 0:
        return max(1, len(lines))
    total = 0
    for line in lines:
        w = _visible_width(line)
        total += 1 if w == 0 else (w + cols - 1) // cols
    return total


def _move_prompt_cursor(text: str, cursor: int, rendered_lines: int, cols: int, *, prefix_rows: int = 0) -> None:
    if rendered_lines <= 0 or cols <= 0:
        return
    row, col = _prompt_cursor_position(text, cursor, cols)
    row = min(max(prefix_rows + row, 0), max(0, rendered_lines - 1))
    rows_up = max(0, rendered_lines - 1 - row)
    sys.stderr.write("\r")
    if rows_up:
        sys.stderr.write(f"\033[{rows_up}A")
    if col:
        sys.stderr.write(f"\033[{col}C")
    sys.stderr.flush()


def _prompt_cursor_position(text: str, cursor: int, cols: int) -> tuple[int, int]:
    cursor = max(0, min(cursor, len(text)))
    before = text[:cursor]
    logical_line_index = before.count("\n")
    current_prefix_width = 2
    row = 0
    lines = text.split("\n")
    for line in lines[:logical_line_index]:
        width = current_prefix_width + _visible_width(line)
        row += 1 if width == 0 else max(1, (width + cols - 1) // cols)
    current_text = before.rsplit("\n", 1)[-1]
    width_before_cursor = current_prefix_width + _visible_width(current_text)
    row += width_before_cursor // cols
    col = width_before_cursor % cols
    return row, col


def _apply_prompt_escape_sequence(buffer: str, cursor: int, sequence: bytes) -> tuple[str, int] | None:
    if sequence in {b"\x1b[D", b"\x1bOD"}:
        return buffer, max(0, cursor - 1)
    if sequence in {b"\x1b[C", b"\x1bOC"}:
        return buffer, min(len(buffer), cursor + 1)
    if sequence in {b"\x1b[H", b"\x1b[1~", b"\x1bOH"}:
        return buffer, _line_start_index(buffer, cursor)
    if sequence in {b"\x1b[F", b"\x1b[4~", b"\x1bOF"}:
        return buffer, _line_end_index(buffer, cursor)
    if sequence == b"\x1b[3~":
        if cursor >= len(buffer):
            return buffer, cursor
        return buffer[:cursor] + buffer[cursor + 1 :], cursor
    if sequence == b"\x1b[A":
        return buffer, _move_cursor_vertical(buffer, cursor, -1)
    if sequence == b"\x1b[B":
        return buffer, _move_cursor_vertical(buffer, cursor, 1)
    return None


def _line_start_index(text: str, cursor: int) -> int:
    return text.rfind("\n", 0, cursor) + 1


def _line_end_index(text: str, cursor: int) -> int:
    index = text.find("\n", cursor)
    return len(text) if index == -1 else index


def _move_cursor_vertical(text: str, cursor: int, direction: int) -> int:
    start = _line_start_index(text, cursor)
    column = cursor - start
    if direction < 0:
        if start == 0:
            return cursor
        previous_end = start - 1
        previous_start = _line_start_index(text, previous_end)
        return min(previous_start + column, previous_end)
    current_end = _line_end_index(text, cursor)
    if current_end >= len(text):
        return cursor
    next_start = current_end + 1
    next_end = _line_end_index(text, next_start)
    return min(next_start + column, next_end)


def _clear_prompt_lines(line_count: int) -> None:
    if line_count <= 0:
        return
    if line_count > 1:
        sys.stderr.write(f"\r\033[{line_count - 1}A")
    else:
        sys.stderr.write("\r")
    for index in range(line_count):
        sys.stderr.write("\r\033[2K")
        if index < line_count - 1:
            sys.stderr.write("\033[1B")
    if line_count > 1:
        sys.stderr.write(f"\r\033[{line_count - 1}A")
    else:
        sys.stderr.write("\r")
    sys.stderr.flush()


def _read_tty_chunk(fd: int, pending: bytes) -> tuple[bytes, bytes]:
    if pending:
        return pending[:1], pending[1:]
    try:
        data = os.read(fd, 4096)
    except Exception:
        return b"", b""
    return data[:1], data[1:]


def _has_pending_input(fd: int, pending: bytes) -> bool:
    """True if more bytes are already available without blocking.

    Used by readers to keep processing a burst (paste, long voice input)
    before redrawing the prompt, so a multi-kilobyte paste redraws once
    instead of once per byte.
    """
    if pending:
        return True
    try:
        readable, _, _ = select.select([fd], [], [], 0)
    except Exception:
        return False
    return bool(readable)


def _read_escape_sequence(fd: int, pending: bytes) -> tuple[bytes, bytes]:
    sequence = b"\x1b"
    deadline = time.monotonic() + 0.03
    while time.monotonic() < deadline:
        if pending:
            sequence += pending[:1]
            pending = pending[1:]
            split_at = _complete_prompt_escape_sequence_length(sequence)
            if split_at is not None:
                return sequence[:split_at], sequence[split_at:] + pending
            continue
        readable, _, _ = select.select([fd], [], [], max(0.0, deadline - time.monotonic()))
        if not readable:
            break
        data = os.read(fd, 1024)
        if not data:
            break
        pending += data
    return sequence, pending


_COMPLETE_PROMPT_ESCAPE_SEQUENCES = (
    b"\x1b[200~",
    b"\x1b[D",
    b"\x1b[C",
    b"\x1b[A",
    b"\x1b[B",
    b"\x1b[H",
    b"\x1b[F",
    b"\x1b[1~",
    b"\x1b[3~",
    b"\x1b[4~",
    b"\x1bOA",
    b"\x1bOB",
    b"\x1bOD",
    b"\x1bOC",
    b"\x1bOH",
    b"\x1bOF",
)


def _complete_prompt_escape_sequence_length(sequence: bytes) -> int | None:
    for known in _COMPLETE_PROMPT_ESCAPE_SEQUENCES:
        if sequence.startswith(known):
            return len(known)
    return None


def _read_bracketed_paste(fd: int, sequence: bytes, pending: bytes) -> tuple[bytes, bytes]:
    start = b"\x1b[200~"
    end = b"\x1b[201~"
    data = sequence[len(start) :] if sequence.startswith(start) else b""
    while True:
        marker = data.find(end)
        if marker != -1:
            pasted = data[:marker]
            rest = data[marker + len(end) :]
            return pasted, rest + pending
        if pending:
            data += pending
            pending = b""
            continue
        chunk = os.read(fd, 1024)
        if not chunk:
            return data, b""
        data += chunk


def _normalize_optional_prompt(prompt: str | None) -> str | None:
    if prompt is None:
        return None
    return prompt.replace("\r\n", "\n").replace("\r", "\n")


@dataclass(frozen=True)
class _SlashCommandDef:
    name: str
    description: str
    supports_inline_args: bool = False
    available_during_task: bool = True
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class _ParsedSlashCommand:
    name: str
    rest: str
    command: _SlashCommandDef


@dataclass(frozen=True)
class _InteractiveSlashResult:
    handled: bool
    exit: bool = False
    status: int = 0
    prompt: str | None = None
    session: CodexSession | None = None


_SLASH_COMMANDS: tuple[_SlashCommandDef, ...] = (
    _SlashCommandDef("model", "choose what model and reasoning effort to use", available_during_task=False),
    _SlashCommandDef("ide", "include current selection, open files, and other context from your IDE", supports_inline_args=True),
    _SlashCommandDef("permissions", "choose what Codex is allowed to do", available_during_task=False),
    _SlashCommandDef("keymap", "remap TUI shortcuts", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("vim", "toggle Vim mode for the composer", available_during_task=False),
    _SlashCommandDef("setup-default-sandbox", "set up elevated agent sandbox", available_during_task=False),
    _SlashCommandDef("sandbox-add-read-dir", "let sandbox read a directory", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("experimental", "toggle experimental features", available_during_task=False),
    _SlashCommandDef("approve", "approve one retry of a recent auto-review denial"),
    _SlashCommandDef("memories", "configure memory use and generation", available_during_task=False),
    _SlashCommandDef("skills", "use skills to improve how Codex performs specific tasks"),
    _SlashCommandDef("hooks", "view and manage lifecycle hooks"),
    _SlashCommandDef("review", "review my current changes and find issues", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("rename", "rename the current thread", supports_inline_args=True),
    _SlashCommandDef("new", "start a new chat during a conversation", available_during_task=False),
    _SlashCommandDef("resume", "resume a saved chat", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("fork", "fork the current chat", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("init", "create an AGENTS.md file with instructions for Codex", available_during_task=False),
    _SlashCommandDef("compact", "summarize conversation to prevent hitting the context limit", available_during_task=False),
    _SlashCommandDef("plan", "switch to Plan mode", supports_inline_args=True, available_during_task=False),
    _SlashCommandDef("goal", "set or view the goal for a long-running task", supports_inline_args=True),
    _SlashCommandDef("collab", "change collaboration mode"),
    _SlashCommandDef("agent", "switch the active agent thread", aliases=("multi-agents", "subagents")),
    _SlashCommandDef("side", "start a side conversation in an ephemeral fork", supports_inline_args=True),
    _SlashCommandDef("copy", "copy last response as markdown"),
    _SlashCommandDef("raw", "toggle raw scrollback mode", supports_inline_args=True),
    _SlashCommandDef("diff", "show git diff"),
    _SlashCommandDef("mention", "mention a file"),
    _SlashCommandDef("status", "show current session configuration and token usage"),
    _SlashCommandDef("debug-config", "show config layers and requirement sources for debugging"),
    _SlashCommandDef("title", "configure terminal title items"),
    _SlashCommandDef("statusline", "configure status line items"),
    _SlashCommandDef("theme", "choose a syntax highlighting theme", available_during_task=False),
    _SlashCommandDef("pets", "choose or hide the terminal pet", supports_inline_args=True, available_during_task=False, aliases=("pet",)),
    _SlashCommandDef("mcp", "list configured MCP tools", supports_inline_args=True),
    _SlashCommandDef("apps", "manage apps"),
    _SlashCommandDef("plugins", "browse plugins"),
    _SlashCommandDef("logout", "log out of Codex", available_during_task=False),
    _SlashCommandDef("quit", "exit Codex"),
    _SlashCommandDef("exit", "exit Codex"),
    _SlashCommandDef("feedback", "send logs to maintainers"),
    _SlashCommandDef("rollout", "print the rollout file path"),
    _SlashCommandDef("ps", "list background terminals"),
    _SlashCommandDef("stop", "stop all background terminals", aliases=("clean",)),
    _SlashCommandDef("clear", "clear the terminal and start a new chat", available_during_task=False),
    _SlashCommandDef("personality", "choose a communication style", available_during_task=False),
    _SlashCommandDef("realtime", "toggle realtime voice mode"),
    _SlashCommandDef("settings", "configure realtime microphone/speaker"),
    _SlashCommandDef("test-approval", "test approval request"),
    _SlashCommandDef("debug-m-drop", "DO NOT USE", available_during_task=False),
    _SlashCommandDef("debug-m-update", "DO NOT USE", available_during_task=False),
)

_SLASH_COMMAND_BY_NAME: dict[str, _SlashCommandDef] = {
    alias: command
    for command in _SLASH_COMMANDS
    for alias in (command.name, *command.aliases)
}


def _parse_slash_name(line: str) -> tuple[str, str] | None:
    first_line = line.split("\n", 1)[0]
    stripped = first_line.removeprefix("/")
    if stripped == first_line:
        return None
    name = stripped
    rest = ""
    for index, char in enumerate(stripped):
        if char.isspace():
            name = stripped[:index]
            rest = stripped[index:].lstrip()
            break
    if not name:
        return None
    return name, rest


def _parse_interactive_slash(prompt: str) -> _ParsedSlashCommand | None:
    parsed = _parse_slash_name(prompt.lstrip())
    if parsed is None:
        return None
    name, rest = parsed
    if "/" in name:
        return None
    command = _SLASH_COMMAND_BY_NAME.get(name.lower())
    if command is None:
        return None
    if rest and not command.supports_inline_args:
        return None
    return _ParsedSlashCommand(name=name.lower(), rest=rest, command=command)


def _handle_interactive_slash_command(
    session: CodexSession,
    prompt: str,
    *,
    color_mode: str = "auto",
    queued_prompts: deque[str] | None = None,
) -> _InteractiveSlashResult:
    value = prompt.lstrip()
    if value.strip() == "/help":
        _print_chat_help()
        return _InteractiveSlashResult(True)
    if value == "/default" or value.startswith("/default ") or value == "/code" or value.startswith("/code "):
        raw = "/default" if value.startswith("/default") else "/code"
        remainder = value[len(raw) :].lstrip() or None
        _set_session_collaboration_mode(session, "Default")
        print("Switched to Default mode.", file=sys.stderr, flush=True)
        return _InteractiveSlashResult(True, prompt=remainder)

    parsed_name = _parse_slash_name(value)
    slash = _parse_interactive_slash(value)
    if slash is None:
        if parsed_name is not None and "/" not in parsed_name[0]:
            name, _ = parsed_name
            command = _SLASH_COMMAND_BY_NAME.get(name.lower())
            if command is not None:
                return _InteractiveSlashResult(False)
            print(
                f"Unrecognized command '/{name}'. Type \"/\" for a list of supported commands.",
                file=sys.stderr,
                flush=True,
            )
            return _InteractiveSlashResult(True)
        return _InteractiveSlashResult(False)

    command = slash.command.name
    if command in {"exit", "quit"}:
        return _InteractiveSlashResult(True, exit=True, status=0)
    if command == "clear":
        _clear_terminal()
        return _InteractiveSlashResult(True)
    if command == "compact":
        if _interactive_turn_controls_available():
            _run_compact_human_interactive(session, color_mode=color_mode, queued_prompts=queued_prompts)
        else:
            _run_compact_human(session, color_mode=color_mode)
        return _InteractiveSlashResult(True)
    if command == "plan":
        _set_session_collaboration_mode(session, "Plan")
        print("Switched to Plan mode.", file=sys.stderr, flush=True)
        return _InteractiveSlashResult(True, prompt=slash.rest.strip() or None)
    if command == "resume":
        resumed = _interactive_resume_session(session, slash.rest.strip())
        return _InteractiveSlashResult(True, session=resumed)
    if command == "fork":
        forked = _interactive_fork_session(session, slash.rest.strip())
        return _InteractiveSlashResult(True, session=forked)
    if command == "status":
        _print_chat_status(session)
        return _InteractiveSlashResult(True)
    if command == "rollout":
        print(f"Current rollout path: {session.state.rollout_path()}", file=sys.stderr, flush=True)
        return _InteractiveSlashResult(True)
    if command == "init":
        init_target = session.config.resolved_cwd() / "AGENTS.md"
        if init_target.exists():
            print("AGENTS.md already exists here. Skipping /init to avoid overwriting it.", file=sys.stderr, flush=True)
            return _InteractiveSlashResult(True)
        return _InteractiveSlashResult(True, prompt=_read_init_command_prompt())
    if command == "model":
        _handle_model_slash(session, slash.rest, color_mode=color_mode)
        return _InteractiveSlashResult(True)
    if command == "raw":
        arg = slash.rest.strip().lower()
        if arg and arg not in {"on", "off"}:
            print("Usage: /raw [on|off]", file=sys.stderr, flush=True)
        else:
            print("'/raw' is recognized but raw scrollback mode is not implemented in this Python CLI yet.", file=sys.stderr, flush=True)
        return _InteractiveSlashResult(True)

    print(
        f"'/{slash.name}' is recognized from upstream Codex but is not implemented in this Python CLI yet.",
        file=sys.stderr,
        flush=True,
    )
    return _InteractiveSlashResult(True)


def _interactive_resume_session(session: CodexSession, rest: str) -> CodexSession | None:
    rollout_path = _resolve_interactive_rollout_selector(
        rest,
        session.config,
        title="Resume saved chat",
    )
    if rollout_path is None:
        return None
    resumed = CodexSession.resume_from_rollout(
        rollout_path,
        session.config,
        model_client=session.model_client,
    )
    print(
        f"Resumed chat {resumed.state.thread_id} from {rollout_path}",
        file=sys.stderr,
        flush=True,
    )
    return resumed


def _interactive_fork_session(session: CodexSession, rest: str) -> CodexSession | None:
    if rest:
        rollout_path = _resolve_interactive_rollout_selector(
            rest,
            session.config,
            title="Fork saved chat",
        )
        if rollout_path is None:
            return None
        forked = CodexSession.fork_from_rollout(
            rollout_path,
            session.config,
            model_client=session.model_client,
        )
    else:
        forked = _fork_session_in_memory(session)
    print(
        f"Forked chat {forked.state.thread_id} from {forked.state.forked_from_id or 'current context'}",
        file=sys.stderr,
        flush=True,
    )
    return forked


def _fork_session_in_memory(session: CodexSession) -> CodexSession:
    forked = CodexSession(session.config, model_client=session.model_client)
    forked.state.history = deepcopy(session.state.history)
    forked.state._rollout_seed_history = deepcopy(session.state.history)
    forked.state.forked_from_id = session.state.thread_id
    forked.state.previous_turn_settings = deepcopy(session.state.previous_turn_settings)
    forked.state.reference_context_item = deepcopy(session.state.reference_context_item)
    forked.state.last_token_usage = deepcopy(session.state.last_token_usage)
    forked.state.total_token_usage = session.state.total_token_usage
    forked.state.session_reasoning_tokens = session.state.session_reasoning_tokens
    forked.state.context_carryover_tokens = session.state.context_carryover_tokens
    forked.state.context_carryover_estimated = session.state.context_carryover_estimated
    forked._initial_context_recorded = getattr(session, "_initial_context_recorded", False)
    return forked


def _resolve_interactive_rollout_selector(rest: str, config: CodexConfig, *, title: str) -> Path | None:
    try:
        tokens = shlex.split(rest)
    except ValueError as exc:
        print(f"Invalid command arguments: {exc}", file=sys.stderr, flush=True)
        return None
    all_cwds = False
    last = False
    selector: str | None = None
    for token in tokens:
        if token == "--all":
            all_cwds = True
        elif token == "--last":
            last = True
        elif selector is None:
            selector = token
        else:
            print("Usage: /resume [--last|SESSION_ID|ROLLOUT_PATH] [--all]", file=sys.stderr, flush=True)
            return None
    if selector is None and not last:
        return _prompt_rollout_picker(config, title=title, all_cwds=all_cwds)
    args = argparse.Namespace(session_id=selector, last=last, all_cwds=all_cwds)
    rollout_path = _resolve_resume_rollout(args, config)
    if rollout_path is None:
        missing = selector or "--last"
        print(f"No Codex rollout found for `{missing}`", file=sys.stderr, flush=True)
    return rollout_path


def _prompt_rollout_picker(config: CodexConfig, *, title: str, all_cwds: bool) -> Path | None:
    choices: list[Path] = []
    for path in _iter_rollout_paths(config.resolved_codex_home()):
        reconstruction = _safe_reconstruct_rollout(path)
        if reconstruction is None:
            continue
        if all_cwds or _rollout_cwd_matches(reconstruction.session_meta, config.resolved_cwd()):
            choices.append(path)
    choices = choices[:10]
    if not choices:
        print("No saved Codex chats found.", file=sys.stderr, flush=True)
        return None
    print(title, file=sys.stderr)
    for index, path in enumerate(choices, start=1):
        print(f"  {index}. {_format_rollout_picker_item(path)}", file=sys.stderr)
    print("Select a chat number, or press Enter to cancel: ", end="", file=sys.stderr, flush=True)
    if not sys.stdin.isatty():
        print(file=sys.stderr, flush=True)
        return None
    raw = sys.stdin.readline().strip()
    if not raw:
        print("Cancelled.", file=sys.stderr, flush=True)
        return None
    try:
        index = int(raw)
    except ValueError:
        print(f"Invalid selection `{raw}`.", file=sys.stderr, flush=True)
        return None
    if index < 1 or index > len(choices):
        print(f"Invalid selection `{raw}`.", file=sys.stderr, flush=True)
        return None
    return choices[index - 1]


def _format_rollout_picker_item(path: Path) -> str:
    reconstruction = _safe_reconstruct_rollout(path)
    thread_id = _rollout_thread_id(reconstruction.session_meta if reconstruction else None) or "unknown"
    cwd = "unknown cwd"
    if reconstruction and isinstance(reconstruction.session_meta, dict):
        raw_cwd = reconstruction.session_meta.get("cwd")
        if isinstance(raw_cwd, str) and raw_cwd:
            cwd = raw_cwd
    stamp = time.strftime("%Y-%m-%d %H:%M", time.localtime(_safe_mtime(path)))
    return f"{thread_id} · {stamp} · {cwd}"


def _read_init_command_prompt() -> str:
    path = Path(__file__).resolve().parent / "upstream" / "openai-codex" / "codex-rs" / "tui" / "prompt_for_init_command.md"
    return path.read_text(encoding="utf-8")


def _print_chat_status(session: CodexSession) -> None:
    config = session.config
    reasoning = config.resolved_reasoning() or {}
    active_context, _active_context_estimated = session.state.active_context_token_status()
    context_tokens = "unknown" if active_context is None else str(active_context)
    session_context, _session_context_estimated = session.state.session_context_token_status()
    session_context_text = (
        "unknown"
        if session_context is None
        else str(session_context)
    )
    api_usage_tokens = session.state.session_usage_tokens()
    api_usage_text = "none" if api_usage_tokens is None else str(api_usage_tokens)
    reasoning_tokens = session.state.session_reasoning_usage_tokens()
    reasoning_text = "none" if reasoning_tokens is None else str(reasoning_tokens)
    lines = [
        "Session status",
        f"  model: {config.model}",
        f"  reasoning: {reasoning.get('effort') or 'none'}",
        f"  cwd: {config.resolved_cwd()}",
        f"  sandbox: {config.sandbox}",
        f"  approval: {config.approval_policy}",
        f"  mode: {config.collaboration_mode}",
        f"  rollout: {session.state.rollout_path()}",
        f"  context tokens: {context_tokens}",
        f"  session context tokens: {session_context_text}",
        f"  session reasoning tokens: {reasoning_text}",
        f"  API usage tokens: {api_usage_text}",
    ]
    print("\n".join(lines), file=sys.stderr, flush=True)


def _interactive_command(prompt: str) -> str | None:
    value = prompt.strip()
    if value in {"/exit", "/quit", ":q"}:
        return "exit"
    if value == "/help":
        return "help"
    if value == "/clear":
        return "clear"
    if value == "/compact":
        return "compact"
    if value == "/plan":
        return "plan"
    if value in {"/default", "/code"}:
        return "default"
    return None


def _interactive_mode_command(prompt: str) -> tuple[str, str | None] | None:
    value = prompt.lstrip()
    for raw, mode in (("/plan", "Plan"), ("/default", "Default"), ("/code", "Default")):
        if value == raw:
            return mode, None
        if value.startswith(f"{raw} "):
            return mode, value[len(raw) :].lstrip() or None
    return None


def _print_chat_help() -> None:
    local = "/resume, /fork, /compact, /plan, /status, /rollout, /init, /clear, /exit"
    recognized = ", ".join(f"/{command.name}" for command in _SLASH_COMMANDS[:18])
    print(
        "Commands implemented locally: "
        f"{local}\n"
        "Upstream commands are recognized and consumed locally; unsupported ones report a local message instead of being sent to the model.\n"
        f"Examples from upstream: {recognized}, ...",
        file=sys.stderr,
        flush=True,
    )


def _clear_terminal() -> None:
    if sys.stderr.isatty():
        print("\033[2J\033[H", end="", file=sys.stderr, flush=True)


class _HumanEventRenderer:
    def __init__(
        self,
        *,
        color_mode: str = "auto",
        line_sink: Callable[[str], None] | None = None,
        status_tracker: _LiveTurnStatus | None = None,
    ) -> None:
        self._tool_arguments: dict[str, Any] = {}
        self._exec_calls: dict[str, _ExecDisplayCall] = {}
        self._exploration_calls: list[_ExecDisplayCall] = []
        self._final_message: str = ""
        self._final_message_rendered = False
        self._printed_any_cell = False
        self._had_work_activity = False
        self._style = _AnsiStyle(_should_use_color(color_mode))
        self._line_sink = line_sink
        self._status_tracker = status_tracker

    def render(self, event: Any) -> None:
        if self._status_tracker is not None:
            self._status_tracker.update(event)
        if event.type == "item.completed":
            self._render_item(event.payload.get("item"), pending_input=bool(event.payload.get("pending_input")))
        elif event.type == "tool.started":
            self._render_tool_started(event.payload)
        elif event.type == "tool.completed":
            self._render_tool_completed(event.payload)
        elif event.type == "warning":
            self._begin_cell()
            self._line(f"warning: {event.payload.get('message') or ''}")
        elif event.type == "stream_error":
            self._begin_cell()
            self._line(str(event.payload.get("message") or "Reconnecting..."))
        elif event.type == "turn.aborted":
            self.render_interrupted()

    def render_error(self, message: str) -> None:
        self._begin_cell()
        self._line(f"ERROR: {message}")

    def render_info_message(self, message: str) -> None:
        self._begin_cell()
        self._line(f"{self._style.dim('•')} {message}")

    def render_interrupted(self) -> None:
        self._begin_cell()
        self._line(
            f"{self._style.red('■')} "
            "Conversation interrupted - tell the model what to do differently. "
            "Something went wrong? Hit `/feedback` to report the issue."
        )

    def render_user_message(self, text: str) -> None:
        normalized = text.rstrip("\r\n")
        if not normalized:
            return
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        lines: list[str] = []
        for raw_line in normalized.split("\n"):
            if raw_line == "":
                lines.append("")
                continue
            lines.extend(_wrap_ansi_line(raw_line, max(10, terminal_width - 2)))
        self._begin_cell()
        self._emit_prefixed_lines(
            lines,
            first_prefix=self._style.dim(self._style.bold("› ")),
            rest_prefix="  ",
        )

    def render_pending_input_preview(self, text: str, *, active: bool) -> None:
        normalized = text.rstrip("\r\n")
        if not normalized:
            return
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        lines: list[str] = []
        for raw_line in normalized.split("\n"):
            if raw_line == "":
                lines.append("")
                continue
            lines.extend(_wrap_ansi_line(raw_line, max(10, terminal_width - 4)))
        self._begin_cell()
        header = "Messages to be submitted after next tool call" if active else "Queued follow-up inputs"
        self._line(f"{self._style.dim('•')} {self._style.bold(header)}")
        self._emit_prefixed_lines(
            lines,
            first_prefix=self._style.dim("  ↳ "),
            rest_prefix=self._style.dim("    "),
            transform=self._style.dim,
        )

    def finish(self, final_message: str, *, print_to_stdout: bool = True) -> None:
        self._flush_exploration()
        self._final_message = final_message or self._final_message
        if not self._final_message:
            return
        if sys.stdout.isatty() and sys.stderr.isatty():
            if not self._final_message_rendered:
                self._render_agent_message(self._final_message)
            return
        if not self._final_message_rendered:
            self._render_agent_message(self._final_message)
        if print_to_stdout:
            print(self._final_message, flush=True)

    def _render_item(self, item: Any, *, pending_input: bool = False) -> None:
        if not isinstance(item, dict):
            return
        item_type = item.get("type")
        if item_type == "message" and item.get("role") == "user" and pending_input:
            self._flush_exploration()
            text = _user_item_text(item)
            if text:
                self.render_user_message(text)
            return
        if item_type == "message" and item.get("role") == "assistant":
            self._flush_exploration()
            text = _assistant_item_text(item)
            if text:
                self._final_message = text
                self._render_agent_message(text)
            return
        if item_type == "reasoning":
            self._flush_exploration()
            text = _reasoning_item_text(item)
            if text.strip():
                self._render_reasoning_message(text)
            return
        if item_type == "web_search_call":
            self._flush_exploration()
            self._begin_cell()
            self._render_web_search_cell(
                _web_search_query(item),
                completed=True,
                action=item.get("action") if isinstance(item.get("action"), dict) else None,
            )

    def _render_tool_started(self, payload: dict[str, Any]) -> None:
        name = str(payload.get("name") or "")
        call_id = str(payload.get("call_id") or "")
        arguments = payload.get("arguments")
        args = arguments if isinstance(arguments, dict) else {}
        if call_id:
            self._tool_arguments[call_id] = arguments

        if name in {"exec_command", "shell_command"}:
            command = str(args.get("cmd") or args.get("command") or "")
            self._exec_calls[call_id] = _ExecDisplayCall(
                call_id=call_id,
                command=command,
                parsed=parse_command_actions(command),
            )
        elif name == "write_stdin":
            self._flush_exploration()
            self._begin_work_cell()
            session_id = args.get("session_id")
            chars = str(args.get("chars") or "")
            if chars:
                self._line(f"{self._style.dim('↳')} {self._style.bold('Interacted with background terminal')} {self._style.dim('·')} {self._style.dim(str(session_id))}")
                self._render_output_block(chars)
            else:
                self._line(f"{self._style.dim('•')} {self._style.bold('Waited for background terminal')} {self._style.dim('·')} {self._style.dim(str(session_id))}")
        elif name == "apply_patch":
            return
        elif name == "web_search":
            self._flush_exploration()
            self._begin_work_cell()
            query = str(args.get("query") or "")
            self._render_web_search_cell(query, completed=False)
        elif name in {"spawn_agent", "send_input", "resume_agent", "wait_agent", "close_agent"}:
            self._flush_exploration()
            self._begin_work_cell()
            self._line(f"{self._style.bold('agent tool:')} {name}")

    def _render_tool_completed(self, payload: dict[str, Any]) -> None:
        name = str(payload.get("name") or "")
        call_id = str(payload.get("call_id") or "")
        metadata = payload.get("metadata")
        meta = metadata if isinstance(metadata, dict) else {}
        arguments = self._tool_arguments.pop(call_id, None)
        ok = bool(payload.get("ok"))

        if name in {"exec_command", "shell_command", "write_stdin"}:
            self._render_command_completed(call_id, ok, payload, meta)
        elif name == "apply_patch":
            self._render_apply_patch_completed(ok, payload, meta, arguments)
        elif name == "update_plan":
            self._flush_exploration()
            self._begin_work_cell()
            self._render_plan(meta)
        elif name == "view_image":
            self._flush_exploration()
            self._begin_work_cell()
            path = meta.get("path")
            self._line(f"{self._style.bold('view image:')} {path}" if path else self._style.bold("view image"))
        elif name == "request_user_input":
            self._flush_exploration()
            self._begin_work_cell()
            if not self._render_request_user_input_result(meta, interrupted=not ok):
                self._line(
                    f"{self._style.bold('request user input:')} {self._style.green('completed')}"
                    if ok
                    else f"{self._style.bold('request user input:')} {self._style.red('failed')}"
                )
        elif name in {"spawn_agent", "send_input", "resume_agent", "wait_agent", "close_agent"}:
            self._flush_exploration()
            self._begin_work_cell()
            status = self._style.green("completed") if ok else self._style.red("failed")
            self._line(f"{self._style.bold('agent tool:')} {name} {status}")
            output = str(payload.get("output") or "")
            if output.strip() and name in {"wait_agent", "close_agent"}:
                self._line(output)
        elif not ok:
            self._flush_exploration()
            self._begin_work_cell()
            output = str(payload.get("output") or "")
            self._line(f"{self._style.bold(name + ':')} {self._style.red('failed')}")
            if output.strip():
                self._line(output)

    def _render_web_search_cell(
        self,
        query: str,
        *,
        completed: bool,
        action: dict[str, Any] | None = None,
    ) -> None:
        header = "Searched" if completed else "Searching the web"
        detail = _web_search_action_detail(action) if action is not None else ""
        text = " ".join(part for part in [self._style.bold(header), detail or query] if part)
        self._emit_prefixed_lines(
            [text],
            first_prefix=f"{self._style.dim('•')} ",
            rest_prefix="  ",
        )

    def _render_command_completed(
        self,
        call_id: str,
        ok: bool,
        payload: dict[str, Any],
        meta: dict[str, Any],
    ) -> None:
        if str(payload.get("name") or "") == "write_stdin":
            output = str(meta.get("aggregated_output") or meta.get("output") or payload.get("output") or "")
            if output.strip():
                self._render_output_block(output)
            return
        call = self._exec_calls.pop(call_id, None)
        if call is None:
            command = str(meta.get("command") or "")
            call = _ExecDisplayCall(call_id=call_id, command=command, parsed=parse_command_actions(command))
        exit_code = meta.get("exit_code")
        exit_value = _int_value(exit_code)
        call.output = str(meta.get("aggregated_output") or meta.get("output") or payload.get("output") or "")
        call.exit_code = exit_value
        call.duration_ms = _duration_ms(meta.get("wall_time_seconds"))
        if exit_value is not None and call.is_exploration:
            self._exploration_calls.append(call)
            return
        self._flush_exploration()
        self._render_exec_call(call, running=exit_value is None, ok=ok)

    def _render_apply_patch_completed(
        self,
        ok: bool,
        payload: dict[str, Any],
        meta: dict[str, Any],
        arguments: Any,
    ) -> None:
        self._flush_exploration()
        self._begin_work_cell()
        if ok:
            changes = _file_change_display_from_metadata(meta)
            if not changes:
                changes = _file_change_display_from_patch(arguments)
            if changes:
                self._render_file_changes(changes)
                return
        self._line(
            f"{self._style.bold('patch:')} {self._style.green('completed')}"
            if ok
            else f"{self._style.bold('patch:')} {self._style.red('failed')}"
        )
        output = str(payload.get("output") or "")
        if output.strip():
            self._line(output)

    def _render_file_changes(self, changes: list["_FileChangeDisplay"]) -> None:
        total_added = sum(change.additions for change in changes)
        total_deleted = sum(change.deletions for change in changes)
        if len(changes) == 1:
            change = changes[0]
            verb = {"add": "Added", "delete": "Deleted"}.get(change.kind, "Edited")
            self._line(
                f"{self._style.dim('•')} {self._style.bold(verb)} "
                f"{_file_change_path_label(change)} (+{change.additions} -{change.deletions})"
            )
            self._render_file_change_rows(change)
            return
        noun = "file" if len(changes) == 1 else "files"
        self._line(
            f"{self._style.dim('•')} {self._style.bold('Edited')} {len(changes)} {noun} "
            f"(+{total_added} -{total_deleted})"
        )
        for index, change in enumerate(changes):
            if index > 0:
                self._line("")
            self._line(
                f"{self._style.dim('  └')} {_file_change_path_label(change)} "
                f"(+{change.additions} -{change.deletions})"
            )
            self._render_file_change_rows(change)

    def _render_file_change_rows(self, change: "_FileChangeDisplay") -> None:
        width = max(1, max((row.line_number or 0 for row in change.rows), default=0))
        line_number_width = len(str(width))
        for row in change.rows:
            if row.kind == "ellipsis":
                self._line(f"{'':>{line_number_width + 5}}{self._style.dim('⋮')}")
                continue
            number = "" if row.line_number is None else str(row.line_number)
            sign = row.kind if row.kind in {"+", "-"} else " "
            rendered = f"    {number:>{line_number_width}} {sign}{row.text}"
            if row.kind == "+":
                rendered = self._style.green(rendered)
            elif row.kind == "-":
                rendered = self._style.red(rendered)
            self._emit_prefixed_lines([rendered], first_prefix="", rest_prefix="      ")

    def _render_exec_call(self, call: "_ExecDisplayCall", *, running: bool, ok: bool) -> None:
        self._begin_work_cell()
        command = _command_display(call.command)
        if running:
            title = "Running"
            bullet = self._style.cyan("•")
        elif call.exit_code == 0 and ok:
            title = "Ran"
            bullet = self._style.green("•")
        else:
            title = "Ran"
            bullet = self._style.red("•")
        header_prefix = f"{bullet} {self._style.bold(title)} "
        self._emit_limited_prefixed_lines(
            [command],
            first_prefix=header_prefix,
            rest_prefix=self._style.dim("  │ "),
            max_lines=3,
            ellipsis_prefix=self._style.dim("  │ "),
        )
        if call.output.strip():
            self._render_output_block(call.output)

    def _flush_exploration(self) -> None:
        if not self._exploration_calls:
            return
        calls = self._exploration_calls
        self._exploration_calls = []
        self._begin_work_cell()
        self._line(f"{self._style.dim('•')} {self._style.bold('Explored')}")
        rows: list[tuple[str, str]] = []
        while calls:
            call = calls.pop(0)
            if call.reads_only:
                names = []
                for action in call.parsed:
                    if action.get("type") == "read":
                        name = str(action.get("name") or action.get("path") or action.get("cmd") or "")
                        if name and name not in names:
                            names.append(name)
                while calls and calls[0].reads_only:
                    next_call = calls.pop(0)
                    for action in next_call.parsed:
                        name = str(action.get("name") or action.get("path") or action.get("cmd") or "")
                        if name and name not in names:
                            names.append(name)
                rows.append(("Read", ", ".join(names)))
                continue
            for action in call.parsed:
                action_type = action.get("type")
                if action_type == "read":
                    rows.append(("Read", str(action.get("name") or action.get("path") or action.get("cmd") or "")))
                elif action_type == "list_files":
                    rows.append(("List", str(action.get("path") or action.get("cmd") or "")))
                elif action_type == "search":
                    query = action.get("query")
                    path = action.get("path")
                    if query and path:
                        rows.append(("Search", f"{query} in {path}"))
                    else:
                        rows.append(("Search", str(query or action.get("cmd") or "")))
                else:
                    rows.append(("Run", str(action.get("cmd") or call.command)))
        for index, (title, text) in enumerate(rows):
            gutter = self._style.dim("  └ " if index == 0 else "    ")
            title_prefix = f"{gutter}{self._style.cyan(title)} "
            continuation = f"{self._style.dim('    ')}{' ' * (_visible_len(title) + 1)}"
            self._emit_prefixed_lines([text], first_prefix=title_prefix, rest_prefix=continuation)

    def _render_output_block(self, output: str) -> None:
        lines = output.rstrip("\n").splitlines()
        if not lines:
            return
        head, tail, omitted = _truncate_middle_parts(lines, 5)
        self._emit_prefixed_lines(
            head,
            first_prefix=self._style.dim("  └ "),
            rest_prefix=self._style.dim("    "),
            transform=self._style.dim,
        )
        if omitted:
            self._line(self._style.dim(f"    {_ellipsis_text(omitted, transcript_hint=True)}"))
            self._emit_prefixed_lines(
                tail,
                first_prefix=self._style.dim("    "),
                rest_prefix=self._style.dim("    "),
                transform=self._style.dim,
            )

    def _render_plan(self, meta: dict[str, Any]) -> None:
        self._line(f"{self._style.dim('•')} {self._style.bold('Updated Plan')}")
        explanation = meta.get("explanation")
        indented: list[tuple[str, Any | None]] = []
        if isinstance(explanation, str) and explanation.strip():
            for rendered_line in _wrap_ansi_line(
                explanation.strip(),
                max(10, shutil.get_terminal_size((100, 24)).columns - 6),
            ):
                indented.append((rendered_line, lambda value, style=self._style: style.dim(style.italic(value))))
        plan = meta.get("plan")
        if isinstance(plan, list) and plan:
            for item in plan:
                if not isinstance(item, dict):
                    continue
                status = str(item.get("status") or "")
                step = str(item.get("step") or "")
                if status == "completed":
                    marker_text = self._style.dim(self._style.strike("✔"))
                    render_step = lambda value, style=self._style: style.dim(style.strike(value))
                elif status == "in_progress":
                    marker_text = self._style.cyan(self._style.bold("□"))
                    render_step = lambda value, style=self._style: style.cyan(style.bold(value))
                else:
                    marker_text = self._style.dim("□")
                    render_step = self._style.dim
                wrapped = _wrap_ansi_line(step, max(10, shutil.get_terminal_size((100, 24)).columns - 8))
                if wrapped:
                    indented.append((f"{marker_text} {render_step(wrapped[0])}", None))
                    for continuation in wrapped[1:]:
                        indented.append((f"  {continuation}", render_step))
            else:
                pass
        elif isinstance(plan, list):
            indented.append((self._style.dim(self._style.italic("(no steps provided)")), None))
        if not indented:
            return
        for index, (line, transform) in enumerate(indented):
            prefix = self._style.dim("  └ " if index == 0 else "    ")
            rendered = transform(line) if transform is not None else line
            self._line(f"{prefix}{rendered}")

    def _render_request_user_input_result(self, meta: dict[str, Any], *, interrupted: bool) -> bool:
        questions = meta.get("questions")
        answers = meta.get("answers")
        if not isinstance(questions, list) or not isinstance(answers, dict):
            return False
        total = len(questions)
        answered = sum(
            1
            for question in questions
            if isinstance(question, dict)
            and _request_user_input_answer_list(answers.get(str(question.get("id") or "")))
        )
        header = f"{self._style.dim('•')} {self._style.bold('Questions')} {self._style.dim(f'{answered}/{total} answered')}"
        if interrupted:
            header += f" {self._style.cyan('(interrupted)')}"
        self._line(header)
        for question in questions:
            if not isinstance(question, dict):
                continue
            question_text = str(question.get("question") or "")
            answer_values = _request_user_input_answer_list(answers.get(str(question.get("id") or "")))
            if not answer_values:
                question_text = f"{question_text} {self._style.dim('(unanswered)')}"
            self._emit_prefixed_lines([question_text], first_prefix="  • ", rest_prefix="    ")
            if not answer_values:
                continue
            if bool(question.get("isSecret") or question.get("is_secret")):
                self._emit_prefixed_lines(
                    ["••••••"],
                    first_prefix=self._style.dim("    answer: "),
                    rest_prefix=self._style.dim("            "),
                    transform=self._style.cyan,
                )
                continue
            options, note = _split_request_user_input_answer_values(answer_values)
            for option in options:
                self._emit_prefixed_lines(
                    [option],
                    first_prefix=self._style.dim("    answer: "),
                    rest_prefix=self._style.dim("            "),
                    transform=self._style.cyan,
                )
            if note:
                label = "    note: " if options else "    answer: "
                continuation = "          " if options else "            "
                self._emit_prefixed_lines(
                    [note],
                    first_prefix=self._style.dim(label),
                    rest_prefix=self._style.dim(continuation),
                    transform=self._style.cyan,
                )
        if interrupted and answered < total:
            self._emit_prefixed_lines(
                [f"interrupted with {total - answered} unanswered"],
                first_prefix=self._style.dim(self._style.cyan("  ↳ ")),
                rest_prefix=self._style.dim("    "),
                transform=lambda value, style=self._style: style.dim(style.cyan(value)),
            )
        return True

    def _render_agent_message(self, text: str) -> None:
        self._final_message_rendered = True
        if self._had_work_activity:
            self._render_final_separator()
            self._had_work_activity = False
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        lines = _render_markdown_for_terminal(
            text,
            self._style,
            terminal_width=max(10, terminal_width - 2),
        )
        if not lines:
            return
        self._begin_cell()
        self._emit_prefixed_lines(lines, first_prefix=f"{self._style.magenta('•')} ", rest_prefix="  ")

    def _render_reasoning_message(self, text: str) -> None:
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        lines = _render_reasoning_for_terminal(text, self._style, terminal_width=max(10, terminal_width - 2))
        if not lines:
            return
        self._begin_cell()
        self._emit_prefixed_lines(
            lines,
            first_prefix=f"{self._style.dim('•')} ",
            rest_prefix="  ",
            transform=self._style.dim,
        )

    def _render_final_separator(self) -> None:
        self._begin_cell()
        width = shutil.get_terminal_size((100, 24)).columns
        self._line(self._style.dim("─" * max(20, width)))

    def _begin_work_cell(self) -> None:
        self._had_work_activity = True
        self._begin_cell()

    def _begin_cell(self) -> None:
        if self._printed_any_cell:
            self._line("")
        self._printed_any_cell = True

    def _emit_prefixed_lines(
        self,
        lines: list[str],
        *,
        first_prefix: str,
        rest_prefix: str,
        transform: Any | None = None,
    ) -> None:
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        first_physical_line = True
        for logical_line in lines:
            if logical_line == "":
                self._line("")
                first_physical_line = False
                continue
            line_first = first_physical_line
            prefix = first_prefix if line_first else rest_prefix
            available_width = max(10, terminal_width - _visible_len(prefix))
            for segment in _wrap_ansi_line(logical_line, available_width):
                prefix = first_prefix if line_first else rest_prefix
                rendered = transform(segment) if transform is not None else segment
                self._line(f"{prefix}{rendered}")
                line_first = False
                first_physical_line = False

    def _emit_limited_prefixed_lines(
        self,
        lines: list[str],
        *,
        first_prefix: str,
        rest_prefix: str,
        max_lines: int,
        ellipsis_prefix: str,
    ) -> None:
        terminal_width = shutil.get_terminal_size((100, 24)).columns
        rendered: list[tuple[str, str]] = []
        first_physical_line = True
        for logical_line in lines:
            line_first = first_physical_line
            prefix = first_prefix if line_first else rest_prefix
            available_width = max(10, terminal_width - _visible_len(prefix))
            for segment in _wrap_ansi_line(logical_line, available_width):
                rendered.append((first_prefix if line_first else rest_prefix, segment))
                line_first = False
                first_physical_line = False
        if len(rendered) <= max_lines:
            for prefix, segment in rendered:
                self._line(f"{prefix}{segment}")
            return
        keep = max(1, max_lines - 1)
        for prefix, segment in rendered[:keep]:
            self._line(f"{prefix}{segment}")
        self._line(f"{ellipsis_prefix}{self._style.dim(_ellipsis_text(len(rendered) - keep))}")

    def _line(self, text: str) -> None:
        if self._line_sink is not None:
            self._line_sink(text)
            return
        print(text, file=sys.stderr, flush=True)


@dataclass
class _ExecDisplayCall:
    call_id: str
    command: str
    parsed: list[dict[str, Any]]
    output: str = ""
    exit_code: int | None = None
    duration_ms: int | None = None

    @property
    def is_exploration(self) -> bool:
        return bool(self.parsed) and all(
            action.get("type") in {"read", "list_files", "search"} for action in self.parsed
        )

    @property
    def reads_only(self) -> bool:
        return bool(self.parsed) and all(action.get("type") == "read" for action in self.parsed)


@dataclass
class _DiffDisplayRow:
    kind: str
    line_number: int | None
    text: str = ""


@dataclass
class _FileChangeDisplay:
    kind: str
    path: str
    additions: int
    deletions: int
    rows: list[_DiffDisplayRow]
    move_path: str | None = None


def _file_change_path_label(change: _FileChangeDisplay) -> str:
    if change.move_path:
        return f"{change.path} → {change.move_path}"
    return change.path


def _file_change_display_from_metadata(meta: dict[str, Any]) -> list[_FileChangeDisplay]:
    changes = meta.get("changes")
    if not isinstance(changes, list):
        return []
    rendered: list[_FileChangeDisplay] = []
    for raw_change in changes:
        if not isinstance(raw_change, dict):
            continue
        path = str(raw_change.get("path") or "")
        if not path:
            continue
        kind = str(raw_change.get("type") or "update")
        move_path_value = raw_change.get("move_path")
        move_path = str(move_path_value) if move_path_value else None
        if kind == "add":
            rows = _content_diff_rows(str(raw_change.get("content") or ""), "+")
        elif kind == "delete":
            rows = _content_diff_rows(str(raw_change.get("content") or ""), "-")
        else:
            rows = _unified_diff_rows(str(raw_change.get("unified_diff") or ""))
        additions = _int_or(raw_change.get("additions"), _count_rows(rows, "+"))
        deletions = _int_or(raw_change.get("deletions"), _count_rows(rows, "-"))
        rendered.append(
            _FileChangeDisplay(
                kind=kind,
                path=path,
                move_path=move_path,
                additions=additions,
                deletions=deletions,
                rows=rows,
            )
        )
    return rendered


def _file_change_display_from_patch(arguments: Any) -> list[_FileChangeDisplay]:
    if isinstance(arguments, str):
        patch = arguments
    elif isinstance(arguments, dict):
        patch = arguments.get("patch") if isinstance(arguments.get("patch"), str) else ""
    else:
        patch = ""
    if not patch.strip():
        return []
    if patch.lstrip().startswith("*** Begin Patch"):
        return _codex_patch_display_changes(patch)
    return _unified_patch_display_changes(patch)


def _content_diff_rows(content: str, kind: str) -> list[_DiffDisplayRow]:
    return [_DiffDisplayRow(kind, index, line) for index, line in enumerate(content.splitlines(), 1)]


def _unified_patch_display_changes(patch: str) -> list[_FileChangeDisplay]:
    changes: list[_FileChangeDisplay] = []
    old_path: str | None = None
    new_path: str | None = None
    hunk_lines: list[str] = []

    def flush() -> None:
        nonlocal old_path, new_path, hunk_lines
        if old_path is None and new_path is None:
            return
        old = _strip_diff_display_prefix(old_path or "")
        new = _strip_diff_display_prefix(new_path or "")
        if old == "/dev/null":
            kind = "add"
            path = new
            move_path = None
        elif new == "/dev/null":
            kind = "delete"
            path = old
            move_path = None
        else:
            kind = "update"
            path = old or new
            move_path = new if old and new and old != new else None
        rows = _unified_diff_rows("\n".join(hunk_lines) + ("\n" if hunk_lines else ""))
        changes.append(
            _FileChangeDisplay(
                kind=kind,
                path=path,
                move_path=move_path,
                additions=_count_rows(rows, "+"),
                deletions=_count_rows(rows, "-"),
                rows=rows,
            )
        )
        old_path = None
        new_path = None
        hunk_lines = []

    for line in patch.splitlines():
        if line.startswith("diff --git "):
            flush()
            continue
        if line.startswith("--- "):
            flush()
            old_path = _diff_header_path(line[4:])
            continue
        if line.startswith("+++ "):
            new_path = _diff_header_path(line[4:])
            continue
        if line.startswith("@@") or (hunk_lines and line.startswith((" ", "+", "-", "\\"))):
            hunk_lines.append(line)
    flush()
    return [change for change in changes if change.path]


def _codex_patch_display_changes(patch: str) -> list[_FileChangeDisplay]:
    changes: list[_FileChangeDisplay] = []
    current_path: str | None = None
    current_kind: str | None = None
    move_path: str | None = None
    rows: list[_DiffDisplayRow] = []
    old_ln = 1
    new_ln = 1

    def flush() -> None:
        nonlocal current_path, current_kind, move_path, rows, old_ln, new_ln
        if current_path and current_kind:
            changes.append(
                _FileChangeDisplay(
                    kind=current_kind,
                    path=current_path,
                    move_path=move_path,
                    additions=_count_rows(rows, "+"),
                    deletions=_count_rows(rows, "-"),
                    rows=rows,
                )
            )
        current_path = None
        current_kind = None
        move_path = None
        rows = []
        old_ln = 1
        new_ln = 1

    for line in patch.splitlines():
        if line.startswith("*** Add File: "):
            flush()
            current_path = line.removeprefix("*** Add File: ").strip()
            current_kind = "add"
            continue
        if line.startswith("*** Delete File: "):
            flush()
            current_path = line.removeprefix("*** Delete File: ").strip()
            current_kind = "delete"
            continue
        if line.startswith("*** Update File: "):
            flush()
            current_path = line.removeprefix("*** Update File: ").strip()
            current_kind = "update"
            continue
        if line.startswith("*** Move to: "):
            move_path = line.removeprefix("*** Move to: ").strip()
            continue
        if line.startswith("*** End Patch"):
            flush()
            break
        if current_kind == "add" and line.startswith("+"):
            rows.append(_DiffDisplayRow("+", new_ln, line[1:]))
            new_ln += 1
        elif current_kind == "update":
            if line.startswith("@@"):
                if rows:
                    rows.append(_DiffDisplayRow("ellipsis", None))
                continue
            if line.startswith("+"):
                rows.append(_DiffDisplayRow("+", new_ln, line[1:]))
                new_ln += 1
            elif line.startswith("-"):
                rows.append(_DiffDisplayRow("-", old_ln, line[1:]))
                old_ln += 1
            elif line.startswith(" "):
                rows.append(_DiffDisplayRow(" ", new_ln, line[1:]))
                old_ln += 1
                new_ln += 1
    return changes


def _unified_diff_rows(diff_text: str) -> list[_DiffDisplayRow]:
    rows: list[_DiffDisplayRow] = []
    old_ln = 1
    new_ln = 1
    in_hunk = False
    for line in diff_text.splitlines():
        if line.startswith("@@"):
            if in_hunk and rows:
                rows.append(_DiffDisplayRow("ellipsis", None))
            in_hunk = True
            old_ln, new_ln = _parse_hunk_line_numbers(line)
            continue
        if not in_hunk:
            continue
        if line.startswith("\\"):
            continue
        if line.startswith("+"):
            rows.append(_DiffDisplayRow("+", new_ln, line[1:]))
            new_ln += 1
        elif line.startswith("-"):
            rows.append(_DiffDisplayRow("-", old_ln, line[1:]))
            old_ln += 1
        elif line.startswith(" "):
            rows.append(_DiffDisplayRow(" ", new_ln, line[1:]))
            old_ln += 1
            new_ln += 1
    return rows


def _parse_hunk_line_numbers(line: str) -> tuple[int, int]:
    match = re.search(r"@@ -(?P<old>\d+)(?:,\d+)? \+(?P<new>\d+)(?:,\d+)? @@", line)
    if not match:
        return 1, 1
    return int(match.group("old")), int(match.group("new"))


def _count_rows(rows: list[_DiffDisplayRow], kind: str) -> int:
    return sum(1 for row in rows if row.kind == kind)


def _int_or(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _diff_header_path(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value[0] in {"'", '"'}:
        try:
            parts = shlex.split(value)
        except ValueError:
            parts = value.split()
        return parts[0] if parts else ""
    return value.split("\t", 1)[0].strip()


def _strip_diff_display_prefix(path: str) -> str:
    if path in {"/dev/null", "dev/null"}:
        return "/dev/null"
    if path.startswith(("a/", "b/")):
        return path[2:]
    return path


class _AnsiStyle:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def bold(self, text: str) -> str:
        return self._wrap("1", text)

    def dim(self, text: str) -> str:
        return self._wrap("2", text)

    def italic(self, text: str) -> str:
        return self._wrap("3", text)

    def strike(self, text: str) -> str:
        return self._wrap("9", text)

    def green(self, text: str) -> str:
        return self._wrap("32", text)

    def red(self, text: str) -> str:
        return self._wrap("31", text)

    def cyan(self, text: str) -> str:
        return self._wrap("36", text)

    def magenta(self, text: str) -> str:
        return self._wrap("35", text)

    def _wrap(self, code: str, text: str) -> str:
        if not self.enabled or not text:
            return text
        return f"\033[{code}m{text}\033[0m"


def _should_use_color(color_mode: str) -> bool:
    if color_mode == "always":
        return True
    if color_mode == "never":
        return False
    return sys.stderr.isatty()


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(text: str) -> int:
    return _display_width(_ANSI_RE.sub("", text))


def _display_width(text: str) -> int:
    width = 0
    for char in text:
        if unicodedata.combining(char):
            continue
        category = unicodedata.category(char)
        if category.startswith("C"):
            continue
        width += 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1
    return width


def _wrap_ansi_line(text: str, width: int) -> list[str]:
    if width <= 0 or _visible_len(text) <= width:
        return [text]
    wrapped: list[str] = []
    current = ""
    current_width = 0
    for chunk in re.findall(r"\s+|\S+\s*", text):
        chunk_width = _visible_len(chunk)
        if current and current_width + chunk_width > width:
            wrapped.append(current.rstrip())
            next_chunk = chunk.lstrip()
            if _visible_len(next_chunk) > width:
                pieces = _split_visible_chunk(next_chunk, width)
                wrapped.extend(piece.rstrip() for piece in pieces[:-1])
                current = pieces[-1].lstrip()
            else:
                current = next_chunk
            current_width = _visible_len(current)
            continue
        if not current and chunk_width > width:
            pieces = _split_visible_chunk(chunk, width)
            wrapped.extend(piece.rstrip() for piece in pieces[:-1])
            current = pieces[-1].lstrip()
            current_width = _visible_len(current)
            continue
        current += chunk
        current_width += chunk_width
    if current or not wrapped:
        wrapped.append(current.rstrip())
    return wrapped


def _split_visible_chunk(text: str, width: int) -> list[str]:
    pieces: list[str] = []
    current = ""
    current_width = 0
    index = 0
    while index < len(text):
        match = _ANSI_RE.match(text, index)
        if match:
            current += match.group(0)
            index = match.end()
            continue
        char = text[index]
        char_width = _display_width(char)
        if current_width + char_width > width and current:
            pieces.append(current)
            current = ""
            current_width = 0
        current += char
        current_width += char_width
        index += 1
    if current:
        pieces.append(current)
    return pieces or [text]


def _ellipsis_text(omitted: int, *, transcript_hint: bool = False) -> str:
    suffix = " (ctrl + t to view transcript)" if transcript_hint else ""
    return f"… +{omitted} lines{suffix}"


def _truncate_middle_parts(lines: list[str], max_lines: int) -> tuple[list[str], list[str], int]:
    if len(lines) <= max_lines:
        return lines, [], 0
    if max_lines <= 1:
        return [], [], len(lines)
    head_count = max(1, (max_lines - 1) // 2)
    tail_count = max(0, max_lines - 1 - head_count)
    omitted = max(0, len(lines) - head_count - tail_count)
    tail = lines[len(lines) - tail_count :] if tail_count else []
    return lines[:head_count], tail, omitted


def _render_markdown_for_terminal(
    text: str,
    style: _AnsiStyle,
    *,
    emphasis: bool = True,
    terminal_width: int | None = None,
) -> list[str]:
    lines: list[str] = []
    in_code_fence = False
    normalized_text = _unwrap_markdown_fences(text)
    raw_lines = normalized_text.splitlines() or [normalized_text]
    index = 0
    width = terminal_width or shutil.get_terminal_size((100, 24)).columns
    while index < len(raw_lines):
        raw_line = raw_lines[index]
        stripped = raw_line.strip()
        if stripped.startswith(("```", "~~~")):
            in_code_fence = not in_code_fence
            index += 1
            continue
        if in_code_fence:
            lines.append(style.dim(raw_line))
            index += 1
            continue
        table = _markdown_table_at(raw_lines, index)
        if table is not None:
            rendered, alignments, consumed = table
            lines.extend(
                _render_markdown_table(
                    rendered,
                    style,
                    emphasis=emphasis,
                    terminal_width=width,
                    alignments=alignments,
                )
            )
            index += consumed
            continue
        if _is_markdown_rule(stripped):
            lines.append(style.dim("─" * max(20, width)))
            index += 1
            continue
        heading = re.match(r"^(#{1,6})\s+(.+?)\s*$", raw_line)
        if heading:
            heading_text = _format_inline_markdown(heading.group(2), style, emphasis=emphasis)
            lines.append(style.bold(heading_text) if emphasis else heading_text)
            index += 1
            continue
        quote = re.match(r"^(\s*)>\s?(.*)$", raw_line)
        if quote:
            lines.append(
                f"{quote.group(1)}{style.dim('>')} "
                f"{style.dim(_format_inline_markdown(quote.group(2), style, emphasis=emphasis))}"
            )
            index += 1
            continue
        lines.append(_format_inline_markdown(raw_line, style, emphasis=emphasis))
        index += 1
    return lines


def _render_reasoning_for_terminal(
    text: str,
    style: _AnsiStyle,
    *,
    terminal_width: int | None = None,
) -> list[str]:
    normalized = _flatten_reasoning_heading(text.strip())
    return _render_markdown_for_terminal(normalized, style, emphasis=False, terminal_width=terminal_width)


def _flatten_reasoning_heading(text: str) -> str:
    strong = re.match(r"^\*\*([^*\n]+)\*\*\s*\n\s*\n(.+)$", text, flags=re.DOTALL)
    if strong:
        return f"{strong.group(1).strip()}: {strong.group(2).lstrip()}"
    heading = re.match(r"^#{1,6}\s+(.+?)\s*\n\s*\n(.+)$", text, flags=re.DOTALL)
    if heading:
        return f"{heading.group(1).strip()}: {heading.group(2).lstrip()}"
    return text


def _format_inline_markdown(text: str, style: _AnsiStyle, *, emphasis: bool = True) -> str:
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 (\2)", text)
    text = re.sub(r"`([^`\n]+)`", lambda match: style.cyan(match.group(1)) if emphasis else match.group(1), text)
    text = re.sub(r"\*\*([^*\n]+)\*\*", lambda match: style.bold(match.group(1)) if emphasis else match.group(1), text)
    text = re.sub(r"__([^_\n]+)__", lambda match: style.bold(match.group(1)) if emphasis else match.group(1), text)
    text = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"\1", text)
    text = re.sub(r"(?<![\w])_([^_\n]+)_(?![\w])", r"\1", text)
    return text


def _unwrap_markdown_fences(text: str) -> str:
    raw_lines = text.splitlines(keepends=True)
    out: list[str] = []
    index = 0
    while index < len(raw_lines):
        line = raw_lines[index]
        stripped = line.strip().lower()
        fence = None
        for marker in ("```", "~~~"):
            if stripped.startswith(marker):
                info = stripped[len(marker) :].strip()
                if info in {"md", "markdown"}:
                    fence = marker
                break
        if fence is None:
            out.append(line)
            index += 1
            continue
        body: list[str] = []
        close_index = index + 1
        while close_index < len(raw_lines):
            if raw_lines[close_index].strip().startswith(fence):
                break
            body.append(raw_lines[close_index])
            close_index += 1
        if close_index < len(raw_lines) and _contains_markdown_table(body):
            out.extend(body)
            index = close_index + 1
        else:
            out.append(line)
            out.extend(body)
            if close_index < len(raw_lines):
                out.append(raw_lines[close_index])
                index = close_index + 1
            else:
                index = close_index
    return "".join(out)


def _contains_markdown_table(lines: list[str]) -> bool:
    plain = [line.rstrip("\n") for line in lines]
    return any(
        _markdown_table_at(plain, index) is not None
        for index in range(max(0, len(plain) - 1))
    )


def _markdown_table_at(lines: list[str], start: int) -> tuple[list[list[str]], list[str], int] | None:
    if start + 1 >= len(lines):
        return None
    header = _parse_table_row(lines[start])
    separator = _parse_table_row(lines[start + 1])
    if header is None or separator is None or not _is_table_separator_row(separator):
        return None
    alignments = [_table_alignment(cell) for cell in separator]
    rows = [header]
    index = start + 2
    while index < len(lines):
        row = _parse_table_row(lines[index])
        if row is None:
            break
        rows.append(row)
        index += 1
    width = max(len(row) for row in rows)
    normalized = [row + [""] * (width - len(row)) for row in rows]
    return normalized, alignments + ["left"] * (width - len(alignments)), index - start


def _parse_table_row(line: str) -> list[str] | None:
    stripped = line.strip()
    if "|" not in stripped:
        return None
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    cells = [cell.strip() for cell in stripped.split("|")]
    return cells if len(cells) >= 2 else None


def _is_table_separator_row(cells: list[str]) -> bool:
    for cell in cells:
        if not re.fullmatch(r":?-{3,}:?", cell.strip()):
            return False
    return True


def _table_alignment(cell: str) -> str:
    stripped = cell.strip()
    if stripped.startswith(":") and stripped.endswith(":"):
        return "center"
    if stripped.endswith(":"):
        return "right"
    return "left"


@dataclass
class _TableColumnMetrics:
    max_width: int
    header_token_width: int
    body_token_width: int
    avg_words_per_cell: float
    avg_cell_width: float
    kind: str


def _render_markdown_table(
    rows: list[list[str]],
    style: _AnsiStyle,
    *,
    emphasis: bool = True,
    terminal_width: int | None = None,
    alignments: list[str] | None = None,
) -> list[str]:
    if not rows or not rows[0]:
        return []
    column_count = len(rows[0])
    normalized = [row[:column_count] + [""] * (column_count - len(row)) for row in rows]
    alignments = (alignments or ["left"] * column_count)[:column_count] + ["left"] * max(
        0,
        column_count - len(alignments or []),
    )
    widths = _table_column_widths(
        normalized,
        terminal_width or shutil.get_terminal_size((100, 24)).columns,
    )
    if widths is None:
        return _render_table_pipe_fallback(normalized, style, emphasis=emphasis)

    rendered: list[str] = []
    rendered.append(_render_table_border("┌", "┬", "┐", widths, style))
    rendered.extend(_render_table_row(normalized[0], widths, alignments, style, emphasis=emphasis, header=True))
    rendered.append(_render_table_border("├", "┼", "┤", widths, style))
    for row in normalized[1:]:
        rendered.extend(_render_table_row(row, widths, alignments, style, emphasis=emphasis, header=False))
    rendered.append(_render_table_border("└", "┴", "┘", widths, style))
    return rendered


def _table_column_widths(rows: list[list[str]], terminal_width: int) -> list[int] | None:
    column_count = len(rows[0])
    min_column_width = 3
    border_width = 1 + (column_count * 3)
    available_width = max(0, terminal_width - border_width)
    if available_width < column_count * min_column_width:
        return None
    metrics = _collect_table_metrics(rows)
    widths = [max(metric.max_width, min_column_width) for metric in metrics]
    if sum(widths) <= available_width:
        return widths
    floors = [_preferred_column_floor(metric, min_column_width) for metric in metrics]
    while sum(floors) > available_width:
        candidates = [
            index
            for index, floor in enumerate(floors)
            if floor > min_column_width
        ]
        if not candidates:
            break
        index = min(candidates, key=lambda idx: (0 if metrics[idx].kind == "narrative" else 1, floors[idx]))
        floors[index] -= 1
    while sum(widths) > available_width:
        candidates = [
            index
            for index, width in enumerate(widths)
            if width > floors[index]
        ]
        if not candidates:
            return None
        index = min(candidates, key=lambda idx: _table_shrink_key(idx, widths, floors, metrics))
        widths[index] -= 1
    return widths


def _collect_table_metrics(rows: list[list[str]]) -> list[_TableColumnMetrics]:
    column_count = len(rows[0])
    metrics: list[_TableColumnMetrics] = []
    for column in range(column_count):
        header = rows[0][column]
        body = [row[column] for row in rows[1:]]
        header_token_width = _longest_token_width(header)
        body_token_width = max((_longest_token_width(cell) for cell in body), default=0)
        max_width = max(_cell_display_width(cell) for cell in [header, *body])
        non_empty_body = [cell for cell in body if cell.strip()]
        if non_empty_body:
            avg_words = sum(len(cell.split()) for cell in non_empty_body) / len(non_empty_body)
            avg_width = sum(_display_width(_plain_cell_text(cell)) for cell in non_empty_body) / len(non_empty_body)
        else:
            avg_words = float(len(header.split()))
            avg_width = float(_display_width(_plain_cell_text(header)))
        if body_token_width >= 20 and avg_words <= 2.0:
            kind = "structured"
        elif avg_words >= 4.0 or avg_width >= 28.0:
            kind = "narrative"
        else:
            kind = "structured"
        metrics.append(
            _TableColumnMetrics(
                max_width=max_width,
                header_token_width=header_token_width,
                body_token_width=body_token_width,
                avg_words_per_cell=avg_words,
                avg_cell_width=avg_width,
                kind=kind,
            )
        )
    return metrics


def _preferred_column_floor(metrics: _TableColumnMetrics, min_column_width: int) -> int:
    if metrics.kind == "narrative":
        token_target = min(metrics.header_token_width, 10)
    else:
        token_target = max(metrics.header_token_width, min(metrics.body_token_width, 16))
    return min(metrics.max_width, max(min_column_width, token_target))


def _table_shrink_key(
    index: int,
    widths: list[int],
    floors: list[int],
    metrics: list[_TableColumnMetrics],
) -> tuple[int, int]:
    metric = metrics[index]
    slack = widths[index] - floors[index]
    kind_cost = 0 if metric.kind == "narrative" else 2
    header_guard = 3 if widths[index] <= metric.header_token_width else 0
    density_guard = 0 if metric.avg_words_per_cell >= 4.0 or metric.avg_cell_width >= 24.0 else 1
    return kind_cost + header_guard + density_guard, -slack


def _longest_token_width(text: str) -> int:
    return max((_display_width(token) for token in _plain_cell_text(text).split()), default=0)


def _cell_display_width(text: str) -> int:
    plain_lines = _plain_cell_text(text).splitlines() or [""]
    return max(_display_width(line) for line in plain_lines)


def _plain_cell_text(text: str) -> str:
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"`([^`\n]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*\n]+)\*\*", r"\1", text)
    text = re.sub(r"__([^_\n]+)__", r"\1", text)
    text = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"\1", text)
    text = re.sub(r"(?<!_)_([^_\n]+)_(?!_)", r"\1", text)
    return text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")


def _render_table_border(left: str, sep: str, right: str, widths: list[int], style: _AnsiStyle) -> str:
    return style.dim(left + sep.join("─" * (width + 2) for width in widths) + right)


def _render_table_row(
    row: list[str],
    widths: list[int],
    alignments: list[str],
    style: _AnsiStyle,
    *,
    emphasis: bool,
    header: bool,
) -> list[str]:
    wrapped_cells = [_wrap_table_cell(cell, widths[index], style, emphasis=emphasis) for index, cell in enumerate(row)]
    row_height = max((len(cell_lines) for cell_lines in wrapped_cells), default=1)
    rendered: list[str] = []
    for line_index in range(row_height):
        parts = [style.dim("│")]
        for column, width in enumerate(widths):
            cell_line = wrapped_cells[column][line_index] if line_index < len(wrapped_cells[column]) else ""
            if header and emphasis and cell_line:
                cell_line = style.bold(cell_line)
            parts.append(" ")
            parts.append(_align_ansi(cell_line, width, alignments[column]))
            parts.append(" ")
            parts.append(style.dim("│"))
        rendered.append("".join(parts))
    return rendered


def _wrap_table_cell(text: str, width: int, style: _AnsiStyle, *, emphasis: bool) -> list[str]:
    raw = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    formatted = _format_inline_markdown(raw, style, emphasis=emphasis)
    lines: list[str] = []
    for logical_line in formatted.splitlines() or [""]:
        if not logical_line:
            lines.append("")
        else:
            lines.extend(_wrap_ansi_line(logical_line, width))
    return lines or [""]


def _align_ansi(text: str, width: int, alignment: str) -> str:
    remaining = max(0, width - _visible_len(text))
    if alignment == "right":
        return (" " * remaining) + text
    if alignment == "center":
        left = remaining // 2
        return (" " * left) + text + (" " * (remaining - left))
    return text + (" " * remaining)


def _render_table_pipe_fallback(rows: list[list[str]], style: _AnsiStyle, *, emphasis: bool) -> list[str]:
    rendered: list[str] = []
    for index, row in enumerate(rows):
        line = "| " + " | ".join(_plain_cell_text(cell).replace("|", "\\|") for cell in row) + " |"
        rendered.append(_format_inline_markdown(line, style, emphasis=emphasis))
        if index == 0:
            rendered.append("|" + "|".join("---" for _ in row) + "|")
    return rendered


def _is_markdown_rule(text: str) -> bool:
    if len(text) < 3:
        return False
    return all(char == "-" for char in text) or all(char == "*" for char in text) or all(char == "_" for char in text)


def _assistant_item_text(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    content = item.get("content")
    if not isinstance(content, list):
        return ""
    for part in content:
        if isinstance(part, dict) and part.get("type") in {"output_text", "text"} and isinstance(part.get("text"), str):
            chunks.append(part["text"])
    return "\n".join(chunks)


def _user_item_text(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    content = item.get("content")
    if not isinstance(content, list):
        return ""
    for part in content:
        if isinstance(part, dict) and part.get("type") in {"input_text", "text"} and isinstance(part.get("text"), str):
            chunks.append(part["text"])
    return "\n".join(chunks)


def _reasoning_item_text(item: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in ("summary", "content"):
        value = item.get(key)
        if isinstance(value, str):
            chunks.append(value)
            continue
        if isinstance(value, list):
            for part in value:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    chunks.append(part["text"])
                elif isinstance(part, str):
                    chunks.append(part)
    return "\n".join(chunk for chunk in chunks if chunk)


def _web_search_query(item: dict[str, Any]) -> str:
    action = item.get("action")
    if isinstance(action, dict) and isinstance(action.get("query"), str):
        return action["query"]
    query = item.get("query")
    return query if isinstance(query, str) else ""


def _web_search_action_detail(action: dict[str, Any]) -> str:
    action_type = str(action.get("type") or "")
    if action_type == "search":
        query = action.get("query")
        if isinstance(query, str) and query:
            return query
        queries = action.get("queries")
        if isinstance(queries, list) and queries:
            first = str(queries[0])
            return f"{first} ..." if len(queries) > 1 and first else first
        return ""
    if action_type == "open_page":
        url = action.get("url")
        return url if isinstance(url, str) else ""
    if action_type == "find_in_page":
        pattern = action.get("pattern")
        url = action.get("url")
        if isinstance(pattern, str) and isinstance(url, str):
            return f"'{pattern}' in {url}"
        if isinstance(pattern, str):
            return f"'{pattern}'"
        return url if isinstance(url, str) else ""
    return ""


def _command_display(command: str) -> str:
    try:
        tokens = shlex.split(command)
    except ValueError:
        return command
    if len(tokens) >= 3 and Path(tokens[0]).name in {"bash", "zsh", "sh"} and tokens[1] in {"-lc", "-c"}:
        return tokens[2]
    return command


def _duration_ms(raw: Any) -> int | None:
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return int(seconds * 1000)


def _duration_suffix(raw: Any) -> str:
    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        return ""
    if seconds <= 0:
        return ""
    millis = int(seconds * 1000)
    return f" in {millis}ms"


def _int_value(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _resolve_resume_rollout(args: argparse.Namespace, config: CodexConfig) -> Path | None:
    if args.last:
        return _latest_rollout(config.resolved_codex_home(), cwd=config.resolved_cwd(), all_cwds=args.all_cwds)
    selector = args.session_id
    if not selector:
        return None
    explicit_path = _resolve_explicit_rollout_path(selector, config.resolved_cwd())
    if explicit_path is not None:
        return explicit_path
    return _find_rollout_by_thread_id(
        config.resolved_codex_home(),
        selector,
        cwd=config.resolved_cwd(),
        all_cwds=args.all_cwds,
    )


def _resolve_explicit_rollout_path(selector: str, cwd: Path) -> Path | None:
    candidates = [Path(selector).expanduser()]
    if not candidates[0].is_absolute():
        candidates.append(cwd / candidates[0])
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate.resolve()
    return None


def _latest_rollout(codex_home: Path, *, cwd: Path, all_cwds: bool) -> Path | None:
    for path in _iter_rollout_paths(codex_home):
        reconstruction = _safe_reconstruct_rollout(path)
        if reconstruction is None:
            continue
        if all_cwds or _rollout_cwd_matches(reconstruction.session_meta, cwd):
            return path
    return None


def _find_rollout_by_thread_id(codex_home: Path, selector: str, *, cwd: Path, all_cwds: bool) -> Path | None:
    for path in _iter_rollout_paths(codex_home):
        reconstruction = _safe_reconstruct_rollout(path)
        if reconstruction is None:
            continue
        if not all_cwds and not _rollout_cwd_matches(reconstruction.session_meta, cwd):
            continue
        thread_id = _rollout_thread_id(reconstruction.session_meta)
        if thread_id == selector or selector in path.name:
            return path
    return None


def _iter_rollout_paths(codex_home: Path) -> list[Path]:
    sessions = codex_home / "sessions"
    if not sessions.exists():
        return []
    paths = [path for path in sessions.glob("????/??/??/rollout-*.jsonl") if path.is_file()]
    return sorted(paths, key=lambda path: (_safe_mtime(path), path.name), reverse=True)


def _safe_reconstruct_rollout(path: Path):
    try:
        return reconstruct_history_from_rollout(path)
    except Exception:
        return None


def _rollout_thread_id(session_meta: dict | None) -> str | None:
    if not isinstance(session_meta, dict):
        return None
    value = session_meta.get("id")
    return str(value) if value else None


def _rollout_cwd_matches(session_meta: dict | None, cwd: Path) -> bool:
    if not isinstance(session_meta, dict):
        return False
    raw_cwd = session_meta.get("cwd")
    if not isinstance(raw_cwd, str) or not raw_cwd:
        return False
    try:
        return Path(raw_cwd).expanduser().resolve() == cwd.resolve()
    except OSError:
        return False


def _safe_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def _read_prompt(prompt_arg: str | None) -> str:
    stdin_text = ""
    if prompt_arg == "-" or not sys.stdin.isatty():
        stdin_text = sys.stdin.read()
    if prompt_arg and prompt_arg != "-":
        if stdin_text:
            return f"{prompt_arg}\n\n<stdin>\n{stdin_text}\n</stdin>"
        return prompt_arg
    return stdin_text


def _load_output_schema(path: str | None) -> dict | None:
    if path is None:
        return None
    schema_path = Path(path)
    try:
        value = json.loads(schema_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Failed to read output schema file {schema_path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Output schema file {schema_path} is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Output schema file {schema_path} must contain a JSON object")
    return value


def _parse_image_args(values: list[str]) -> list[str]:
    paths: list[str] = []
    for value in values:
        paths.extend(part for part in value.split(",") if part)
    return paths


def _resolve_oss_provider(args: argparse.Namespace, config: dict) -> str | None:
    if not args.oss:
        return None
    provider = args.local_provider or _string_config(config, "oss_provider")
    if not provider:
        raise ValueError(
            "No default OSS provider configured. Use --local-provider=provider or set "
            "oss_provider to one of: lmstudio, ollama in config.toml"
        )
    if provider not in {"lmstudio", "ollama"}:
        raise ValueError(f"Invalid OSS provider `{provider}`; expected one of: lmstudio, ollama")
    return provider


def _exec_model(args: argparse.Namespace, config: dict, oss_provider: str | None) -> str:
    if args.model:
        return args.model
    if oss_provider:
        return _default_oss_model(oss_provider)
    return _string_config(config, "model") or CodexConfig().model


def _default_oss_model(provider: str) -> str:
    if provider == "lmstudio":
        return "openai/gpt-oss-20b"
    return "gpt-oss:20b"


def _load_cli_config(args: argparse.Namespace) -> dict:
    config: dict = {}
    if not args.ignore_user_config:
        path = _default_config_path()
        if path.exists():
            try:
                config = tomllib.loads(path.read_text(encoding="utf-8"))
            except (OSError, tomllib.TOMLDecodeError) as exc:
                raise ValueError(f"Error loading config.toml: {exc}") from exc
    for override in args.config_overrides:
        key, value = _parse_config_override(override)
        _apply_dotted_config(config, key, value)
    profile_name = args.profile or _string_config(config, "profile")
    if not profile_name:
        return _without_profiles(config)
    profiles = config.get("profiles", {})
    if not isinstance(profiles, dict) or profile_name not in profiles or not isinstance(profiles[profile_name], dict):
        raise ValueError(f"Config profile `{profile_name}` was not found in config.toml")
    return _deep_merge(_without_profiles(config), profiles[profile_name])


def _default_codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME") or os.environ.get("CODEX_PY_HOME", "~/.codex-python")).expanduser()


def _default_config_path() -> Path:
    codex_home = os.environ.get("CODEX_HOME")
    if codex_home:
        return Path(codex_home).expanduser() / "config.toml"
    official = Path("~/.codex/config.toml").expanduser()
    if official.exists():
        return official
    return _default_codex_home() / "config.toml"


def _parse_config_override(raw: str) -> tuple[str, object]:
    key, separator, value = raw.partition("=")
    key = key.strip()
    if not separator or not key:
        raise ValueError(f"Invalid -c/--config override `{raw}`; expected key=value")
    value = value.strip()
    try:
        parsed = tomllib.loads(f"value = {value}")["value"]
    except tomllib.TOMLDecodeError:
        parsed = value.strip("\"'")
    return key, parsed


def _apply_dotted_config(config: dict, key: str, value: object) -> None:
    parts = [part for part in key.split(".") if part]
    if not parts:
        raise ValueError(f"Invalid empty config override key `{key}`")
    cursor = config
    for part in parts[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[parts[-1]] = value


def _without_profiles(config: dict) -> dict:
    return {key: value for key, value in config.items() if key != "profiles"}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _string_config(config: dict, key: str) -> str | None:
    value = config.get(key)
    return value if isinstance(value, str) and value else None


def _bool_config(config: dict, key: str, default: bool) -> bool:
    value = config.get(key)
    return value if isinstance(value, bool) else default


def _remote_compaction_config(config: dict) -> str:
    value = (_string_config(config, "remote_compaction") or os.environ.get("PY_CODEX_REMOTE_COMPACTION") or "auto").lower()
    if value not in {"auto", "off", "required"}:
        raise ValueError("remote_compaction must be one of: auto, off, required")
    return value


def _bool_nested_config(config: dict, path: tuple[str, ...], default: bool) -> bool:
    value: object = config
    for part in path:
        if not isinstance(value, dict):
            return default
        value = value.get(part)
    return value if isinstance(value, bool) else default


def _int_nested_config(config: dict, path: tuple[str, ...], default: int) -> int:
    value: object = config
    for part in path:
        if not isinstance(value, dict):
            return default
        value = value.get(part)
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _int_config(config: dict, key: str) -> int | None:
    value = config.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _model_provider_config(config: dict, provider_id: str) -> dict:
    providers = config.get("model_providers")
    if not isinstance(providers, dict):
        return {}
    provider = providers.get(provider_id)
    return provider if isinstance(provider, dict) else {}


def _path_list_config(config: dict, key: str) -> list[str]:
    value = config.get(key)
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if isinstance(item, str)]
    return []


def _sandbox_config(config: dict) -> str | None:
    value = _string_config(config, "sandbox_mode")
    return value if value in {"read-only", "workspace-write", "danger-full-access"} else None


def _approval_config(config: dict) -> str | None:
    value = _string_config(config, "approval_policy")
    return value if value in {"untrusted", "on-failure", "on-request", "never"} else None


def _collaboration_mode_config(config: dict) -> str:
    value = _string_config(config, "collaboration_mode") or _string_config(config, "mode")
    normalized = (value or "Default").replace("_", " ").replace("-", " ").strip().lower()
    modes = {
        "default": "Default",
        "plan": "Plan",
        "execute": "Execute",
        "pair programming": "Pair Programming",
        "pair": "Pair Programming",
    }
    return modes.get(normalized, "Default")


def _request_user_input_available_modes(config: dict) -> tuple[str, ...]:
    value = config.get("request_user_input_available_modes")
    if isinstance(value, list):
        modes = tuple(
            mode
            for raw in value
            if isinstance(raw, str)
            for mode in [_collaboration_mode_config({"collaboration_mode": raw})]
        )
        if modes:
            return modes
    if _bool_nested_config(config, ("features", "default_mode_request_user_input"), False):
        return ("Default", "Plan")
    return ("Plan",)


def _web_search_settings(config: dict) -> tuple[bool, bool]:
    value = config.get("web_search")
    if value in {False, "disabled"}:
        return (False, False)
    if value in {True, "live"}:
        return (True, True)
    return (True, False)


if __name__ == "__main__":
    raise SystemExit(main())
