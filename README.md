# codex-python

A pure-Python port of [openai/codex](https://github.com/openai/codex) (originally Rust). Re-implements the public `codex exec` core path; does **not** shell out to the official `codex` binary.

Parity target: request / protocol / tool / prompt compatibility with upstream at a pinned commit.

- Upstream pinned at `392e94e9ea756cffd89f35941e881d29b2a81a6e` (see `UPSTREAM.md`)
- License: Apache-2.0
- Python ≥ 3.11, no runtime dependencies

## Why Python

- Drop into a training loop, eval rig, or notebook as `import codex`
- Swap the model adapter for a fake, assert on emitted events, fuzz tool inputs
- Each subsystem is a single file you can read and monkey-patch

## Quick start

```bash
git clone https://github.com/sunnweiwei/codex-python
cd codex-python
export OPENAI_API_KEY=sk-...          # or put it in secrets/openai.env
python -m codex
```

Optional install:

```bash
pip install -e .
```

## Interactive front-end

```bash
python -m codex
```

- Multi-turn chat in one process, streaming output, tool calls rendered inline
- Mid-turn interrupt / inject context
- Slash commands (`/compact`, `/resume`, `/fork`, ...)
- Honors `--sandbox` / `--approval-policy`

## CLI (one-shot)

```bash
python -m codex exec --skip-git-repo-check "List the files."
python -m codex exec --json --skip-git-repo-check "List the files."   # JSONL events
python -m codex exec resume --last "Keep going."
python -m codex exec fork  --last "Try a different approach."
```

## Python API

`CodexSession` is stateful — call `run()` repeatedly for multi-turn.

```python
from codex import CodexConfig, CodexSession

session = CodexSession(CodexConfig(cwd=".", sandbox="workspace-write"))
print(session.run("Inspect the project and report the next step.").final_message)
```

Multi-turn:

```python
session = CodexSession(CodexConfig(cwd=".", sandbox="workspace-write"))
for msg in [
    "Read the README and a couple of source files.",
    "Find the entry point and explain the request flow.",
    "Write a small failing test for that flow.",
    "Run the test and iterate until it fails for the right reason.",
]:
    print(session.run(msg).final_message)
# session.state.history / session.state.thread_id persist across calls.
```

Streaming:

```python
for event in session.stream("Refactor this module."):
    if event.type == "model.delta":
        print(event.payload.get("delta", ""), end="", flush=True)
```

`CodexConfig` mirrors the upstream config surface: `model`, `provider`, `sandbox`, `approval_policy`, `cwd`, `skip_git_repo_check`, `ephemeral`, profile loading.

## Implemented

- Session + turn loop with follow-up tool calls
- Responses API request assembly
- Tools: `exec_command`, `write_stdin`, `update_plan`, `request_user_input`, `apply_patch`, `view_image`, multi-agent v1, hosted `web_search`
- Sandbox + approval policy (`read-only`, `workspace-write`, `danger-full-access`)
- `apply_patch` parser + applier, validated against upstream fixtures
- Conversation persistence, rollout, `resume` / `fork`
- JSONL event output, compaction, memory read/write, `AGENTS.md` discovery
- Interactive chat front-end

Prompt assets in `codex/assets/` are copied verbatim from upstream; SHA-256 hashes pinned in `codex/parity_manifest.json` and verified at load.

## Module map

| Upstream (Rust) | Python |
|---|---|
| `codex-rs/core` session/turn/loop       | `codex/core.py` |
| `codex-rs/core/src/tools`               | `codex/tools.py` |
| `codex-rs/core` model adapters          | `codex/model.py` |
| `codex-rs/core` state, rollout, resume  | `codex/state.py` |
| `codex-rs/core` prompts / context       | `codex/prompts.py` |
| `codex-rs/core/*.md` prompt assets      | `codex/assets/` |
| `codex-rs/memories`                     | `codex/memory.py` |
| `codex-rs/protocol`                     | `codex/types.py` |
| `codex-rs/cli`, `codex-rs/tui`          | `codex/cli.py` |

## Not covered

App-server / SDKs, full TUI, cloud tasks, plugin marketplace, MCP server hosting, OS-level sandbox binaries. The hosted Codex backend is obviously not portable.

## Tests

```bash
python -m unittest discover tests
```

Apply-patch fixture tests auto-skip if you don't have upstream checked out locally.

## License

Apache-2.0. See `LICENSE` and `UPSTREAM.md`.