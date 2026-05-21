# Upstream

This project is a Python port of [openai/codex](https://github.com/openai/codex).

- Upstream repository: `https://github.com/openai/codex`
- Pinned commit: `392e94e9ea756cffd89f35941e881d29b2a81a6e`
- Commit date: `2026-05-13 07:13:57 +0000`
- Upstream license: Apache-2.0

The upstream source tree is **not vendored** in this repository. If you want to do parity work locally, clone upstream alongside this repo:

```bash
git clone https://github.com/openai/codex /path/to/openai-codex
cd /path/to/openai-codex && git checkout 392e94e9ea756cffd89f35941e881d29b2a81a6e
```

Prompt assets in `codex/assets/` are copied verbatim from upstream at the pinned commit. Their SHA-256 hashes are recorded in `codex/parity_manifest.json` and verified at load via `codex.prompts.verify_asset_hashes()`.