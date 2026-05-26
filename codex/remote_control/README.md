# Remote Control Package

This package contains the Python implementation of Codex remote control.  Keep
runtime code here independent from vendored upstream source; upstream may be
used by parity tests, not by this package.

## Module Map

- `constants.py`: protocol constants, compatibility version, socket and state filenames.
- `types.py`: public dataclasses, config, status types, and remote-control errors.
- `protocol.py`: wire envelopes, chunking/reassembly, outbound ack buffer.
- `transport.py`: URL normalization, ChatGPT auth loading, enrollment state, websocket headers.
- `local.py`: local Unix socket used by CLI/TUI clients to talk to the daemon.
- `service.py`: websocket lifecycle, client stream bookkeeping, notification dispatch.
- `app_server.py`: JSON-RPC app-server dispatcher and thread/turn orchestration.
- `app_helpers.py`: app-server helper routines for config/account/fs/process/history payloads.
- `daemon.py`: foreground/start/stop entrypoints and daemon status probing.
- `pid.py`: pid file and process-liveness helpers.
- `display.py`: human/JSON CLI output helpers for `remote-control`.
- `utils.py`: small platform, URL, logging, and parsing helpers.

Remote debugging should usually start from `transport.py` for enrollment/header
issues, `service.py` for websocket/client stream issues, and `app_server.py` for
phone or local-client method handling.
