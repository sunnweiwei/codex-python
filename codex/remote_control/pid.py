from __future__ import annotations

import os
from pathlib import Path

from .constants import REMOTE_CONTROL_PID_FILE
from .types import RemoteControlConfig

def _remote_control_log_path(config: RemoteControlConfig) -> Path:
    return config.codex_home / "remote-control.log"


def _pid_file(config: RemoteControlConfig) -> Path:
    return config.codex_home / REMOTE_CONTROL_PID_FILE


def _read_pid_file(config: RemoteControlConfig) -> int | None:
    try:
        return int(_pid_file(config).read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_pid_file(config: RemoteControlConfig, pid: int | None = None) -> None:
    config.codex_home.mkdir(parents=True, exist_ok=True)
    _pid_file(config).write_text(str(pid or os.getpid()), encoding="utf-8")


def _clear_pid_file(config: RemoteControlConfig) -> None:
    path = _pid_file(config)
    try:
        if path.exists() and _read_pid_file(config) == os.getpid():
            path.unlink()
        elif path.exists():
            pid = _read_pid_file(config)
            if pid is None or not _process_is_running(pid):
                path.unlink()
    except OSError:
        pass


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


