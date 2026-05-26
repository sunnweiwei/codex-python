from __future__ import annotations

from typing import Any

from .types import RemoteControlError, RemoteControlReadyStatus, RemoteControlStartJsonOutput, RemoteControlMode

def remote_control_start_human_lines(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
) -> list[str]:
    return _remote_control_start_human_lines(status, mode=mode)


def remote_control_start_json_output(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
    daemon: dict[str, Any] | None = None,
) -> RemoteControlStartJsonOutput:
    _ensure_remote_control_startable(status)
    return RemoteControlStartJsonOutput(
        mode=mode,
        status=status.status,
        server_name=status.server_name,
        environment_id=status.environment_id,
        timed_out=status.timed_out,
        daemon=daemon,
    )


def remote_control_stop_human_message(status: str) -> str:
    if status == "stopped":
        return "Remote control stopped."
    if status == "notRunning":
        return "Remote control is not running."
    return f"Remote control stop completed with status {status}."


def remote_control_official_args(
    subcommand: str | None = None,
    *,
    json_output: bool = False,
) -> list[str]:
    args = ["remote-control"]
    if json_output:
        args.append("--json")
    if subcommand:
        if subcommand not in {"start", "stop"}:
            raise ValueError(f"unsupported remote-control subcommand `{subcommand}`")
        args.append(subcommand)
    return args



def _remote_control_start_human_lines(
    status: RemoteControlReadyStatus,
    *,
    mode: RemoteControlMode,
) -> list[str]:
    _ensure_remote_control_startable(status)
    if status.status == "connected":
        lines = [f"This machine is available for remote control as {status.server_name}."]
    else:
        lines = [f"Remote control is enabled on {status.server_name} and still connecting."]
    if mode == "foreground":
        lines.append("Press Ctrl-C to stop.")
    return lines


def _ensure_remote_control_startable(status: RemoteControlReadyStatus) -> None:
    if status.status in {"connected", "connecting"}:
        return
    if status.status == "errored":
        raise RemoteControlError(
            f"Remote control is enabled on {status.server_name} but the connection is errored."
        )
    raise RemoteControlError(f"Remote control is disabled on {status.server_name}.")

