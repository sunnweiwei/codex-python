from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from ..auth import chatgpt_backend_base_url
from ..types import CodexConfig
from .constants import DEFAULT_REMOTE_CONTROL_BASE_URL
from .utils import (
    _default_remote_control_app_server_client_name,
    _default_remote_control_app_server_client_version,
    _default_remote_control_server_name,
    _default_remote_control_user_agent_override,
    _env_bool,
    _validate_remote_control_server_name,
)

RemoteControlConnectionStatus = Literal["disabled", "connecting", "connected", "errored"]
RemoteControlMode = Literal["foreground", "daemon"]


class RemoteControlError(RuntimeError):
    pass


class RemoteControlUnavailable(RemoteControlError):
    pass


@dataclass(frozen=True)
class RemoteControlTarget:
    websocket_url: str
    enroll_url: str


@dataclass(frozen=True)
class EnrollRemoteServerRequest:
    name: str
    os: str
    arch: str
    app_server_version: str
    installation_id: str


@dataclass(frozen=True)
class RemoteControlReadyStatus:
    status: RemoteControlConnectionStatus
    server_name: str
    environment_id: str | None = None
    timed_out: bool = False


@dataclass(frozen=True)
class RemoteControlStartJsonOutput:
    mode: RemoteControlMode
    status: RemoteControlConnectionStatus
    server_name: str
    environment_id: str | None
    timed_out: bool
    daemon: dict[str, Any] | None = None

    def to_json(self) -> str:
        payload = {
            "mode": self.mode,
            "status": self.status,
            "serverName": self.server_name,
            "environmentId": self.environment_id,
            "timedOut": self.timed_out,
        }
        if self.daemon is not None:
            payload["daemon"] = self.daemon
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


@dataclass(frozen=True)
class RemoteControlEnrollment:
    account_id: str
    environment_id: str
    server_id: str
    server_name: str
    app_server_version: str | None = None
    enroll_os: str | None = None
    enroll_arch: str | None = None


@dataclass(frozen=True)
class RemoteControlAuth:
    access_token: str
    account_id: str
    is_fedramp_account: bool = False


@dataclass(frozen=True)
class RemoteControlConfig:
    codex_home: Path
    auth_codex_home: Path | None = None
    codex_config: CodexConfig | None = None
    cwd: Path = field(default_factory=Path.cwd)
    model: str | None = None
    remote_control_url: str = DEFAULT_REMOTE_CONTROL_BASE_URL
    server_name: str = field(default_factory=_default_remote_control_server_name)
    app_server_client_name: str | None = field(
        default_factory=_default_remote_control_app_server_client_name
    )
    app_server_client_version: str | None = field(
        default_factory=_default_remote_control_app_server_client_version
    )
    allow_desktop_compat_identity: bool = field(
        default_factory=lambda: _env_bool("PY_CODEX_REMOTE_CONTROL_ALLOW_DESKTOP_COMPAT", default=True)
    )
    user_agent_override: str | None = field(
        default_factory=_default_remote_control_user_agent_override
    )
    json_output: bool = False
    foreground: bool = True
    quiet: bool = False

    def __post_init__(self) -> None:
        codex_home = Path(self.codex_home).expanduser().resolve()
        official_home = (Path.home() / ".codex").resolve()
        if codex_home == official_home:
            raise RemoteControlError(
                "Python remote-control must not use the official Codex/Desktop state directory "
                f"{official_home}. Set CODEX_PY_HOME to a separate directory such as ~/.codex-python."
            )
        object.__setattr__(self, "codex_home", codex_home)
        if self.auth_codex_home is not None:
            object.__setattr__(self, "auth_codex_home", Path(self.auth_codex_home).expanduser().resolve())
        object.__setattr__(self, "server_name", _validate_remote_control_server_name(self.server_name))

    @classmethod
    def from_codex_config(
        cls,
        config: CodexConfig,
        *,
        json_output: bool = False,
        foreground: bool = True,
    ) -> "RemoteControlConfig":
        return cls(
            codex_home=config.resolved_codex_home(),
            auth_codex_home=config.resolved_auth_codex_home(),
            codex_config=config,
            cwd=config.resolved_cwd(),
            model=config.model,
            remote_control_url=chatgpt_backend_base_url(config.chatgpt_base_url),
            json_output=json_output,
            foreground=foreground,
        )
