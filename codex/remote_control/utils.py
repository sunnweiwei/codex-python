from __future__ import annotations

import json
import os
import platform
import sys
import time
from ipaddress import ip_address
from urllib.parse import ParseResult

from ..auth import ORIGINATOR
from .constants import PYTHON_REMOTE_CONTROL_VERSION, REMOTE_CONTROL_SERVER_NAME_ENV, REQUIRED_REMOTE_CONTROL_SERVER_NAME

REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_NAME = "Codex Desktop"
REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_VERSION = "dumb"


def _env_truthy(name: str) -> bool:
    value = os.environ.get(name)
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _env_bool(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _default_remote_control_server_name() -> str:
    configured = os.environ.get(REMOTE_CONTROL_SERVER_NAME_ENV)
    if configured:
        return _validate_remote_control_server_name(configured, source=REMOTE_CONTROL_SERVER_NAME_ENV)
    return REQUIRED_REMOTE_CONTROL_SERVER_NAME


def _validate_remote_control_server_name(value: str, *, source: str = "server_name") -> str:
    name = (value or "").strip()
    if name != REQUIRED_REMOTE_CONTROL_SERVER_NAME:
        raise ValueError(
            f"{source} must be exactly {REQUIRED_REMOTE_CONTROL_SERVER_NAME!r}; got {value!r}. "
            "Python remote-control uses the same fixed host identity as upstream Codex so the iPhone app does not see split devices."
        )
    return name


def _remote_control_enroll_os() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform in {"win32", "cygwin", "msys"}:
        return "windows"
    if sys.platform.startswith("freebsd"):
        return "freebsd"
    if sys.platform.startswith("openbsd"):
        return "openbsd"
    if sys.platform.startswith("netbsd"):
        return "netbsd"
    return sys.platform


def _remote_control_enroll_arch() -> str:
    machine = (platform.machine() or platform.processor() or "unknown").lower()
    rust_arch_aliases = {
        "amd64": "x86_64",
        "x64": "x86_64",
        "arm64": "aarch64",
    }
    return rust_arch_aliases.get(machine, machine)


def _remote_control_os_display_and_version() -> tuple[str, str]:
    system = platform.system().lower()
    if system == "darwin":
        version = platform.mac_ver()[0] or platform.release()
        if version and version.count(".") == 1:
            version = f"{version}.0"
        return "Mac OS", version
    if system.startswith("win"):
        return "Windows", platform.release()
    if system:
        return system.capitalize(), platform.release()
    return sys.platform, platform.release()


def _codex_user_agent(
    originator: str = ORIGINATOR,
    suffix: str | None = None,
    *,
    override: str | None = None,
) -> str:
    explicit = override
    if explicit and explicit.strip():
        return explicit.strip()
    os_display, os_version = _remote_control_os_display_and_version()
    arch = platform.machine() or platform.processor() or "unknown"
    value = f"{originator}/{PYTHON_REMOTE_CONTROL_VERSION} ({os_display} {os_version}; {arch}) unknown"
    suffix = (suffix or "").strip()
    if suffix:
        value = f"{value} ({suffix})"
    return value


def _codex_desktop_compat_user_agent(client_version: str | None = None) -> str:
    os_display, os_version = _remote_control_os_display_and_version()
    arch = platform.machine() or platform.processor() or "unknown"
    suffix = (client_version or REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_VERSION).strip()
    return f"{REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_NAME}/{PYTHON_REMOTE_CONTROL_VERSION} ({os_display} {os_version}; {arch}) {suffix}"


def _default_remote_control_app_server_client_name() -> str:
    return os.environ.get("PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_NAME") or REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_NAME


def _default_remote_control_app_server_client_version() -> str:
    return os.environ.get("PY_CODEX_REMOTE_CONTROL_APP_SERVER_CLIENT_VERSION") or REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_VERSION


def _default_remote_control_user_agent_override() -> str:
    configured = os.environ.get("PY_CODEX_REMOTE_CONTROL_USER_AGENT")
    if configured:
        return configured
    return _codex_desktop_compat_user_agent(_default_remote_control_app_server_client_version())


def _remote_control_client_identity(
    app_server_client_name: str | None,
    app_server_client_version: str | None,
    *,
    allow_desktop_compat_identity: bool = False,
) -> tuple[str, str | None]:
    default_originator = os.environ.get("CODEX_INTERNAL_ORIGINATOR_OVERRIDE") or ORIGINATOR
    if (
        app_server_client_name
        and app_server_client_name.strip().lower() == REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_NAME.lower()
    ):
        if allow_desktop_compat_identity:
            return "Codex Desktop", (app_server_client_version or "").strip() or None
        return default_originator, None
    if app_server_client_name:
        version = app_server_client_version or PYTHON_REMOTE_CONTROL_VERSION
        return app_server_client_name, f"{app_server_client_name}; {version}"
    return default_originator, None


def _effective_app_server_client_name(config: object) -> str | None:
    name = getattr(config, "app_server_client_name", None)
    if name and name.strip().lower() == REMOTE_CONTROL_DESKTOP_COMPAT_CLIENT_NAME.lower():
        return None
    return name


def _remote_log(event: str, **fields: object) -> None:
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


def _ceil_div(left: int, right: int) -> int:
    return -(-left // right)


def _ensure_trailing_path_slash(parsed: ParseResult) -> ParseResult:
    path = parsed.path or "/"
    if not path.endswith("/"):
        path += "/"
    return parsed._replace(path=path)


def _is_allowed_chatgpt_host(host: str | None) -> bool:
    if not host:
        return False
    host = host.lower()
    return host in {
        "chatgpt.com",
        "chat.openai.com",
        "api.chatgpt.com",
        "api.chatgpt-staging.com",
    } or host.endswith(".chatgpt.com")


def _is_localhost(host: str | None) -> bool:
    if not host:
        return False
    lowered = host.lower()
    if lowered == "localhost":
        return True
    try:
        address = ip_address(lowered)
    except ValueError:
        return False
    return address.is_loopback


def _invalid_remote_control_url_message(remote_control_url: str) -> str:
    return (
        f"invalid remote control URL `{remote_control_url}`; expected HTTPS URL for "
        "chatgpt.com or a localhost URL"
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None
