from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request

from collections.abc import Sequence
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .auth import CodexAuthSnapshot
from .auth import chatgpt_codex_base_url
from .auth import load_auth_snapshot
from .auth import normalize_auth_mode
from .auth import refresh_chatgpt_auth
from .types import ModelResponse, PromptRequest
from .types import CodexConfig


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OPENAI_ENV_FILE = PROJECT_ROOT / "secrets" / "openai.env"
OPENAI_ORIGINATOR = "codex_cli_rs"


@dataclass(frozen=True)
class ModelStreamEvent:
    type: str
    payload: dict[str, Any]


class ModelClient(Protocol):
    def create(self, request: PromptRequest) -> ModelResponse:
        """Create one model response."""

    def stream(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        """Stream model response lifecycle events."""


class RemoteCompactionError(RuntimeError):
    """Raised when the provider compact endpoint cannot return replacement history."""


class OpenAIResponsesModel:
    def __init__(self, *, api_key: str | None = None, base_url: str | None = None):
        self.api_key = api_key or load_openai_api_key()
        self.base_url = base_url or os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1"
        self.auth_display_name = "API key" if self.api_key else "OpenAI SDK auth"

    def create(self, request: PromptRequest) -> ModelResponse:
        return collect_model_stream_events(self.stream(request))

    def stream(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        try:
            from openai import OpenAI
        except ImportError:
            yield from self._stream_via_http(request)
            return

        client = OpenAI(api_key=self.api_key) if self.api_key else OpenAI()
        kwargs = request.to_responses_kwargs()
        extra_body = {}
        client_metadata = kwargs.pop("client_metadata", None)
        if client_metadata is not None:
            extra_body["client_metadata"] = client_metadata
        if extra_body:
            kwargs["extra_body"] = extra_body
        extra_headers = _responses_headers(request)
        if extra_headers:
            kwargs["extra_headers"] = extra_headers
        response = client.responses.create(**kwargs)
        if request.stream:
            yield from iter_model_stream_events(response)
            return
        data = _model_dump(response)
        yield from _scripted_stream_events(data)

    def _stream_via_http(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        """Fallback path used when the `openai` package is not installed.

        Sends the same payload as the SDK to POST {base_url}/responses, parsing
        SSE for stream=True and a single JSON body otherwise.
        """
        body_dict = request.to_responses_kwargs()
        body_dict = {k: v for k, v in body_dict.items() if v is not None}
        body = json.dumps(body_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream" if request.stream else "application/json",
        }
        headers.update(_responses_headers(request))
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if os.environ.get("OPENAI_ORGANIZATION"):
            headers["OpenAI-Organization"] = os.environ["OPENAI_ORGANIZATION"]
        if os.environ.get("OPENAI_PROJECT"):
            headers["OpenAI-Project"] = os.environ["OPENAI_PROJECT"]
        url = f"{self.base_url.rstrip('/')}/responses"
        http_request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            response = urllib.request.urlopen(http_request, timeout=600)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"responses request failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"responses request failed: {exc.reason}") from exc

        if not request.stream:
            with response:
                payload = json.loads(response.read().decode("utf-8"))
            yield from _scripted_stream_events(payload)
            return

        with response:
            yield from iter_model_stream_events(_iter_sse_events(response))

    def compact(
        self,
        request: PromptRequest,
        *,
        session_id: str | None = None,
        thread_id: str | None = None,
        installation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        payload = request.to_compact_payload()
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = self._compact_headers(
            session_id=session_id,
            thread_id=thread_id,
            installation_id=installation_id,
        )
        http_request = urllib.request.Request(
            self._compact_url(),
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(http_request, timeout=120) as response:
                response_body = response.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RemoteCompactionError(f"remote compact failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RemoteCompactionError(f"remote compact failed: {exc.reason}") from exc

        try:
            data = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RemoteCompactionError("remote compact returned invalid JSON") from exc
        output = data.get("output") if isinstance(data, dict) else None
        if not isinstance(output, list):
            raise RemoteCompactionError("remote compact response did not include an output list")
        return [_model_dump(item) for item in output]

    def _compact_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/responses/compact"

    def _compact_headers(
        self,
        *,
        session_id: str | None,
        thread_id: str | None,
        installation_id: str | None,
    ) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if os.environ.get("OPENAI_ORGANIZATION"):
            headers["OpenAI-Organization"] = os.environ["OPENAI_ORGANIZATION"]
        if os.environ.get("OPENAI_PROJECT"):
            headers["OpenAI-Project"] = os.environ["OPENAI_PROJECT"]
        if installation_id:
            headers["x-codex-installation-id"] = installation_id
        if session_id:
            headers["session_id"] = session_id
            headers["session-id"] = session_id
        if thread_id:
            headers["thread_id"] = thread_id
            headers["thread-id"] = thread_id
        return headers


class ChatGPTCodexModel:
    """Responses transport for official Codex ChatGPT account auth.

    Official Codex uses the same Responses request body, but when the active
    auth mode is ChatGPT it sends the request to the Codex backend at
    chatgpt.com/backend-api/codex with the persisted ChatGPT access token.
    """

    def __init__(
        self,
        *,
        auth_snapshot: CodexAuthSnapshot | None = None,
        auth_home: Path | str | None = None,
        base_url: str | None = None,
    ):
        self.auth_home = Path(auth_home).expanduser().resolve() if auth_home is not None else None
        self.auth_snapshot = auth_snapshot or self._load_auth()
        self.base_url = chatgpt_codex_base_url(base_url)
        self.auth_display_name = "ChatGPT"

    def create(self, request: PromptRequest) -> ModelResponse:
        return collect_model_stream_events(self.stream(request))

    def stream(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        yield from self._stream_via_http(request, allow_refresh=True)

    def compact(
        self,
        request: PromptRequest,
        *,
        session_id: str | None = None,
        thread_id: str | None = None,
        installation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        payload = request.to_compact_payload()
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = self._headers(
            request,
            accept="application/json",
            session_id=session_id,
            thread_id=thread_id,
            installation_id=installation_id,
        )
        response_body = self._urlopen_bytes(
            self._compact_url(),
            body,
            headers,
            timeout=120,
            allow_refresh=True,
            error_type=RemoteCompactionError,
        )
        try:
            data = json.loads(response_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RemoteCompactionError("remote compact returned invalid JSON") from exc
        output = data.get("output") if isinstance(data, dict) else None
        if not isinstance(output, list):
            raise RemoteCompactionError("remote compact response did not include an output list")
        return [_model_dump(item) for item in output]

    def _stream_via_http(self, request: PromptRequest, *, allow_refresh: bool) -> Iterable[ModelStreamEvent]:
        body_dict = request.to_responses_kwargs()
        body_dict = {k: v for k, v in body_dict.items() if v is not None}
        body = json.dumps(body_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = self._headers(
            request,
            accept="text/event-stream" if request.stream else "application/json",
        )
        http_request = urllib.request.Request(self._responses_url(), data=body, headers=headers, method="POST")
        try:
            response = urllib.request.urlopen(http_request, timeout=600)
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and allow_refresh and self._refresh_auth_for_retry():
                yield from self._stream_via_http(request, allow_refresh=False)
                return
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ChatGPT responses request failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"ChatGPT responses request failed: {exc.reason}") from exc

        if not request.stream:
            with response:
                payload = json.loads(response.read().decode("utf-8"))
            yield from _scripted_stream_events(payload)
            return

        with response:
            yield from iter_model_stream_events(_iter_sse_events(response))

    def _urlopen_bytes(
        self,
        url: str,
        body: bytes,
        headers: dict[str, str],
        *,
        timeout: float,
        allow_refresh: bool,
        error_type: type[Exception],
    ) -> bytes:
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 401 and allow_refresh and self._refresh_auth_for_retry():
                refreshed_headers = dict(headers)
                refreshed_headers["Authorization"] = f"Bearer {self.auth_snapshot.access_token}"
                return self._urlopen_bytes(
                    url,
                    body,
                    refreshed_headers,
                    timeout=timeout,
                    allow_refresh=False,
                    error_type=error_type,
                )
            detail = exc.read().decode("utf-8", errors="replace")
            raise error_type(f"ChatGPT compact request failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise error_type(f"ChatGPT compact request failed: {exc.reason}") from exc

    def _headers(
        self,
        request: PromptRequest,
        *,
        accept: str,
        session_id: str | None = None,
        thread_id: str | None = None,
        installation_id: str | None = None,
    ) -> dict[str, str]:
        self._refresh_stale_auth()
        headers = {
            "Content-Type": "application/json",
            "Accept": accept,
            "Authorization": f"Bearer {self.auth_snapshot.access_token}",
            "originator": OPENAI_ORIGINATOR,
            "User-Agent": f"{OPENAI_ORIGINATOR}/python-codex",
            "version": "python-codex",
        }
        if self.auth_snapshot.account_id:
            headers["ChatGPT-Account-ID"] = self.auth_snapshot.account_id
        headers.update(_responses_headers(request, session_id=session_id, thread_id=thread_id))
        if installation_id:
            headers["x-codex-installation-id"] = installation_id
        return headers

    def _load_auth(self) -> CodexAuthSnapshot:
        snapshot = load_auth_snapshot(self.auth_home, mode="chatgpt")
        if snapshot is None:
            raise RuntimeError("ChatGPT auth requested, but no ChatGPT auth snapshot was found")
        return snapshot

    def _refresh_stale_auth(self) -> None:
        if self.auth_snapshot.needs_proactive_refresh():
            try:
                self.auth_snapshot = refresh_chatgpt_auth(self.auth_snapshot)
            except RuntimeError:
                if self.auth_snapshot.access_token_expiration() is not None:
                    raise

    def _refresh_auth_for_retry(self) -> bool:
        try:
            self.auth_snapshot = refresh_chatgpt_auth(self.auth_snapshot)
            return True
        except RuntimeError:
            return False

    def _responses_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/responses"

    def _compact_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/responses/compact"


class FallbackModelClient:
    """Use ChatGPT account auth first, then API key auth for quota/budget failures.

    The fallback is intentionally limited to request-start failures where no
    model output has been yielded yet. That keeps the retry behavior legible:
    the same request is re-issued once through the API-key transport instead of
    mixing two partial model streams in a single turn.
    """

    def __init__(
        self,
        *,
        primary: ModelClient,
        fallback: ModelClient,
        primary_label: str = "ChatGPT",
        fallback_label: str = "API key",
    ) -> None:
        self.primary = primary
        self.fallback = fallback
        self.primary_label = primary_label
        self.fallback_label = fallback_label
        self._using_fallback = False
        self._last_fallback_reason: str | None = None

    @property
    def auth_display_name(self) -> str:
        return self.fallback_label if self._using_fallback else self.primary_label

    @property
    def auth_fallback_display_name(self) -> str | None:
        return None if self._using_fallback else self.fallback_label

    @property
    def using_fallback(self) -> bool:
        return self._using_fallback

    @property
    def last_fallback_reason(self) -> str | None:
        return self._last_fallback_reason

    def create(self, request: PromptRequest) -> ModelResponse:
        try:
            return self._active_client().create(request)
        except Exception as exc:
            if self._using_fallback or not _looks_like_account_limit_error(exc):
                raise
            self._switch_to_fallback(exc)
            return self.fallback.create(request)

    def stream(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        yielded = False
        try:
            for event in self._active_client().stream(request):
                yielded = True
                yield event
            return
        except Exception as exc:
            if self._using_fallback or yielded or not _looks_like_account_limit_error(exc):
                raise
            self._switch_to_fallback(exc)
        yield ModelStreamEvent(
            "warning",
            {"message": f"{self.primary_label} limit reached; switched to {self.fallback_label}."},
        )
        yield from self.fallback.stream(request)

    def compact(
        self,
        request: PromptRequest,
        *,
        session_id: str | None = None,
        thread_id: str | None = None,
        installation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        compact = getattr(self._active_client(), "compact", None)
        if not callable(compact):
            raise RemoteCompactionError("active model transport does not support remote compaction")
        try:
            return compact(
                request,
                session_id=session_id,
                thread_id=thread_id,
                installation_id=installation_id,
            )
        except Exception as exc:
            if self._using_fallback or not _looks_like_account_limit_error(exc):
                raise
            self._switch_to_fallback(exc)
        fallback_compact = getattr(self.fallback, "compact", None)
        if not callable(fallback_compact):
            raise RemoteCompactionError("fallback model transport does not support remote compaction")
        return fallback_compact(
            request,
            session_id=session_id,
            thread_id=thread_id,
            installation_id=installation_id,
        )

    def _active_client(self) -> ModelClient:
        return self.fallback if self._using_fallback else self.primary

    def _switch_to_fallback(self, exc: Exception) -> None:
        self._using_fallback = True
        self._last_fallback_reason = str(exc).strip() or type(exc).__name__


def _iter_sse_events(stream: Any) -> Iterable[dict[str, Any]]:
    """Parse an OpenAI Responses SSE stream into raw event dicts.

    Each event looks like:
        event: <name>
        data: <json>
        <blank line>
    The JSON payload already carries `type`, so the event name is informational.
    """
    data_lines: list[str] = []
    for raw_line in stream:
        line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, (bytes, bytearray)) else str(raw_line)
        line = line.rstrip("\r\n")
        if line == "":
            if data_lines:
                payload = "\n".join(data_lines)
                data_lines = []
                if payload == "[DONE]":
                    return
                try:
                    yield json.loads(payload)
                except json.JSONDecodeError:
                    continue
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
    if data_lines:
        payload = "\n".join(data_lines)
        if payload and payload != "[DONE]":
            try:
                yield json.loads(payload)
            except json.JSONDecodeError:
                pass


class ScriptedResponsesModel:
    """Deterministic model used by tests and CLI smoke runs."""

    def __init__(self, responses: Sequence[dict[str, Any]]):
        self.responses = list(responses)
        self.requests: list[PromptRequest] = []
        self.index = 0
        self._lock = threading.Lock()

    @classmethod
    def from_env(cls) -> "ScriptedResponsesModel | None":
        raw = os.environ.get("PY_CODEX_FAKE_RESPONSES")
        if not raw:
            return None
        return cls(json.loads(raw))

    def create(self, request: PromptRequest) -> ModelResponse:
        return collect_model_stream_events(self.stream(request))

    def stream(self, request: PromptRequest) -> Iterable[ModelStreamEvent]:
        response = self._next_response(request)
        if isinstance(response.get("events"), list):
            yield from iter_model_stream_events(response["events"])
        else:
            yield from _scripted_stream_events(response)

    def _next_response(self, request: PromptRequest) -> dict[str, Any]:
        with self._lock:
            self.requests.append(request)
            if self.index >= len(self.responses):
                raise RuntimeError("scripted model exhausted")
            response = self.responses[self.index]
            self.index += 1
        payload = dict(response)
        payload.setdefault("id", f"fake-{self.index}")
        return payload


def default_model_client(config: CodexConfig | None = None) -> ModelClient:
    scripted = ScriptedResponsesModel.from_env()
    if scripted is not None:
        return scripted
    auth_mode = normalize_auth_mode(
        getattr(config, "auth_mode", None) if config is not None else os.environ.get("PY_CODEX_AUTH_MODE")
    )
    auth_home = getattr(config, "auth_codex_home", None) if config is not None else None
    if auth_mode in {"auto", "chatgpt"}:
        try:
            snapshot = load_auth_snapshot(auth_home, mode=auth_mode)
        except RuntimeError:
            if auth_mode == "chatgpt":
                raise
            snapshot = None
        if snapshot is not None and snapshot.is_chatgpt:
            chatgpt_client = ChatGPTCodexModel(
                auth_snapshot=snapshot,
                auth_home=snapshot.auth_home,
                base_url=getattr(config, "chatgpt_base_url", None) if config is not None else None,
            )
            if auth_mode == "auto":
                api_key = _api_key_for_openai_transport(auth_home)
                if api_key:
                    return FallbackModelClient(
                        primary=chatgpt_client,
                        fallback=OpenAIResponsesModel(
                            api_key=api_key,
                            base_url=getattr(config, "openai_base_url", None) if config is not None else None,
                        ),
                    )
            return chatgpt_client
    return OpenAIResponsesModel(base_url=getattr(config, "openai_base_url", None) if config is not None else None)


def _api_key_for_openai_transport(auth_home: Path | str | None) -> str | None:
    try:
        snapshot = load_auth_snapshot(auth_home, mode="api_key")
    except RuntimeError:
        snapshot = None
    if snapshot is not None and snapshot.api_key:
        return snapshot.api_key
    return load_openai_api_key()


def _looks_like_account_limit_error(exc: Exception) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    markers = (
        "budget",
        "quota",
        "insufficient_quota",
        "usage limit",
        "rate_limit",
        "rate limit",
        "billing",
        "subscription",
        "limit reached",
        "exceeded",
    )
    return any(marker in text for marker in markers)


def _responses_headers(
    request: PromptRequest,
    *,
    session_id: str | None = None,
    thread_id: str | None = None,
) -> dict[str, str]:
    resolved_session_id = session_id or request.session_id
    resolved_thread_id = thread_id or request.thread_id or request.prompt_cache_key
    headers: dict[str, str] = {}
    if resolved_session_id:
        headers["session-id"] = resolved_session_id
    if resolved_thread_id:
        headers["thread-id"] = resolved_thread_id
        headers["x-client-request-id"] = resolved_thread_id
    client_metadata = request.client_metadata or {}
    installation_id = client_metadata.get("x-codex-installation-id")
    if installation_id:
        headers["x-codex-installation-id"] = installation_id
    return headers


def load_openai_api_key(env_file: Path = DEFAULT_OPENAI_ENV_FILE) -> str | None:
    env_key = os.environ.get("OPENAI_API_KEY")
    if env_key:
        return env_key

    values = load_env_file(env_file)
    key = values.get("OPENAI_API_KEY")
    if key:
        os.environ.setdefault("OPENAI_API_KEY", key)
    return key


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key:
            values[key] = value
    return values


def collect_stream_response(events: Iterable[Any]) -> ModelResponse:
    return collect_model_stream_events(iter_model_stream_events(events))


def model_response_to_stream_events(response: ModelResponse) -> Iterable[ModelStreamEvent]:
    payload: dict[str, Any] = {"id": response.id, "output": response.output}
    raw_response = response.raw.get("response") if isinstance(response.raw, dict) else None
    if isinstance(raw_response, dict) and "usage" in raw_response:
        payload["usage"] = raw_response["usage"]
    yield from _scripted_stream_events(payload)


def collect_model_stream_events(events: Iterable[ModelStreamEvent]) -> ModelResponse:
    raw_events: list[dict[str, Any]] = []
    output: list[dict[str, Any]] = []
    completed_response: dict[str, Any] | None = None
    response_id = ""

    for event in events:
        data = {"type": event.type, **event.payload}
        raw_events.append(data)
        if event.type == "item.completed" and isinstance(event.payload.get("item"), dict):
            output.append(event.payload["item"])
        elif event.type == "model.response":
            response_id = str(event.payload.get("response_id") or response_id)
            response = event.payload.get("response")
            if isinstance(response, dict):
                completed_response = response
        elif event.type == "model.failed":
            response = event.payload.get("response")
            if isinstance(response, dict):
                completed_response = response

    if completed_response is not None:
        completed_output = completed_response.get("output")
        if isinstance(completed_output, list):
            output = [_model_dump(item) for item in completed_output]
        response_id = str(completed_response.get("id") or response_id)

    return ModelResponse(id=response_id, output=output, raw={"events": raw_events, "response": completed_response})


def iter_model_stream_events(events: Iterable[Any]) -> Iterable[ModelStreamEvent]:
    for event in events:
        data = _model_dump(event)
        event_type = str(data.get("type") or "")
        if event_type == "response.output_item.added" and isinstance(data.get("item"), dict):
            yield ModelStreamEvent(
                "item.started",
                {
                    "item": data["item"],
                    "item_id": _event_item_id(data),
                    "output_index": data.get("output_index"),
                    "raw_type": event_type,
                },
            )
        elif event_type == "response.output_item.done" and isinstance(data.get("item"), dict):
            yield ModelStreamEvent(
                "item.completed",
                {
                    "item": data["item"],
                    "item_id": _event_item_id(data),
                    "output_index": data.get("output_index"),
                    "raw_type": event_type,
                },
            )
        elif event_type.endswith(".delta") and "delta" in data:
            yield ModelStreamEvent(
                "item.delta",
                {
                    "item_id": _event_item_id(data),
                    "output_index": data.get("output_index"),
                    "content_index": data.get("content_index"),
                    "delta": data.get("delta"),
                    "raw_type": event_type,
                },
            )
        elif event_type == "response.completed" and isinstance(data.get("response"), dict):
            response = data["response"]
            yield _token_count_event(response)
            yield ModelStreamEvent(
                "model.response",
                {
                    "response_id": str(response.get("id", "")),
                    "response": response,
                    "usage": response.get("usage"),
                    "raw_type": event_type,
                },
            )
        elif event_type in {"response.failed", "response.incomplete"}:
            response = data.get("response") if isinstance(data.get("response"), dict) else None
            if isinstance(response, dict) and isinstance(response.get("usage"), dict):
                yield _token_count_event(response)
            yield ModelStreamEvent(
                "model.failed",
                {
                    "response_id": str(response.get("id", "")) if isinstance(response, dict) else "",
                    "response": response,
                    "error": data.get("error"),
                    "raw_type": event_type,
                },
            )


def _scripted_stream_events(response: dict[str, Any]) -> Iterable[ModelStreamEvent]:
    response_id = str(response.get("id", ""))
    output = [_model_dump(item) for item in response.get("output", [])]
    for index, item in enumerate(output):
        item_id = str(item.get("id") or item.get("call_id") or f"item-{index}")
        yield ModelStreamEvent("item.started", {"item": item, "item_id": item_id, "output_index": index})
        for delta in _scripted_item_deltas(item):
            yield ModelStreamEvent("item.delta", {"item_id": item_id, "output_index": index, **delta})
        yield ModelStreamEvent("item.completed", {"item": item, "item_id": item_id, "output_index": index})
    completed_response = {"id": response_id, "output": output}
    if "usage" in response:
        completed_response["usage"] = response["usage"]
        yield _token_count_event(completed_response)
    yield ModelStreamEvent(
        "model.response",
        {
            "response_id": response_id,
            "response": completed_response,
            "usage": completed_response.get("usage"),
        },
    )


def _scripted_item_deltas(item: dict[str, Any]) -> Iterable[dict[str, Any]]:
    if item.get("type") == "message":
        for content_index, part in enumerate(item.get("content", [])):
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                yield {"content_index": content_index, "delta": part["text"], "raw_type": "response.output_text.delta"}
    elif item.get("type") == "reasoning":
        for key in ("summary", "content"):
            value = item.get(key)
            if isinstance(value, str):
                yield {"delta": value, "raw_type": "response.reasoning_summary_text.delta"}
            elif isinstance(value, list):
                for part in value:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        yield {"delta": part["text"], "raw_type": "response.reasoning_summary_text.delta"}
                    elif isinstance(part, str):
                        yield {"delta": part, "raw_type": "response.reasoning_summary_text.delta"}
    elif item.get("type") == "function_call" and isinstance(item.get("arguments"), str):
        yield {"delta": item["arguments"], "raw_type": "response.function_call_arguments.delta"}
    elif item.get("type") == "custom_tool_call" and isinstance(item.get("input"), str):
        yield {"delta": item["input"], "raw_type": "response.custom_tool_call_input.delta"}


def _token_count_event(response: dict[str, Any]) -> ModelStreamEvent:
    usage = response.get("usage")
    return ModelStreamEvent(
        "token_count",
        {"usage": usage if isinstance(usage, dict) else None},
    )


def _event_item_id(data: dict[str, Any]) -> str:
    item = data.get("item")
    if isinstance(item, dict):
        value = item.get("id") or item.get("call_id")
        if value is not None:
            return str(value)
    for key in ("item_id", "id"):
        if data.get(key) is not None:
            return str(data[key])
    output_index = data.get("output_index")
    return f"item-{output_index}" if output_index is not None else ""


def _model_dump(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "dict"):
        return value.dict()
    raise TypeError(f"cannot convert response object to dict: {type(value)!r}")
