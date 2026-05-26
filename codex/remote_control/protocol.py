from __future__ import annotations

import base64
import json
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .constants import (
    REMOTE_CONTROL_REASSEMBLED_MAX_BYTES,
    REMOTE_CONTROL_SEGMENT_ASSEMBLY_MAX_COUNT,
    REMOTE_CONTROL_SEGMENT_COUNT_MAX,
    REMOTE_CONTROL_SEGMENT_MAX_BYTES,
    REMOTE_CONTROL_SEGMENT_TARGET_BYTES,
)
from .types import RemoteControlError
from .utils import _ceil_div, _optional_int, _optional_string

class RemoteClientEvent(str, Enum):
    CLIENT_MESSAGE = "client_message"
    CLIENT_MESSAGE_CHUNK = "client_message_chunk"
    ACK = "ack"
    PING = "ping"
    CLIENT_CLOSED = "client_closed"


class RemoteServerEvent(str, Enum):
    SERVER_MESSAGE = "server_message"
    SERVER_MESSAGE_CHUNK = "server_message_chunk"
    ACK = "ack"
    PONG = "pong"


@dataclass
class _ClientSegmentAssembly:
    stream_id: str
    seq_id: int
    segment_count: int
    message_size_bytes: int
    raw: bytearray = field(default_factory=bytearray)
    next_segment_id: int = 0
    last_chunk_seen_at: float = field(default_factory=time.monotonic)


class _ClientSegmentReassembler:
    def __init__(self) -> None:
        self._assemblies: dict[str, _ClientSegmentAssembly] = {}

    def observe(self, envelope: "ClientEnvelope") -> "ClientEnvelope | None":
        if envelope.event.get("type") != RemoteClientEvent.CLIENT_MESSAGE_CHUNK.value:
            return envelope
        segment_id = _optional_int(envelope.event.get("segment_id"))
        segment_count = _optional_int(envelope.event.get("segment_count"))
        message_size_bytes = _optional_int(envelope.event.get("message_size_bytes"))
        chunk_base64 = _optional_string(envelope.event.get("message_chunk_base64"))
        if (
            envelope.seq_id is None
            or envelope.stream_id is None
            or segment_id is None
            or segment_count is None
            or message_size_bytes is None
            or not chunk_base64
        ):
            return None
        if self.should_ignore_chunk(envelope.client_id, envelope.stream_id, envelope.seq_id, segment_id):
            return None
        if (
            segment_count <= 0
            or segment_count > REMOTE_CONTROL_SEGMENT_COUNT_MAX
            or segment_id >= segment_count
            or message_size_bytes <= 0
            or message_size_bytes > REMOTE_CONTROL_REASSEMBLED_MAX_BYTES
        ):
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None

        assembly = self._assemblies.get(envelope.client_id)
        if assembly is None or assembly.stream_id != envelope.stream_id:
            self._evict_assemblies_if_full()
            assembly = _ClientSegmentAssembly(
                stream_id=envelope.stream_id,
                seq_id=envelope.seq_id,
                segment_count=segment_count,
                message_size_bytes=message_size_bytes,
            )
            self._assemblies[envelope.client_id] = assembly
        elif (
            assembly.seq_id != envelope.seq_id
            or assembly.segment_count != segment_count
            or assembly.message_size_bytes != message_size_bytes
        ):
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None

        if segment_id < assembly.next_segment_id:
            return None
        if segment_id != assembly.next_segment_id:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        try:
            chunk = base64.b64decode(chunk_base64.encode("ascii"), validate=True)
        except Exception:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        if len(assembly.raw) + len(chunk) > message_size_bytes:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        assembly.raw.extend(chunk)
        assembly.next_segment_id += 1
        assembly.last_chunk_seen_at = time.monotonic()
        if assembly.next_segment_id < segment_count:
            return None
        if len(assembly.raw) != message_size_bytes:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        try:
            message = json.loads(bytes(assembly.raw).decode("utf-8"))
        except Exception:
            self.invalidate_stream(envelope.client_id, envelope.stream_id)
            return None
        self.invalidate_stream(envelope.client_id, envelope.stream_id)
        if not isinstance(message, dict):
            return None
        return ClientEnvelope(
            client_id=envelope.client_id,
            stream_id=envelope.stream_id,
            seq_id=envelope.seq_id,
            cursor=envelope.cursor,
            event={"type": RemoteClientEvent.CLIENT_MESSAGE.value, "message": message},
        )

    def should_ignore_chunk(self, client_id: str, stream_id: str, seq_id: int, segment_id: int) -> bool:
        assembly = self._assemblies.get(client_id)
        return bool(
            assembly
            and assembly.stream_id == stream_id
            and (seq_id < assembly.seq_id or (seq_id == assembly.seq_id and segment_id < assembly.next_segment_id))
        )

    def invalidate_stream(self, client_id: str, stream_id: str) -> None:
        assembly = self._assemblies.get(client_id)
        if assembly and assembly.stream_id == stream_id:
            self._assemblies.pop(client_id, None)

    def invalidate_client(self, client_id: str) -> None:
        self._assemblies.pop(client_id, None)

    def _evict_assemblies_if_full(self) -> None:
        while len(self._assemblies) >= REMOTE_CONTROL_SEGMENT_ASSEMBLY_MAX_COUNT:
            oldest = min(self._assemblies.items(), key=lambda item: item[1].last_chunk_seen_at, default=None)
            if oldest is None:
                return
            self._assemblies.pop(oldest[0], None)


class _OutboundBuffer:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._buffer_by_stream: dict[tuple[str, str], list[ServerEnvelope]] = {}

    def insert(self, envelope: "ServerEnvelope") -> None:
        with self._lock:
            self._buffer_by_stream.setdefault((envelope.client_id, envelope.stream_id), []).append(envelope)

    def ack(self, client_id: str, stream_id: str, acked_seq_id: int, acked_segment_id: int | None) -> None:
        acked_cursor = (acked_seq_id, acked_segment_id if acked_segment_id is not None else sys.maxsize)
        key = (client_id, stream_id)
        with self._lock:
            buffer = self._buffer_by_stream.get(key)
            if not buffer:
                return
            remaining = [
                envelope
                for envelope in buffer
                if (envelope.seq_id, _server_envelope_segment_id(envelope)) > acked_cursor
            ]
            if remaining:
                self._buffer_by_stream[key] = remaining
            else:
                self._buffer_by_stream.pop(key, None)

    def remove_stream(self, client_id: str, stream_id: str) -> None:
        with self._lock:
            self._buffer_by_stream.pop((client_id, stream_id), None)

    def remove_client(self, client_id: str) -> None:
        with self._lock:
            for key in list(self._buffer_by_stream):
                if key[0] == client_id:
                    self._buffer_by_stream.pop(key, None)

    def server_envelopes(self) -> list["ServerEnvelope"]:
        with self._lock:
            return [envelope for buffer in self._buffer_by_stream.values() for envelope in buffer]


@dataclass(frozen=True)
class ClientEnvelope:
    client_id: str
    event: dict[str, Any]
    stream_id: str | None = None
    seq_id: int | None = None
    cursor: str | None = None

    @classmethod
    def from_wire(cls, payload: dict[str, Any]) -> "ClientEnvelope":
        if not isinstance(payload, dict):
            raise RemoteControlError("remote-control client envelope must be a JSON object")
        client_id = payload.get("client_id")
        if not isinstance(client_id, str) or not client_id:
            raise RemoteControlError("remote-control client envelope is missing client_id")
        event_type = payload.get("type")
        if not isinstance(event_type, str):
            raise RemoteControlError("remote-control client envelope is missing type")
        event = {"type": event_type}
        for key, value in payload.items():
            if key not in {"client_id", "stream_id", "seq_id", "cursor", "type"}:
                event[key] = value
        return cls(
            client_id=client_id,
            stream_id=_optional_string(payload.get("stream_id")),
            seq_id=_optional_int(payload.get("seq_id")),
            cursor=_optional_string(payload.get("cursor")),
            event=event,
        )

    def to_wire(self) -> dict[str, Any]:
        event = dict(self.event)
        payload: dict[str, Any] = {
            "client_id": self.client_id,
            "type": event.pop("type"),
            **event,
        }
        if self.stream_id is not None:
            payload["stream_id"] = self.stream_id
        if self.seq_id is not None:
            payload["seq_id"] = self.seq_id
        if self.cursor is not None:
            payload["cursor"] = self.cursor
        return payload


@dataclass(frozen=True)
class ServerEnvelope:
    client_id: str
    stream_id: str
    seq_id: int
    event: dict[str, Any]

    def to_wire(self) -> dict[str, Any]:
        event = dict(self.event)
        return {
            "client_id": self.client_id,
            "stream_id": self.stream_id,
            "seq_id": self.seq_id,
            "type": event.pop("type"),
            **event,
        }


def _remote_control_message_starts_connection(envelope: ClientEnvelope) -> bool:
    if envelope.event.get("type") != RemoteClientEvent.CLIENT_MESSAGE.value:
        return False
    message = envelope.event.get("message")
    return isinstance(message, dict) and message.get("method") == "initialize"




class _DeferredResponse:
    pass


_DEFERRED_RESPONSE = _DeferredResponse()


def _split_server_envelope_for_transport(envelope: ServerEnvelope) -> list[ServerEnvelope]:
    if envelope.event.get("type") != RemoteServerEvent.SERVER_MESSAGE.value:
        return [envelope]
    if len(_serialized_envelope_bytes(envelope)) <= REMOTE_CONTROL_SEGMENT_MAX_BYTES:
        return [envelope]
    message = envelope.event.get("message")
    raw = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    message_size_bytes = len(raw)
    if message_size_bytes > REMOTE_CONTROL_REASSEMBLED_MAX_BYTES:
        return []
    minimal_segment_count = min(max(1, message_size_bytes), REMOTE_CONTROL_SEGMENT_COUNT_MAX)
    if (
        len(_serialized_envelope_bytes(_server_message_chunk_envelope(envelope, 0, minimal_segment_count, message_size_bytes, raw[:1])))
        > REMOTE_CONTROL_SEGMENT_MAX_BYTES
    ):
        return []
    segment_count = max(2, _ceil_div(message_size_bytes, REMOTE_CONTROL_SEGMENT_TARGET_BYTES))
    while True:
        chunk_size = max(1, _ceil_div(message_size_bytes, segment_count))
        segment_count = _ceil_div(message_size_bytes, chunk_size)
        chunks = list(raw[i : i + chunk_size] for i in range(0, message_size_bytes, chunk_size))
        if len(chunks) <= REMOTE_CONTROL_SEGMENT_COUNT_MAX:
            envelopes = [
                _server_message_chunk_envelope(envelope, segment_id, len(chunks), message_size_bytes, chunk)
                for segment_id, chunk in enumerate(chunks)
            ]
            if all(len(_serialized_envelope_bytes(item)) <= REMOTE_CONTROL_SEGMENT_MAX_BYTES for item in envelopes):
                return envelopes
        if chunk_size == 1 or len(chunks) >= REMOTE_CONTROL_SEGMENT_COUNT_MAX:
            return []
        next_segment_count = segment_count + 1
        next_chunk_size = max(1, _ceil_div(message_size_bytes, next_segment_count))
        segment_count = message_size_bytes if next_chunk_size == chunk_size else next_segment_count


def _server_message_chunk_envelope(
    envelope: ServerEnvelope,
    segment_id: int,
    segment_count: int,
    message_size_bytes: int,
    chunk: bytes,
) -> ServerEnvelope:
    return ServerEnvelope(
        client_id=envelope.client_id,
        stream_id=envelope.stream_id,
        seq_id=envelope.seq_id,
        event={
            "type": RemoteServerEvent.SERVER_MESSAGE_CHUNK.value,
            "segment_id": segment_id,
            "segment_count": segment_count,
            "message_size_bytes": message_size_bytes,
            "message_chunk_base64": base64.b64encode(chunk).decode("ascii"),
        },
    )


def _serialized_envelope_bytes(envelope: ServerEnvelope) -> bytes:
    return json.dumps(envelope.to_wire(), ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _server_envelope_segment_id(envelope: ServerEnvelope) -> int:
    return _optional_int(envelope.event.get("segment_id")) or 0
