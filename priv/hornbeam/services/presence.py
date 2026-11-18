from dataclasses import dataclass
from typing import TypeIs

from hornbeam.api.errors import RuntimeProtocolError
from hornbeam.services._wire import _pair, _tuple, byte_value
from hornbeam.services.subscription import _OwnedSubscription


@dataclass(frozen=True)
class PresenceEntry:
    key: str
    ref: str
    payload: bytes


def _list(value: object) -> TypeIs[list[object]]:
    return isinstance(value, list)


def _text(value: object) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as error:
            raise RuntimeProtocolError("invalid presence text") from error
    if not isinstance(value, str) or not value:
        raise RuntimeProtocolError("expected nonempty presence text")
    return value


def presence_event(raw: object) -> tuple[PresenceEntry, ...]:
    if not _pair(raw) or raw[0] not in (b"presence", "presence") or not _list(raw[1]):
        raise RuntimeProtocolError("expected a presence snapshot envelope")
    entries: list[PresenceEntry] = []
    for row in raw[1]:
        if not _tuple(row) or len(row) != 3:
            raise RuntimeProtocolError("expected a three-item presence entry")
        payload = byte_value(row[2])
        if len(payload) > 65536:
            raise RuntimeProtocolError("presence payload exceeds 65536 bytes")
        entries.append(PresenceEntry(_text(row[0]), _text(row[1]), payload))
    return tuple(entries)


class PresenceSubscription(_OwnedSubscription[tuple[PresenceEntry, ...]]):
    """An owned presence entry and bounded stream of full snapshots.

    Snapshots are eventually consistent across nodes, not durable history.
    Closing removes this entry before acknowledging cleanup. Intermediate
    changes may coalesce; a full delivery budget raises SubscriptionOverflow.
    """

    def __init__(
        self,
        owner: object,
        token: bytes,
        timeout: float,
        topic: bytes,
        max_bytes: int,
        key: bytes,
        payload: bytes,
    ) -> None:
        super().__init__(owner, token, timeout, topic, max_bytes)
        self._key = key
        self._payload = payload

    def _open_call(self) -> tuple[str, list[object]]:
        return "presence", [
            self._owner,
            self._token,
            self._topic,
            self._key,
            self._payload,
            self._max_bytes,
        ]

    def _decode(self, raw: object) -> tuple[PresenceEntry, ...]:
        return presence_event(raw)
