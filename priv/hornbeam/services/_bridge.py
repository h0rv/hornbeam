from collections.abc import Mapping
from typing import Literal, Protocol, TypeIs

from hornbeam.api.errors import RuntimeProtocolError, RuntimeUnavailable


class NativeChannel(Protocol):
    async def async_receive(self, *, native_only: bool = False) -> object: ...


type ServiceAction = Literal[
    "owner",
    "cache_set",
    "cache_get",
    "cache_delete",
    "cache_clear",
    "cache_exists",
    "cache_ttl",
    "counter_incr",
    "publish",
    "subscribe",
    "unsubscribe",
    "subscription_status",
    "subscription_count",
]


class Bridge(Protocol):
    async def call(self, action: ServiceAction, args: list[object]) -> object: ...
    def channel(self, resource: object) -> NativeChannel: ...


class NativeBridge:
    def __init__(self) -> None:
        try:
            import erlang
        except ImportError as exc:
            raise RuntimeUnavailable("Beam.connect() requires the Hornbeam embedding") from exc
        self._erlang = erlang

    async def call(self, action: ServiceAction, args: list[object]) -> object:
        try:
            return await self._erlang.async_call("hornbeam_services", action, args)
        except (ImportError, RuntimeError, AttributeError) as exc:
            raise RuntimeUnavailable("Hornbeam callback failed: {}".format(action)) from exc

    def channel(self, resource: object) -> NativeChannel:
        return self._erlang.Channel(resource)


def _tuple(value: object) -> TypeIs[tuple[object, ...]]:
    return isinstance(value, tuple)


def _pair(value: object) -> TypeIs[tuple[object, object]]:
    return _tuple(value) and len(value) == 2


def _mapping(value: object) -> TypeIs[Mapping[object, object]]:
    return isinstance(value, Mapping)


def scope_owner(scope: Mapping[str, object]) -> object:
    extensions = scope.get("extensions")
    if not _mapping(extensions):
        raise RuntimeProtocolError("scope has no Hornbeam extensions mapping")
    owner = extensions.get("hornbeam.owner")
    if owner is None:
        raise RuntimeProtocolError("scope has no hornbeam.owner")
    return owner


class Missing:
    pass


MISSING = Missing()


def result(raw: object, *, missing: bool = False) -> object:
    if not _pair(raw):
        raise RuntimeProtocolError("expected a two-item runtime response tuple")
    tag, value = raw
    if not isinstance(tag, (bytes, str)):
        raise RuntimeProtocolError("runtime response tag must be bytes or text")
    if tag == b"ok" or tag == "ok":
        return value
    if tag == b"missing" or tag == "missing":
        if missing and (value == b"" or value == ""):
            return MISSING
        raise RuntimeProtocolError("unexpected or malformed missing response")
    if tag == b"error" or tag == "error":
        if not isinstance(value, (bytes, str)):
            raise RuntimeProtocolError("runtime error reason must be bytes or text")
        if value == b"unavailable" or value == "unavailable":
            raise RuntimeUnavailable("Hornbeam services are unavailable")
        raise RuntimeProtocolError("Hornbeam rejected call: {!r}".format(value))
    raise RuntimeProtocolError("unknown runtime response tag")


def integer(value: object, *, minimum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RuntimeProtocolError("expected an integer")
    if minimum is not None and value < minimum:
        raise RuntimeProtocolError("integer below permitted minimum")
    return value


def boolean(value: object) -> bool:
    if not isinstance(value, bool):
        raise RuntimeProtocolError("expected a boolean")
    return value


def acknowledged(raw: object) -> None:
    if boolean(result(raw)) is not True:
        raise RuntimeProtocolError("operation was not acknowledged")


def byte_value(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise RuntimeProtocolError("expected forced bytes")
    return value


def event(raw: object) -> bytes | None:
    if raw is None:
        return None
    if not _pair(raw) or raw[0] not in (b"event", "event"):
        raise RuntimeProtocolError("expected an event envelope or channel closure")
    payload = byte_value(raw[1])
    if len(payload) > 65536:
        raise RuntimeProtocolError("event payload exceeds 65536 bytes")
    return payload


type Status = Literal["open", "closed", "overflow", "owner_down", "invalid_payload"]


def status(raw: object) -> Status:
    value = result(raw)
    if value == b"open" or value == "open":
        return "open"
    if value == b"closed" or value == "closed":
        return "closed"
    if value == b"overflow" or value == "overflow":
        return "overflow"
    if value == b"owner_down" or value == "owner_down":
        return "owner_down"
    if value == b"invalid_payload" or value == "invalid_payload":
        return "invalid_payload"
    raise RuntimeProtocolError("unknown subscription status")
