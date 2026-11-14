from hornbeam._shared.validation import byte_payload, milliseconds, text
from hornbeam.api.errors import RuntimeProtocolError
from hornbeam.services._wire import (
    MISSING,
    acknowledged,
    boolean,
    byte_value,
    integer,
    result,
)


class ByteStore:
    """A namespace-scoped byte cache. None means persistent or missing TTL."""

    def __init__(self, namespace: str) -> None:
        self._namespace = text(namespace, name="namespace", limit=256)

    def _key(self, key: str) -> list[object]:
        return [self._namespace, text(key, name="key", limit=1024)]

    async def set(self, key: str, value: bytes, *, expires_in: float | None = None) -> None:
        from erlang import async_call

        args = [*self._key(key), byte_payload(value, limit=1048576), milliseconds(expires_in)]
        acknowledged(await async_call("hornbeam_services", "cache_set", args))

    async def get(self, key: str, *, renew_for: float | None = None) -> bytes | None:
        from erlang import async_call

        value = result(
            await async_call(
                "hornbeam_services", "cache_get", [*self._key(key), milliseconds(renew_for)]
            ),
            missing=True,
        )
        return None if value is MISSING else byte_value(value)

    async def delete(self, key: str) -> None:
        from erlang import async_call

        acknowledged(await async_call("hornbeam_services", "cache_delete", self._key(key)))

    async def delete_all(self) -> None:
        from erlang import async_call

        acknowledged(await async_call("hornbeam_services", "cache_clear", [self._namespace]))

    async def exists(self, key: str) -> bool:
        from erlang import async_call

        return boolean(
            result(await async_call("hornbeam_services", "cache_exists", self._key(key)))
        )

    async def expires_in(self, key: str) -> float | None:
        from erlang import async_call

        value = result(
            await async_call("hornbeam_services", "cache_ttl", self._key(key)), missing=True
        )
        if value is MISSING:
            return None
        ttl = integer(value, minimum=-1)
        if ttl == -1:
            return None
        try:
            return ttl / 1000
        except OverflowError as exc:
            raise RuntimeProtocolError("runtime TTL cannot be represented in seconds") from exc
