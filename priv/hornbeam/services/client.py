import math
import secrets
from collections.abc import Mapping

from hornbeam._shared.validation import byte_payload, text
from hornbeam.api.errors import RuntimeProtocolError, RuntimeUnavailable
from hornbeam.services._wire import integer, result, scope_owner
from hornbeam.services.store import ByteStore
from hornbeam.services.subscription import Subscription


class Beam:
    """An explicitly owned handle to Hornbeam services, never a memory fallback."""

    def __init__(self, *, owner: object, cleanup_timeout: float = 5.0) -> None:
        if owner is None:
            raise ValueError("an explicit runtime owner is required")
        if not math.isfinite(cleanup_timeout) or cleanup_timeout <= 0:
            raise ValueError("cleanup_timeout must be finite and positive")
        self._owner = owner
        self._cleanup_timeout = cleanup_timeout

    @classmethod
    async def connect(cls) -> Beam:
        try:
            from erlang import async_call
        except ImportError as exc:
            raise RuntimeUnavailable("Beam.connect() requires the Hornbeam embedding") from exc
        owner = result(await async_call("hornbeam_services", "owner", []))
        if owner is None:
            raise RuntimeProtocolError("runtime returned no owner")
        return cls(owner=owner)

    def for_scope(self, scope: Mapping[str, object]) -> Beam:
        return Beam(owner=scope_owner(scope), cleanup_timeout=self._cleanup_timeout)

    def store(self, namespace: str) -> ByteStore:
        return ByteStore(namespace)

    async def incr(self, key: str, delta: int = 1, *, namespace: str = "default") -> int:
        from erlang import async_call

        if type(delta) is not int:
            raise TypeError("delta must be an integer")
        args: list[object] = [
            text(namespace, name="namespace", limit=256),
            text(key, name="key", limit=1024),
            delta,
        ]
        return integer(result(await async_call("hornbeam_services", "counter_incr", args)))

    async def publish(self, topic: str, payload: bytes) -> int:
        from erlang import async_call

        args: list[object] = [
            text(topic, name="topic", limit=256, nonempty=True),
            byte_payload(payload, limit=65536),
        ]
        return integer(result(await async_call("hornbeam_services", "publish", args)), minimum=0)

    def subscribe(self, topic: str, *, max_bytes: int = 65536) -> Subscription:
        encoded = text(topic, name="topic", limit=256, nonempty=True)
        if type(max_bytes) is not int:
            raise TypeError("max_bytes must be an integer")
        if not 1024 <= max_bytes <= 1048576:
            raise ValueError("max_bytes must be between 1024 and 1048576")
        token = secrets.token_bytes(16)
        return Subscription(self._owner, token, self._cleanup_timeout, encoded, max_bytes)

    async def subscription_count(self) -> int:
        from erlang import async_call

        return integer(
            result(await async_call("hornbeam_services", "subscription_count", [])), minimum=0
        )
