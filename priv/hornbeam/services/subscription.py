import asyncio
from types import TracebackType
from typing import TYPE_CHECKING, NoReturn, Self

from hornbeam._shared.lifecycle import await_cleanup
from hornbeam.api.errors import (
    RuntimeProtocolError,
    SubscriptionClosed,
    SubscriptionOverflow,
)
from hornbeam.services._wire import (
    Status,
    acknowledged,
    event,
    result,
    status,
)

if TYPE_CHECKING:
    from erlang import Channel


class Subscription:
    """One owned native channel with at most one pending receive."""

    def __init__(
        self,
        owner: object,
        token: bytes,
        timeout: float,
        topic: bytes,
        max_bytes: int,
    ) -> None:
        self._topic = topic
        self._max_bytes = max_bytes
        self._entered = False
        self._owner = owner
        self._token = token
        self._timeout = timeout
        self._channel: Channel | None = None
        self._pending: asyncio.Task[bytes] | None = None
        self._receiving = False
        self._closed = False
        self._terminal: Status = "open"
        self._cleanup: asyncio.Task[None] | None = None
        self._retirement: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        from erlang import Channel, async_call

        if self._entered or self._closed:
            raise SubscriptionClosed("subscription context cannot be reused")
        self._entered = True
        try:
            resource = result(
                await async_call(
                    "hornbeam_services",
                    "subscribe",
                    [self._owner, self._token, self._topic, self._max_bytes],
                )
            )
            self._channel = Channel(resource)
            if self._closed:
                await self.aclose()
                await self._retire()
                self._raise_closed()
            return self
        except BaseException:
            await self.aclose()
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _raise_closed(self) -> NoReturn:
        if self._terminal == "overflow":
            raise SubscriptionOverflow("subscription pending data budget exceeded")
        if self._terminal == "invalid_payload":
            raise RuntimeProtocolError("subscription received an invalid payload")
        raise SubscriptionClosed("subscription is closed")

    async def receive(self) -> bytes:
        if self._receiving:
            raise RuntimeError("only one pending receive is permitted per subscription")
        if self._closed:
            self._raise_closed()
        channel = self._channel
        if channel is None:
            raise SubscriptionClosed("subscription has not opened")
        self._receiving = True
        try:
            pending = asyncio.create_task(self._receive(channel))
            self._pending = pending
            try:
                return await pending
            except asyncio.CancelledError:
                if self._closed:
                    self._raise_closed()
                raise
        finally:
            self._pending = None
            self._receiving = False

    async def _receive(self, channel: Channel) -> bytes:
        from erlang import ChannelClosed, async_call

        try:
            raw = await channel.async_receive(native_only=True)
        except ChannelClosed:
            if self._closed:
                self._raise_closed()
            self._terminal = status(
                await async_call(
                    "hornbeam_services", "subscription_status", [self._owner, self._token]
                )
            )
            if self._terminal == "open":
                raise RuntimeProtocolError(
                    "native channel closed while runtime reports open"
                ) from None
            self._closed = True
            self._raise_closed()
        if self._closed:
            self._raise_closed()
        return event(raw)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive()
        except SubscriptionClosed:
            raise StopAsyncIteration from None

    async def _finalize(self) -> None:
        from erlang import async_call

        try:
            async with asyncio.timeout(self._timeout):
                terminal = status(
                    await async_call(
                        "hornbeam_services", "subscription_finalize", [self._owner, self._token]
                    )
                )
                if terminal == "open":
                    raise RuntimeProtocolError("retired subscription still reports open")
                if self._terminal == "open":
                    self._terminal = terminal
        except TimeoutError as exc:
            raise RuntimeProtocolError("subscription retirement acknowledgement timed out") from exc

    async def _retire(self) -> None:
        if self._retirement is None:
            self._retirement = asyncio.create_task(self._finalize())
        await await_cleanup(self._retirement)

    async def _close(self) -> None:
        from erlang import async_call

        try:
            async with asyncio.timeout(self._timeout):
                pending = self._pending
                if pending is not None:
                    pending.cancel()
                try:
                    acknowledged(
                        await async_call(
                            "hornbeam_services", "unsubscribe", [self._owner, self._token]
                        )
                    )
                finally:
                    if pending is not None:
                        await asyncio.gather(pending, return_exceptions=True)
                # A canceled open has no acknowledged channel. Keep its native
                # tombstone: its subscribe may still arrive after unsubscribe.
                if self._channel is not None:
                    await self._retire()
        except TimeoutError as exc:
            raise RuntimeProtocolError("subscription cleanup acknowledgement timed out") from exc

    async def aclose(self) -> None:
        """Close once; all callers await the same bounded acknowledgement."""
        self._closed = True
        if self._cleanup is None:
            self._cleanup = asyncio.create_task(self._close())
        await await_cleanup(self._cleanup)
