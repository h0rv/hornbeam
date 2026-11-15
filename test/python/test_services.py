import asyncio
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType

import pytest
from hornbeam.api.errors import (
    RuntimeProtocolError,
    RuntimeUnavailable,
    SubscriptionClosed,
    SubscriptionOverflow,
)
from hornbeam.services._wire import byte_value, integer
from hornbeam.services.client import Beam as NativeBeam
from hornbeam.services.subscription import Subscription
from hypothesis import given
from hypothesis import strategies as st


@pytest.fixture(autouse=True)
def restore_native_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "erlang", ModuleType("erlang"))
    monkeypatch.delitem(sys.modules, "erlang")


def Beam(native: TestDoubleBridge, *, owner: object, cleanup_timeout: float = 5.0) -> NativeBeam:
    sys.modules["erlang"] = native
    return NativeBeam(owner=owner, cleanup_timeout=cleanup_timeout)


def wire_milliseconds(seconds: float | None) -> int:
    async def run() -> int:
        bridge = TestDoubleBridge()
        await Beam(bridge, owner="o").store("n").set("k", b"v", expires_in=seconds)
        return integer(bridge.calls[-1][1][3])

    return asyncio.run(run())


class TestDoubleChannelClosed(Exception):
    __test__ = False


class TestDoubleChannel:
    __test__ = False

    def __init__(self) -> None:
        self.queue: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
        self.entered = asyncio.Event()
        self.native_only: list[bool] = []

    async def async_receive(self, *, native_only: bool = False) -> object:
        self.native_only.append(native_only)
        self.entered.set()
        value = await self.queue.get()
        if isinstance(value, TestDoubleChannelClosed):
            raise value
        return value

    def close(self) -> None:
        self.queue.put_nowait(TestDoubleChannelClosed("Channel closed"))


class TestDoubleBridge(ModuleType):
    __test__ = False
    ChannelClosed = TestDoubleChannelClosed

    def __init__(self) -> None:
        super().__init__("erlang")
        self.calls: list[tuple[str, list[object]]] = []
        self.channels: dict[bytes, TestDoubleChannel] = {}
        self.states: dict[bytes, bytes] = {}
        self.topics: dict[bytes, bytes] = {}
        self.values: dict[tuple[bytes, bytes], bytes] = {}
        self.ttls: dict[tuple[bytes, bytes], int] = {}
        self.counters: dict[tuple[bytes, bytes], int] = {}
        self.open_started = asyncio.Event()
        self.close_started = asyncio.Event()
        self.status_started = asyncio.Event()
        self.open_gate = asyncio.Event()
        self.close_gate = asyncio.Event()
        self.status_gate = asyncio.Event()
        self.open_gate.set()
        self.close_gate.set()
        self.status_gate.set()
        self.fail_close = False
        self.override: dict[str, object] = {}

    def Channel(self, resource: object) -> TestDoubleChannel:
        if not isinstance(resource, TestDoubleChannel):
            raise RuntimeProtocolError("test double expected its channel resource")
        return resource

    async def async_call(self, module: str, action: str, args: list[object]) -> object:
        assert module == "hornbeam_services"
        return await self.call(action, args)

    async def call(self, action: str, args: list[object]) -> object:
        self.calls.append((action, args.copy()))
        if action in self.override:
            return self.override[action]
        if action == "owner":
            return (b"ok", "application-owner")
        if action == "subscribe":
            token = byte_value(args[1])
            self.open_started.set()
            await self.open_gate.wait()
            channel = TestDoubleChannel()
            if self.states.get(token) == b"closed":
                channel.close()
            else:
                self.states[token] = b"open"
            self.channels[token] = channel
            self.topics[token] = byte_value(args[2])
            return (b"ok", channel)
        if action == "unsubscribe":
            self.close_started.set()
            await self.close_gate.wait()
            if self.fail_close:
                return (b"error", b"cleanup_failed")
            token = byte_value(args[1])
            if self.states.get(token) not in (b"overflow", b"invalid_payload", b"owner_down"):
                self.states[token] = b"closed"
            channel = self.channels.get(token)
            if channel is not None and channel.queue.empty():
                channel.close()
            return (b"ok", True)
        if action == "subscription_finalize":
            token = byte_value(args[1])
            terminal = self.states.get(token, b"closed")
            if terminal not in (b"closed", b"overflow", b"owner_down", b"invalid_payload"):
                terminal = b"closed"
            return (b"ok", terminal)
        if action == "subscription_status":
            self.status_started.set()
            await self.status_gate.wait()
            return (b"ok", self.states.get(byte_value(args[1]), b"closed"))
        if action == "subscription_count":
            return (b"ok", sum(value == b"open" for value in self.states.values()))
        if action == "publish":
            count = 0
            for token, channel in self.channels.items():
                if self.topics[token] == args[0] and self.states[token] == b"open":
                    count += 1
                    if channel.queue.full():
                        self.states[token] = b"overflow"
                        while not channel.queue.empty():
                            channel.queue.get_nowait()
                        channel.close()
                    else:
                        channel.queue.put_nowait((b"event", args[1]))
            return (b"ok", count)
        if action == "cache_clear":
            self.values = {key: value for key, value in self.values.items() if key[0] != args[0]}
            return (b"ok", True)
        key = (byte_value(args[0]), byte_value(args[1]))
        if action == "cache_set":
            ttl = integer(args[3])
            self.ttls[key] = ttl
            if ttl == 0:
                self.values.pop(key, None)
            else:
                self.values[key] = byte_value(args[2])
            return (b"ok", True)
        if action == "cache_get":
            if key not in self.values:
                return (b"missing", b"")
            renew = integer(args[2])
            if self.ttls[key] >= 0 and renew >= 0:
                self.ttls[key] = renew
            return (b"ok", self.values[key])
        if action == "cache_exists":
            return (b"ok", key in self.values)
        if action == "cache_delete":
            self.values.pop(key, None)
            return (b"ok", True)
        if action == "cache_ttl":
            return (b"ok", self.ttls[key]) if key in self.values else (b"missing", b"")
        if action == "counter_incr":
            self.counters[key] = self.counters.get(key, 0) + integer(args[2])
            return (b"ok", self.counters[key])
        raise AssertionError("unexpected test double action {}".format(action))


@asynccontextmanager
async def opened(bridge: TestDoubleBridge) -> AsyncGenerator[Subscription]:
    async with Beam(bridge, owner="request-owner").subscribe("updates") as subscription:
        yield subscription


def only_channel(bridge: TestDoubleBridge) -> TestDoubleChannel:
    return next(iter(bridge.channels.values()))


@given(st.binary(max_size=4096))
def test_bytes_roundtrip__including_utf8(payload: bytes) -> None:
    async def run() -> None:
        bridge = TestDoubleBridge()
        beam = Beam(bridge, owner="owner")
        await beam.store("namespace").set("key", payload)
        assert await beam.store("namespace").get("key") == payload
        async with opened(bridge) as subscription:
            assert await beam.publish("updates", payload) == 1
            assert await subscription.receive() == payload
            assert only_channel(bridge).native_only == [True]

    asyncio.run(run())


@pytest.mark.parametrize("payload", [b"", b"plain UTF8", "日本語".encode(), b"\xff\x00\xfe"])
async def test_payload_examples(payload: bytes) -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        await Beam(bridge, owner="o").publish("updates", payload)
        assert await subscription.receive() == payload


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [(None, -1), (0, 0), (0.00001, 1), (0.001, 1), (1.25, 1250), (1, 1000)],
)
def test_ttl_conversion(seconds: float | None, expected: int) -> None:
    assert wire_milliseconds(seconds) == expected


@given(st.floats(min_value=0, max_value=1e12, allow_nan=False, allow_infinity=False))
def test_ttl_rounding_property(seconds: float) -> None:
    milliseconds = wire_milliseconds(seconds)
    assert milliseconds >= seconds * 1000
    assert milliseconds - 1 < seconds * 1000 or milliseconds == 0


@pytest.mark.parametrize("seconds", [-1.0, -0.001, float("nan"), float("inf"), -float("inf"), True])
async def test_invalid_ttl_never_crosses_bridge(seconds: float) -> None:
    bridge = TestDoubleBridge()
    with pytest.raises(ValueError):
        await Beam(bridge, owner="o").store("n").set("k", b"v", expires_in=seconds)
    assert bridge.calls == []


def test_finite_huge_ttl_does_not_overflow() -> None:
    assert wire_milliseconds(1.7e308) > 10**308


async def test_namespaces_counters_and_ttls() -> None:
    bridge = TestDoubleBridge()
    beam: NativeBeam = Beam(bridge, owner="o")
    first, second = beam.store("a:b"), beam.store("a")
    await first.set("c", b"first", expires_in=1.25)
    await second.set("b:c", b"second")
    assert await first.expires_in("c") == 1.25
    assert await first.get("c", renew_for=2) == b"first"
    assert await first.expires_in("c") == 2
    assert await second.get("b:c", renew_for=3) == b"second"
    assert await second.expires_in("b:c") is None
    assert await beam.incr("c", 3, namespace="a:b") == 3
    assert await first.get("c") == b"first"
    await first.delete_all()
    assert await first.get("c") is None
    assert await first.expires_in("c") is None
    assert await second.exists("b:c") is True
    await second.delete("b:c")
    await second.delete("missing")
    assert await second.exists("b:c") is False
    await first.set("zero", b"v", expires_in=0)
    assert await first.get("zero") is None


async def test_request_owner_is_explicit() -> None:
    bridge = TestDoubleBridge()
    beam = Beam(bridge, owner="application-owner")
    scopes: list[dict[str, object]] = [{}, {"extensions": {}}, {"extensions": None}]
    for scope in scopes:
        with pytest.raises(RuntimeProtocolError):
            beam.for_scope(scope)
    scoped = beam.for_scope({"extensions": {"hornbeam.owner": "request-owner"}})
    async with scoped.subscribe("updates"):
        assert bridge.calls[-1][1][0] == "request-owner"


async def test_no_runtime_has_no_fallback() -> None:
    with pytest.raises(RuntimeUnavailable, match="embedding"):
        await NativeBeam.connect()


@pytest.mark.parametrize(
    "response",
    [
        None,
        [],
        [b"ok", b"v"],
        (True, b"v"),
        (b"ok", "v"),
        (b"missing", b"bad"),
        (b"error", 1),
        (b"unknown", b"v"),
    ],
)
async def test_malformed_get(response: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["cache_get"] = response
    with pytest.raises(RuntimeProtocolError):
        await Beam(bridge, owner="o").store("n").get("k")


@pytest.mark.parametrize("response", [(b"ok", True), (b"ok", 1.0), (b"ok", -1), (b"missing", b"")])
async def test_malformed_count(response: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["subscription_count"] = response
    with pytest.raises(RuntimeProtocolError):
        await Beam(bridge, owner="o").subscription_count()


@pytest.mark.parametrize("response", [(b"ok", 1), (b"ok", False), (b"ok", None)])
async def test_strict_acknowledgements(response: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["cache_delete"] = response
    with pytest.raises(RuntimeProtocolError):
        await Beam(bridge, owner="o").store("n").delete("k")


@pytest.mark.parametrize("response", [(b"missing", b""), ("missing", "")])
async def test_exact_missing_encodings(response: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["cache_get"] = response
    assert await Beam(bridge, owner="o").store("n").get("k") is None


@pytest.mark.parametrize("response", [(b"error", b"unavailable"), ("error", "unavailable")])
async def test_unavailable_encodings(response: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["cache_get"] = response
    with pytest.raises(RuntimeUnavailable):
        await Beam(bridge, owner="o").store("n").get("k")


async def test_independent_subscriptions_and_idempotent_close() -> None:
    bridge = TestDoubleBridge()
    beam = Beam(bridge, owner="o")
    async with beam.subscribe("updates") as first, beam.subscribe("updates") as second:
        assert await beam.subscription_count() == 2
        assert await beam.publish("updates", b"hello") == 2
        assert await first.receive() == await second.receive() == b"hello"
        await first.aclose()
        await first.aclose()
        assert await beam.subscription_count() == 1
        await beam.publish("updates", b"other")
        assert await second.receive() == b"other"
        assert [value async for value in first] == []
    assert await beam.subscription_count() == 0
    assert len([a for a, _ in bridge.calls if a == "unsubscribe"]) == 2
    tokens = [args[1] for action, args in bridge.calls if action == "subscribe"]
    assert tokens[0] != tokens[1]
    assert all(len(byte_value(token)) == 16 for token in tokens)


async def test_acknowledged_close__retires_once_after_unsubscribe() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        await subscription.aclose()
        await subscription.aclose()
    assert [action for action, _ in bridge.calls] == [
        "subscribe",
        "unsubscribe",
        "subscription_finalize",
    ]


async def test_sequential_contexts__finalize_every_acknowledged_open() -> None:
    bridge = TestDoubleBridge()
    beam = Beam(bridge, owner="same-live-owner")
    for _ in range(512):
        async with beam.subscribe("updates"):
            pass
    assert [action for action, _ in bridge.calls] == [
        "subscribe",
        "unsubscribe",
        "subscription_finalize",
    ] * 512


async def test_retirement__rejects_open_status_without_retry() -> None:
    bridge = TestDoubleBridge()
    bridge.override["subscription_finalize"] = (b"ok", b"open")
    with pytest.raises(RuntimeProtocolError, match="still reports open"):
        async with opened(bridge) as subscription:
            with pytest.raises(RuntimeProtocolError, match="still reports open"):
                await subscription.aclose()
    assert sum(action == "subscription_finalize" for action, _ in bridge.calls) == 1


async def test_overflow_close__retains_reason_after_native_retirement() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        token = next(iter(bridge.channels))
        bridge.states[token] = b"overflow"
        await subscription.aclose()
        bridge.states.pop(token)
        with pytest.raises(SubscriptionOverflow):
            await subscription.receive()
    assert sum(action == "subscription_finalize" for action, _ in bridge.calls) == 1


async def test_receive_cancel_reuse_and_single_waiter() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        waiting = asyncio.create_task(subscription.receive())
        await only_channel(bridge).entered.wait()
        with pytest.raises(RuntimeError, match="one pending"):
            await subscription.receive()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        await Beam(bridge, owner="o").publish("updates", b"after cancellation")
        assert await subscription.receive() == b"after cancellation"
        assert not bridge.close_started.is_set()


async def test_close_wakes_receive() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        waiting = asyncio.create_task(subscription.receive())
        await only_channel(bridge).entered.wait()
        await subscription.aclose()
        with pytest.raises(SubscriptionClosed):
            await waiting


async def test_cancel_during_open_sends_tombstone() -> None:
    bridge = TestDoubleBridge()
    bridge.open_gate.clear()

    async def client() -> None:
        async with opened(bridge):
            pytest.fail("opening was cancelled")

    task = asyncio.create_task(client())
    await bridge.open_started.wait()
    token = byte_value(bridge.calls[0][1][1])
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bridge.states[token] == b"closed"
    assert all(action != "subscription_finalize" for action, _ in bridge.calls)
    bridge.open_gate.set()
    await bridge.call("subscribe", ["request-owner", token, b"updates", 65536])
    assert bridge.states[token] == b"closed"
    assert await Beam(bridge, owner="o").subscription_count() == 0


async def test_cleanup_survives_repeated_cancellation() -> None:
    bridge = TestDoubleBridge()
    bridge.close_gate.clear()
    entered = asyncio.Event()

    async def client() -> None:
        async with opened(bridge):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(client())
    await entered.wait()
    task.cancel()
    await bridge.close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    bridge.close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(state == b"closed" for state in bridge.states.values())


async def test_direct_close_cancellation_waits_for_ack() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        bridge.close_gate.clear()
        task = asyncio.create_task(subscription.aclose())
        await bridge.close_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        bridge.close_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await subscription.aclose()


async def test_failed_cleanup_is_explicit_and_retained() -> None:
    bridge = TestDoubleBridge()
    bridge.fail_close = True
    with pytest.raises(RuntimeProtocolError, match="cleanup_failed"):
        async with opened(bridge) as subscription:
            with pytest.raises(RuntimeProtocolError, match="cleanup_failed"):
                await subscription.aclose()
    assert sum(action == "unsubscribe" for action, _ in bridge.calls) == 1


async def test_cancel_open_then_repeated_cancel_cleanup() -> None:
    bridge = TestDoubleBridge()
    bridge.open_gate.clear()
    bridge.close_gate.clear()

    async def client() -> None:
        async with opened(bridge):
            pytest.fail("cancelled opening cannot enter the context")

    task = asyncio.create_task(client())
    await bridge.open_started.wait()
    task.cancel()
    await bridge.close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    bridge.close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(value == b"closed" for value in bridge.states.values())


async def test_cleanup_failure_is_not_hidden_by_cancellation() -> None:
    bridge = TestDoubleBridge()
    bridge.open_gate.clear()
    bridge.close_gate.clear()
    bridge.fail_close = True

    async def client() -> None:
        async with opened(bridge):
            pass

    task = asyncio.create_task(client())
    await bridge.open_started.wait()
    task.cancel()
    await bridge.close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    bridge.close_gate.set()
    with pytest.raises(RuntimeProtocolError, match="cleanup_failed"):
        await task


async def test_close_before_open_ack_never_yields_subscription() -> None:
    bridge = TestDoubleBridge()
    bridge.open_gate.clear()
    subscription = Beam(bridge, owner="o").subscribe("updates")
    opening = asyncio.create_task(subscription.__aenter__())
    await bridge.open_started.wait()
    await subscription.aclose()
    bridge.open_gate.set()
    with pytest.raises(SubscriptionClosed):
        await opening
    assert await Beam(bridge, owner="o").subscription_count() == 0
    assert sum(action == "unsubscribe" for action, _ in bridge.calls) == 1
    assert sum(action == "subscription_finalize" for action, _ in bridge.calls) == 1


async def test_simultaneous_close_callers_share_acknowledgement() -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        bridge.close_gate.clear()
        first = asyncio.create_task(subscription.aclose())
        second = asyncio.create_task(subscription.aclose())
        await bridge.close_started.wait()
        first.cancel()
        await asyncio.sleep(0)
        assert not second.done()
        bridge.close_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        await second
    assert sum(action == "unsubscribe" for action, _ in bridge.calls) == 1


async def test_application_error_runs_cleanup() -> None:
    bridge = TestDoubleBridge()
    with pytest.raises(LookupError, match="application"):
        async with opened(bridge):
            raise LookupError("application")
    assert await Beam(bridge, owner="o").subscription_count() == 0


@given(st.lists(st.booleans(), max_size=12))
def test_receive_cancellation_sequences_keep_ownership(cancel_receives: list[bool]) -> None:
    async def run() -> None:
        bridge = TestDoubleBridge()
        beam = Beam(bridge, owner="o")
        baseline = asyncio.all_tasks()
        async with opened(bridge) as subscription:
            channel = only_channel(bridge)
            for cancel in cancel_receives:
                channel.entered.clear()
                receiving = asyncio.create_task(subscription.receive())
                await channel.entered.wait()
                if cancel:
                    receiving.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await receiving
                else:
                    await beam.publish("updates", b"v")
                    assert await receiving == b"v"
                assert await beam.subscription_count() == 1
        assert await beam.subscription_count() == 0
        assert asyncio.all_tasks() == baseline

    asyncio.run(run())


@pytest.mark.parametrize("value", [True, 1.5, -2, "123"])
async def test_malformed_ttl_response(value: object) -> None:
    bridge = TestDoubleBridge()
    bridge.override["cache_ttl"] = (b"ok", value)
    with pytest.raises(RuntimeProtocolError):
        await Beam(bridge, owner="o").store("n").expires_in("k")


async def test_payload_and_namespace_limits() -> None:
    bridge = TestDoubleBridge()
    beam = Beam(bridge, owner="o")
    with pytest.raises(ValueError):
        beam.store("é" * 129)
    with pytest.raises(ValueError):
        await beam.store("n").get("é" * 513)
    with pytest.raises(ValueError):
        await beam.publish("updates", b"v" * 65537)
    with pytest.raises(ValueError):
        await beam.store("n").set("k", b"v" * 1048577)
    assert bridge.calls == []
    await beam.store("é" * 128).set("é" * 512, b"v")
    assert len(bridge.calls) == 1


async def test_cleanup_timeout_is_explicit() -> None:
    bridge = TestDoubleBridge()
    bridge.close_gate.clear()
    beam = Beam(bridge, owner="o", cleanup_timeout=0.01)
    with pytest.raises(RuntimeProtocolError, match="timed out"):
        async with beam.subscribe("updates"):
            pass


@pytest.mark.parametrize(
    "terminal", [b"closed", b"owner_down", b"overflow", b"invalid_payload", b"open", b"wat"]
)
async def test_channel_closure_queries_status(terminal: bytes) -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        token = next(iter(bridge.channels))
        bridge.states[token] = terminal
        only_channel(bridge).close()
        expected = SubscriptionClosed
        if terminal == b"overflow":
            expected = SubscriptionOverflow
        elif terminal not in (b"closed", b"owner_down"):
            expected = RuntimeProtocolError
        with pytest.raises(expected):
            await subscription.receive()
        assert bridge.status_started.is_set()


async def test_overflow_observable_and_not_iteration_end() -> None:
    bridge = TestDoubleBridge()
    beam = Beam(bridge, owner="o")
    async with opened(bridge) as subscription:
        for _ in range(3):
            await beam.publish("updates", b"v")
        with pytest.raises(SubscriptionOverflow):
            await anext(subscription)
        with pytest.raises(SubscriptionOverflow):
            await subscription.receive()


async def test_cancel_during_overflow_status_then_reuse() -> None:
    bridge = TestDoubleBridge()
    bridge.status_gate.clear()
    async with opened(bridge) as subscription:
        token = next(iter(bridge.channels))
        bridge.states[token] = b"overflow"
        channel = only_channel(bridge)
        channel.close()
        task = asyncio.create_task(subscription.receive())
        await bridge.status_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        bridge.status_gate.set()
        channel.close()
        with pytest.raises(SubscriptionOverflow):
            await subscription.receive()


async def test_close_cancels_pending_status_query() -> None:
    bridge = TestDoubleBridge()
    bridge.status_gate.clear()
    async with opened(bridge) as subscription:
        only_channel(bridge).close()
        task = asyncio.create_task(subscription.receive())
        await bridge.status_started.wait()
        await subscription.aclose()
        with pytest.raises(SubscriptionClosed):
            await task


@pytest.mark.parametrize(
    "envelope", [None, b"v", (b"event", "text"), (b"other", b"v"), (b"event", b"x" * 65537)]
)
async def test_malformed_receive(envelope: object) -> None:
    bridge = TestDoubleBridge()
    async with opened(bridge) as subscription:
        only_channel(bridge).queue.put_nowait(envelope)
        with pytest.raises(RuntimeProtocolError):
            await subscription.receive()
        assert not bridge.status_started.is_set()


async def test_malformed_open_still_unsubscribes() -> None:
    bridge = TestDoubleBridge()
    bridge.override["subscribe"] = (b"wat", object())
    with pytest.raises(RuntimeProtocolError):
        async with opened(bridge):
            pass
    assert bridge.close_started.is_set()


@pytest.mark.parametrize("topic", ["", "é" * 129])
async def test_invalid_topic_does_not_open(topic: str) -> None:
    bridge = TestDoubleBridge()
    with pytest.raises(ValueError):
        async with Beam(bridge, owner="o").subscribe(topic):
            pass
    assert bridge.calls == []


class UpstreamNative(TestDoubleBridge):
    def __init__(self) -> None:
        super().__init__()
        self.closure = "wait"
        self.wait_result: tuple[str, object] = ("error", "closed")

    def _channel_try_receive(self, resource: object) -> None:
        if self.closure == "try":
            raise RuntimeError("channel closed")

    def _channel_is_closed(self, resource: object) -> bool:
        return self.closure == "closed"

    def _channel_wait(self, resource: object, callback_id: int, capsule: object) -> object:
        return self.wait_result


@pytest.fixture
def upstream_native(monkeypatch: pytest.MonkeyPatch) -> UpstreamNative:
    source = Path(__file__).resolve().parents[3] / "erlang-python/priv/_erlang_impl/_channel.py"
    spec = spec_from_file_location("erlang", source)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "erlang", module)
    spec.loader.exec_module(module)
    native = UpstreamNative()
    monkeypatch.setattr(module, "async_call", native.async_call, raising=False)
    monkeypatch.setattr(module, "_channel_try_receive", native._channel_try_receive, raising=False)
    monkeypatch.setattr(module, "_channel_is_closed", native._channel_is_closed, raising=False)
    monkeypatch.setattr(module, "_channel_wait", native._channel_wait, raising=False)
    return native


@pytest.mark.parametrize("closure", ["try", "closed", "wait"])
@pytest.mark.parametrize("iterate", [False, True])
@pytest.mark.parametrize(
    ("terminal", "expected"),
    [
        (b"closed", SubscriptionClosed),
        (b"owner_down", SubscriptionClosed),
        (b"overflow", SubscriptionOverflow),
        (b"invalid_payload", RuntimeProtocolError),
        (b"open", RuntimeProtocolError),
    ],
)
async def test_upstream_channel__closure_queries_status(
    upstream_native: UpstreamNative,
    monkeypatch: pytest.MonkeyPatch,
    closure: str,
    iterate: bool,
    terminal: bytes,
    expected: type[Exception],
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "_loop_capsule", object(), raising=False)
    monkeypatch.setattr(loop, "_timers", {}, raising=False)
    upstream_native.closure = closure
    async with NativeBeam(owner="owner").subscribe("updates") as subscription:
        token = next(iter(upstream_native.channels))
        upstream_native.states[token] = terminal
        error = StopAsyncIteration if iterate and expected is SubscriptionClosed else expected
        with pytest.raises(error):
            if iterate:
                await anext(subscription)
            else:
                await subscription.receive()
        assert upstream_native.status_started.is_set()
        assert upstream_native.calls[-1] == ("subscription_status", ["owner", token])


async def test_upstream_channel__none_payload_is_not_closure(
    upstream_native: UpstreamNative, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "_loop_capsule", object(), raising=False)
    monkeypatch.setattr(loop, "_timers", {}, raising=False)
    upstream_native.wait_result = ("ok", None)
    async with NativeBeam(owner="owner").subscribe("updates") as subscription:
        with pytest.raises(RuntimeProtocolError, match="event envelope"):
            await subscription.receive()
        assert not upstream_native.status_started.is_set()


@pytest.mark.parametrize("max_bytes", [0, 1023, 1048577])
async def test_invalid_budget_does_not_open(max_bytes: int) -> None:
    bridge = TestDoubleBridge()
    with pytest.raises(ValueError):
        async with Beam(bridge, owner="o").subscribe("updates", max_bytes=max_bytes):
            pass
    assert bridge.calls == []
