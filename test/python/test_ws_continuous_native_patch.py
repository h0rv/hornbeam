"""Unit regressions for continuous WS execution, not native bridge evidence.

Run: uv run --python 3.14 python -m unittest discover -s test/python \
    -p test_ws_continuous_native_patch.py -v
"""

import asyncio
import importlib.util
import sys
import types
import unittest
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import override

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
App = Callable[[Message, Receive, Send], Awaitable[None]]
_EOF = object()


class FakeChannelClosed(Exception):
    pass


class FakeChannel:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.closed = False
        self.native_calls = 0

    async def async_receive(self, *, native_only: bool = False) -> object:
        if not native_only:
            raise AssertionError("Runner must require native input")
        self.native_calls += 1
        value = await self.queue.get()
        if value is _EOF:
            raise FakeChannelClosed
        return value

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.queue.put_nowait(_EOF)


def channel_from_ref(ref: object) -> FakeChannel:
    if not isinstance(ref, FakeChannel):
        raise TypeError("Invalid channel reference")
    return ref


_bridge = types.ModuleType("erlang")
_bridge.__dict__.update(Channel=channel_from_ref, ChannelClosed=FakeChannelClosed)
_previous_bridge = sys.modules.get("erlang")
sys.modules["erlang"] = _bridge
try:
    _path = Path(__file__).resolve().parents[2] / "priv/hornbeam_websocket_runner.py"
    _spec = importlib.util.spec_from_file_location("ws_continuous_patch_runner", _path)
    if _spec is None or _spec.loader is None:
        raise ImportError(str(_path))
    runner = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(runner)
finally:
    if _previous_bridge is None:
        del sys.modules["erlang"]
    else:
        sys.modules["erlang"] = _previous_bridge


class ContinuousWebSocketPatchTests(unittest.IsolatedAsyncioTestCase):
    @override
    async def asyncSetUp(self) -> None:
        self.output: asyncio.Queue[tuple[object, object]] = asyncio.Queue()
        _bridge.__dict__["send"] = lambda owner, message: self.output.put_nowait((owner, message))
        async def async_call(name: str, transport_owner: object, owner: object, message: object) -> int:
            self.assertEqual(name, "hornbeam_ws_output")
            if message != b"abort":
                self.output.put_nowait((owner, message))
            return 1
        _bridge.__dict__["async_call"] = async_call
        self.tasks: list[asyncio.Task[bytes]] = []
        self.modules: list[str] = []

    @override
    async def asyncTearDown(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for name in self.modules:
            sys.modules.pop(name, None)
        self.assertEqual(runner.get_session_count(), 0)

    def start(
        self, app: App, owner: str = "ws-pid", *, output_owner: str | None = None
    ) -> tuple[FakeChannel, asyncio.Task[bytes]]:
        name = "ws_continuous_patch_app_{}".format(len(self.modules))
        module = types.ModuleType(name)
        module.__dict__["app"] = app
        sys.modules[name] = module
        self.modules.append(name)
        channel = FakeChannel()
        task: asyncio.Task[bytes] = asyncio.create_task(
            runner.handle_websocket(owner, name, "app", {}, owner, channel, output_owner)
        )
        self.tasks.append(task)
        return channel, task

    async def frame(self) -> tuple[object, object]:
        async with asyncio.timeout(2):
            return await self.output.get()

    async def finish(self, task: asyncio.Task[bytes]) -> bytes:
        async with asyncio.timeout(2):
            return await task

    async def test_accept_welcome_and_idle_output_are_all_immediate(self) -> None:
        async def app(scope: Message, receive: Receive, send: Send) -> None:
            self.assertEqual(scope["extensions"], {"hornbeam.owner": "ws-pid"})
            self.assertEqual(await receive(), {"type": "websocket.connect"})
            await send({"type": "websocket.accept"})
            await send({"type": "websocket.send", "text": "welcome"})
            await asyncio.sleep(0.02)
            await send({"type": "websocket.send", "text": "idle-push"})
            await receive()

        channel, task = self.start(app)
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_accept", b"")))
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_send", b"text", b"welcome")))
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_send", b"text", b"idle-push")))
        self.assertGreater(channel.native_calls, 0)
        channel.queue.put_nowait((b"disconnect", 1000))
        await self.finish(task)
        self.assertTrue(channel.closed)

    async def test_pyview_five_item_join_is_application_data_and_binary_stays_bytes(
        self,
    ) -> None:
        seen: list[Message] = []

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            for _ in range(2):
                seen.append(await receive())
            await send({"type": "websocket.send", "bytes": seen[1]["bytes"]})
            await send({"type": "websocket.close", "code": 1000})

        channel, task = self.start(app)
        join = b'["1","1","lv:probe","phx_join",{}]'
        channel.queue.put_nowait(("text", join))
        channel.queue.put_nowait(("binary", "café雪".encode()))
        await self.finish(task)
        self.assertEqual(
            seen,
            [
                {"type": "websocket.receive", "text": join.decode()},
                {"type": "websocket.receive", "bytes": "café雪".encode()},
            ],
        )
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_accept", b"")))
        self.assertEqual(
            await self.frame(),
            ("ws-pid", (b"websocket_send", b"binary", "café雪".encode())),
        )
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_close", 1000, b"")))

    async def test_finished_application_is_not_restarted(self) -> None:
        invocations = 0

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            nonlocal invocations
            invocations += 1
            await send({"type": "websocket.accept"})

        channel, task = self.start(app)
        await self.finish(task)
        channel.queue.put_nowait((b"text", b"late"))
        await asyncio.sleep(0)
        self.assertEqual(invocations, 1)
        self.assertTrue(channel.closed)
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_accept", b"")))
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_close", 1000, b"")))

    async def test_disconnect_is_delivered_to_receive_with_peer_code(self) -> None:
        disconnected: list[Message] = []

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            disconnected.append(await receive())

        channel, task = self.start(app)
        await self.frame()
        channel.queue.put_nowait((b"disconnect", 1001))
        channel.close()
        await self.finish(task)
        self.assertEqual(disconnected, [{"type": "websocket.disconnect", "code": 1001}])
        self.assertTrue(self.output.empty())

    async def test_channel_eof_cancels_app_that_never_receives_again(self) -> None:
        finalized = asyncio.Event()

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            try:
                await send({"type": "websocket.accept"})
                await asyncio.Event().wait()
            finally:
                finalized.set()

        channel, task = self.start(app)
        await self.frame()
        channel.close()
        await self.finish(task)
        self.assertTrue(finalized.is_set())

    async def test_output_relay_does_not_replace_scope_owner(self) -> None:
        async def app(scope: Message, receive: Receive, send: Send) -> None:
            self.assertEqual(scope["extensions"], {"hornbeam.owner": "ws-pid"})
            await send({"type": "websocket.accept"})

        _, task = self.start(app, output_owner="relay-pid")
        await self.finish(task)
        self.assertEqual(await self.frame(), ("relay-pid", (b"websocket_accept", b"")))
        self.assertEqual(await self.frame(), ("relay-pid", (b"websocket_close", 1000, b"")))

    async def test_application_close_allows_awaited_finally_cleanup(self) -> None:
        finalized = asyncio.Event()

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            try:
                await send({"type": "websocket.accept"})
                await send({"type": "websocket.close", "code": 1000})
            finally:
                await asyncio.sleep(0.05)
                finalized.set()

        channel, task = self.start(app)
        await self.finish(task)
        self.assertTrue(finalized.is_set())
        self.assertTrue(channel.closed)

    async def test_ready_fast_consumer_handles_native_burst_without_false_overflow(self) -> None:
        seen: list[Message] = []

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            for _ in range(100):
                seen.append(await receive())
            await send({"type": "websocket.close"})

        channel, task = self.start(app)
        await self.frame()
        for _ in range(100):
            channel.queue.put_nowait((b"text", b"x"))
        await self.finish(task)
        self.assertEqual(len(seen), 100)
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_close", 1000, b"")))
        self.assertTrue(self.output.empty())

    async def test_backpressure_recovers_when_slow_consumer_resumes(self) -> None:
        seen: list[Message] = []

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            await asyncio.sleep(0.05)
            for _ in range(32):
                seen.append(await receive())
            await send({"type": "websocket.close"})

        channel, task = self.start(app)
        await self.frame()
        for _ in range(32):
            channel.queue.put_nowait((b"text", b"x"))
        await self.finish(task)
        self.assertEqual(len(seen), 32)
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_close", 1000, b"")))

    async def test_application_close_cleans_up_without_peer_ack(self) -> None:
        finalized = asyncio.Event()

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            try:
                await send({"type": "websocket.close", "code": 1008, "reason": "no"})
                await asyncio.Event().wait()
            finally:
                finalized.set()

        channel, task = self.start(app)
        await self.finish(task)
        self.assertEqual(await self.frame(), ("ws-pid", (b"websocket_close", 1008, b"no")))
        self.assertTrue(finalized.is_set())
        self.assertTrue(channel.closed)

    async def test_failure_closes_once_and_does_not_expose_exception(self) -> None:
        async def app(scope: Message, receive: Receive, send: Send) -> None:
            raise ValueError("secret")

        channel, task = self.start(app)
        with self.assertRaisesRegex(ValueError, "secret"):
            await self.finish(task)
        self.assertEqual(
            await self.frame(),
            ("ws-pid", (b"websocket_close", 1011, b"Application error")),
        )
        self.assertTrue(self.output.empty())
        self.assertTrue(channel.closed)

    async def test_slow_app_input_overflow_is_observable_and_bounded(self) -> None:
        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            await asyncio.Event().wait()

        channel, task = self.start(app)
        await self.frame()
        for _ in range(17):
            channel.queue.put_nowait((b"text", b"data"))
        self.assertEqual(
            await self.frame(),
            ("ws-pid", (b"websocket_close", 1013, b"Application input queue full")),
        )
        await self.finish(task)
        self.assertTrue(channel.closed)

    async def test_cancellation_awaits_app_cleanup_and_closes_channel(self) -> None:
        finalized = asyncio.Event()

        async def app(scope: Message, receive: Receive, send: Send) -> None:
            try:
                await send({"type": "websocket.accept"})
                await receive()
                await receive()
            finally:
                await asyncio.sleep(0)
                finalized.set()

        channel, task = self.start(app)
        await self.frame()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await self.finish(task)
        self.assertTrue(finalized.is_set())
        self.assertTrue(channel.closed)

    async def test_independent_clients_and_immediate_legacy_broadcast(self) -> None:
        async def app(scope: Message, receive: Receive, send: Send) -> None:
            await receive()
            await send({"type": "websocket.accept"})
            runner.broadcast(scope["session_id"], "topic", {"hello": "world"})
            await receive()

        first, first_task = self.start(app, "first")
        second, second_task = self.start(app, "second")
        outputs = [await self.frame() for _ in range(4)]
        self.assertEqual([owner for owner, _ in outputs].count("first"), 2)
        self.assertEqual([owner for owner, _ in outputs].count("second"), 2)
        first.queue.put_nowait((b"disconnect", 1000))
        await self.finish(first_task)
        self.assertFalse(second_task.done())
        self.assertEqual(runner.get_session_ids(), ["second"])
        second.queue.put_nowait((b"disconnect", 1000))
        await self.finish(second_task)


if __name__ == "__main__":
    unittest.main()
