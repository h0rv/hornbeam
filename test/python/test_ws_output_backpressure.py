import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from typing import override
from unittest.mock import patch


class Channel:
    def close(self) -> None:
        return None

    async def async_receive(self, *, native_only: bool = False) -> object:
        await asyncio.Future()
        raise AssertionError


bridge = types.ModuleType("erlang")
path = Path(__file__).resolve().parents[2] / "priv/hornbeam_websocket_runner.py"
spec = importlib.util.spec_from_file_location("ws_output_runner", path)
if spec is None or spec.loader is None:
    raise ImportError(str(path))
runner = importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {"erlang": bridge}):
    spec.loader.exec_module(runner)


class OutputBackpressureTests(unittest.IsolatedAsyncioTestCase):
    @override
    async def asyncSetUp(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.messages: list[object] = []
        self.aborts = 0
        self.session = runner.WebSocketSession("relay", Channel(), "actual-owner")
        self.session.accepted = True
        bridge.__dict__["async_call"] = self.callback

    async def callback(
        self, name: str, owner: object, relay: object, message: object
    ) -> int:
        self.assertEqual(
            (name, owner, relay), ("hornbeam_ws_output", "actual-owner", "relay")
        )
        if message == b"abort":
            self.aborts += 1
            self.release.set()
            return 0
        self.messages.append(message)
        self.entered.set()
        await self.release.wait()
        return 1

    async def test_send_waits_and_concurrent_sends_are_serial(self) -> None:
        first = asyncio.create_task(
            self.session.send({"type": "websocket.send", "text": "one"})
        )
        await self.entered.wait()
        second = asyncio.create_task(
            self.session.send({"type": "websocket.send", "bytes": b"two"})
        )
        await asyncio.sleep(0)
        self.assertFalse(first.done())
        self.assertFalse(second.done())
        self.assertEqual(self.messages, [(b"websocket_send", b"text", b"one")])
        self.release.set()
        await asyncio.gather(first, second)
        self.assertEqual(self.messages[-1], (b"websocket_send", b"binary", b"two"))
        self.assertEqual(self.session.pending_output, 0)

    async def test_pending_limit_aborts_actual_owner(self) -> None:
        with patch.object(runner, "_MAX_PENDING_OUTPUT", 2):
            first = asyncio.create_task(
                self.session.send({"type": "websocket.send", "text": "one"})
            )
            await self.entered.wait()
            second = asyncio.create_task(
                self.session.send({"type": "websocket.send", "text": "two"})
            )
            await asyncio.sleep(0)
            with self.assertRaises(OSError):
                await self.session.send({"type": "websocket.send", "text": "three"})
            results = await asyncio.gather(first, second, return_exceptions=True)
        self.assertEqual(len(self.messages), 1)
        self.assertIsInstance(results[1], OSError)
        self.assertEqual(self.aborts, 1)
        self.assertEqual(self.session.pending_output, 0)

    async def test_oversized_utf8_aborts_without_output(self) -> None:
        with patch.object(runner, "_MAX_OUTPUT_FRAME", 3):
            with self.assertRaises(OSError):
                await self.session.send({"type": "websocket.send", "text": "雪雪"})
        self.assertEqual(self.messages, [])
        self.assertEqual(self.aborts, 1)

    async def test_timeout_aborts_and_raises_oserror(self) -> None:
        with patch.object(runner, "_OUTPUT_TIMEOUT", 0.01):
            with self.assertRaises(OSError):
                await self.session.send({"type": "websocket.send", "bytes": b"stalled"})
        self.assertEqual(self.aborts, 1)
        self.assertTrue(self.session.closed)

    async def test_cancel_aborts_and_preserves_cancellation(self) -> None:
        task = asyncio.create_task(
            self.session.send({"type": "websocket.send", "bytes": b"stalled"})
        )
        await self.entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.aborts, 1)
        self.assertEqual(self.session.pending_output, 0)

    async def test_close_awaits_transport_then_rejects_send(self) -> None:
        task = asyncio.create_task(
            self.session.send({"type": "websocket.close", "code": 1000})
        )
        await self.entered.wait()
        self.assertFalse(task.done())
        self.assertFalse(self.session.close_sent.done())
        self.release.set()
        await task
        self.assertTrue(self.session.close_sent.done())
        with self.assertRaises(OSError):
            await self.session.send({"type": "websocket.send", "text": "late"})

    async def test_transport_rejection_is_oserror(self) -> None:
        async def rejected(*args: object) -> int:
            return 0

        bridge.__dict__["async_call"] = rejected
        with self.assertRaises(OSError):
            await self.session.send({"type": "websocket.send", "bytes": b"gone"})
        self.assertTrue(self.session.closed)


if __name__ == "__main__":
    unittest.main()
