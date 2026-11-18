"""HTTP receive fairness tests, independent of the native bridge."""

import asyncio

import hornbeam_asgi_worker as worker
import pytest


@pytest.mark.parametrize("body_ref", ["empty", ("body", b""), ("body", b"\xff\x00")])
def test_body_eof__should_suspend_watcher_and_allow_response(body_ref: object) -> None:
    async def exercise() -> None:
        receiver = worker.BodyReceiver(body_ref)
        first = await receiver.receive()
        assert first["type"] == "http.request"
        assert first["more_body"] is False
        returned = 0

        async def watch() -> None:
            nonlocal returned
            # A bounded loop detects immediate EOF repetition without hanging
            # the test runner itself on the inherited implementation.
            for _ in range(100):
                message = await receiver.receive()
                returned += 1
                if message["type"] == "http.disconnect":
                    return

        async with asyncio.TaskGroup() as tasks:
            watcher = tasks.create_task(watch())
            await asyncio.sleep(0)
            assert returned == 0
            assert not watcher.done()
            receiver.signal_disconnect()
        assert returned == 1

    asyncio.run(exercise())


def test_cancelled_eof_receive__should_allow_another_waiter() -> None:
    async def exercise() -> None:
        receiver = worker.BodyReceiver("empty")
        await receiver.receive()
        async with asyncio.TaskGroup() as tasks:
            pending = tasks.create_task(receiver.receive())
            await asyncio.sleep(0)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            replacement = tasks.create_task(receiver.receive())
            await asyncio.sleep(0)
            assert not replacement.done()
            receiver.signal_disconnect()
        assert replacement.result() == {"type": "http.disconnect"}

    asyncio.run(exercise())


def test_repeated_disconnect__should_yield_to_other_tasks() -> None:
    async def exercise() -> None:
        receiver = worker.BodyReceiver("empty")
        receiver.signal_disconnect()
        reads = 0

        async def watch() -> None:
            nonlocal reads
            for _ in range(100):
                assert await receiver.receive() == {"type": "http.disconnect"}
                reads += 1

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(watch())
            await asyncio.sleep(0)
            assert reads < 100
        assert reads == 100

    asyncio.run(exercise())


def test_channel_eof__should_suspend_until_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TestChannelClosed(Exception):
        pass

    class TestChannel:
        def __init__(self, resource: object) -> None:
            self.resource = resource

        async def async_receive_bytes(self) -> bytes:
            raise TestChannelClosed

    monkeypatch.setattr(worker, "ByteChannel", TestChannel)
    monkeypatch.setattr(worker, "ByteChannelClosed", TestChannelClosed)

    async def exercise() -> None:
        receiver = worker.BodyReceiver(("channel", object()))
        assert await receiver.receive() == {
            "type": "http.request",
            "body": b"",
            "more_body": False,
        }
        async with asyncio.TaskGroup() as tasks:
            pending = tasks.create_task(receiver.receive())
            await asyncio.sleep(0)
            assert not pending.done()
            receiver.signal_disconnect()
        assert pending.result() == {"type": "http.disconnect"}

    asyncio.run(exercise())


class FakeErlang:
    """Records flow-controlled output; replies 1 (written) or 0 (client gone)."""

    def __init__(self, written: bool) -> None:
        self.written = written
        self.calls: list[tuple[object, ...]] = []

    async def async_call(self, name: str, *args: object) -> int:
        self.calls.append((name, *args))
        return 1 if self.written else 0


def protocol(fake: FakeErlang, sent: list[object]) -> worker.ASGIProtocol:
    proto = worker.ASGIProtocol("owner", None, {}, worker.BodyReceiver("empty"))
    proto._send_fn = lambda _pid, message: sent.append(message)
    return proto


def test_streaming_send__should_wait_for_each_write(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeErlang(written=True)
    monkeypatch.setattr(worker, "erlang", fake)
    sent: list[object] = []
    proto = protocol(fake, sent)

    async def exercise() -> None:
        await proto.send({"type": "http.response.start", "status": 200, "headers": []})
        await proto.send({"type": "http.response.body", "body": b"a", "more_body": True})
        await proto.send({"type": "http.response.body", "body": b"b", "more_body": True})
        await proto.send({"type": "http.response.body", "body": b""})

    asyncio.run(exercise())
    assert fake.calls == [
        ("hornbeam_http_output", "owner", (b"start_response", 200, [], b"a")),
        ("hornbeam_http_output", "owner", (b"chunk", b"b")),
    ]
    assert sent == [b"fin"]


def test_streaming_send__should_raise_oserror_once_client_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeErlang(written=False)
    monkeypatch.setattr(worker, "erlang", fake)
    proto = protocol(fake, [])

    async def exercise() -> None:
        await proto.send({"type": "http.response.start", "status": 200, "headers": []})
        with pytest.raises(OSError):
            await proto.send({"type": "http.response.body", "body": b"a", "more_body": True})
        with pytest.raises(OSError):
            await proto.send({"type": "http.response.body", "body": b"b", "more_body": True})
        assert await proto.receive() == {"type": "http.disconnect"}

    asyncio.run(exercise())
    assert len(fake.calls) == 1


def test_single_body_response__should_not_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeErlang(written=True)
    monkeypatch.setattr(worker, "erlang", fake)
    sent: list[object] = []
    proto = protocol(fake, sent)

    async def exercise() -> None:
        await proto.send({"type": "http.response.start", "status": 200, "headers": []})
        await proto.send({"type": "http.response.body", "body": b"ok"})

    asyncio.run(exercise())
    assert fake.calls == []
    assert sent == [(b"start_response", 200, [], b"ok"), b"fin"]
