"""Raw ASGI fixture for hornbeam_ws_continuous_patch_SUITE."""

import asyncio
from collections.abc import Awaitable, Callable

import erlang

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


def assert_compact_scope(scope: Message) -> None:
    headers = scope["headers"]
    assert isinstance(headers, tuple)
    assert all(isinstance(pair, tuple) for pair in headers)
    assert tuple(value for name, value in headers if name == b"x-repeat") == (
        b"first",
        b"second",
    )
    assert scope["subprotocols"] == ("first", "second")
    for field in ("client", "server"):
        address = scope[field]
        assert isinstance(address, tuple) and len(address) == 2
        assert address[0] == "127.0.0.1"
        assert isinstance(address[1], int) and address[1] > 0


async def app(scope: Message, receive: Receive, send: Send) -> None:
    if scope["type"] != "websocket":
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"ws fixture"})
        return
    assert await receive() == {"type": "websocket.connect"}
    assert "method" not in scope
    if scope["path"] == "/scope-shape":
        assert_compact_scope(scope)
    await send({"type": "websocket.accept"})
    await send({"type": "websocket.send", "text": "welcome"})
    extensions = scope["extensions"]
    assert isinstance(extensions, dict)
    # This reaches the wire only if hornbeam.owner is the actual WS process.
    erlang.send(
        extensions["hornbeam.owner"],
        (b"websocket_send", b"text", b"owner-direct"),
    )
    await asyncio.sleep(0.2)
    await send({"type": "websocket.send", "text": "idle-push"})
    while True:
        message = await receive()
        if message["type"] == "websocket.disconnect":
            return
        text = message.get("text")
        if isinstance(text, str):
            await send({"type": "websocket.send", "text": "echo:" + text})
        else:
            await send({"type": "websocket.send", "bytes": message["bytes"]})
