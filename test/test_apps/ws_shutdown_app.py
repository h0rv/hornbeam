import asyncio
from collections.abc import Awaitable, Callable

import erlang

type Message = dict[str, object]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]


async def record(event: bytes) -> None:
    await erlang.async_call("ws_shutdown_record", event)


async def app(scope: Message, receive: Receive, send: Send) -> None:
    if scope["type"] == "lifespan":
        await receive()
        await send({"type": "lifespan.startup.complete"})
        await receive()
        await record(b"shutdown")
        await send({"type": "lifespan.shutdown.complete"})
    elif scope["type"] == "websocket":
        await receive()
        await send({"type": "websocket.accept"})
        try:
            await send({"type": "websocket.send", "text": "ready"})
            while (await receive())["type"] != "websocket.disconnect":
                pass
        finally:
            await asyncio.sleep(0.03)
            await record(b"closed")
