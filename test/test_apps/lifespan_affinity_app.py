import asyncio
from contextlib import suppress
from typing import NotRequired, TypedDict

from hornbeam_lifespan_runner import Receive, Send


class State(TypedDict):
    loop: asyncio.AbstractEventLoop
    requests: asyncio.Queue[None]
    values: asyncio.Queue[str]
    request_local: NotRequired[bool]


class Scope(TypedDict):
    type: str
    state: State


async def app(scope: Scope, receive: Receive, send: Send) -> None:
    if scope['type'] == 'lifespan':
        await receive()
        requests: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
        values: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        state = scope['state']
        state.update(loop=asyncio.get_running_loop(), requests=requests, values=values)

        async def feed() -> None:
            while True:
                await requests.get()
                await values.put('same-loop')

        worker = asyncio.create_task(feed())
        try:
            await send({'type': 'lifespan.startup.complete'})
            await receive()
        finally:
            worker.cancel()
            with suppress(asyncio.CancelledError):
                await worker
        await send({'type': 'lifespan.shutdown.complete'})
        return

    state = scope['state']
    assert asyncio.get_running_loop() is state['loop'], 'handler is not on lifespan loop'
    assert 'request_local' not in state
    state['request_local'] = True
    state['requests'].put_nowait(None)
    async with asyncio.timeout(2):
        value = await state['values'].get()
    if scope['type'] == 'http':
        await send({'type': 'http.response.start', 'status': 200, 'headers': []})
        await send({'type': 'http.response.body', 'body': value.encode()})
    else:
        assert (await receive())['type'] == 'websocket.connect'
        await send({'type': 'websocket.accept'})
        await send({'type': 'websocket.send', 'text': value})
        await receive()
