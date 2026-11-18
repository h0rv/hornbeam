"""Streaming probe for the native ASGI stream suite.

Each route records what it observed under the request's ``id`` query
parameter; ``/stats?id=...`` returns that record as JSON.
"""

import asyncio
import json
import threading
from collections.abc import Awaitable, Callable
from urllib.parse import parse_qs

Receive = Callable[[], Awaitable[dict[str, object]]]
Send = Callable[[dict[str, object]], Awaitable[None]]

_lock = threading.Lock()
_records: dict[str, dict[str, object]] = {}

SSE_HEADERS = [(b"content-type", b"text/event-stream"), (b"cache-control", b"no-cache")]


def record(key: str, **fields: object) -> None:
    with _lock:
        _records.setdefault(key, {}).update(fields)


def event(index: int) -> bytes:
    return f"data: {index}\n\n".encode("ascii")


async def sse(params: dict[str, str], send: Send) -> None:
    gap = int(params["gap_ms"]) / 1000
    await send({"type": "http.response.start", "status": 200, "headers": SSE_HEADERS})
    for index in range(int(params["count"])):
        if index:
            await asyncio.sleep(gap)
        await send({"type": "http.response.body", "body": event(index), "more_body": True})
    await send({"type": "http.response.body", "body": b""})


async def watch(key: str, receive: Receive, send: Send) -> None:
    """Idle stream: the only way to learn about the client leaving is receive()."""
    await send({"type": "http.response.start", "status": 200, "headers": SSE_HEADERS})
    await send({"type": "http.response.body", "body": event(0), "more_body": True})
    while (message := await receive())["type"] != "http.disconnect":
        pass
    record(key, received=message["type"])


async def ticker(key: str, send: Send) -> None:
    """Busy stream: the next send() after a disconnect must fail."""
    await send({"type": "http.response.start", "status": 200, "headers": SSE_HEADERS})
    index = 0
    try:
        while True:
            await send({"type": "http.response.body", "body": event(index), "more_body": True})
            index += 1
            await asyncio.sleep(0.02)
    except OSError:
        record(key, send_error=True)
    finally:
        record(key, done=True)


async def flood(key: str, params: dict[str, str], send: Send) -> None:
    """Send as fast as send() returns, publishing bytes accepted so far."""
    chunk = b"x" * int(params["chunk"])
    total = int(params["total"])
    sent = 0
    record(key, sent=0, done=False)
    await send({"type": "http.response.start", "status": 200})
    try:
        while sent < total:
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
            sent += len(chunk)
            record(key, sent=sent)
        await send({"type": "http.response.body", "body": b""})
    except OSError:
        record(key, send_error=True)
    finally:
        record(key, done=True)


async def application(scope: dict[str, object], receive: Receive, send: Send) -> None:
    query = scope["query_string"]
    assert isinstance(query, bytes)
    params = {name: values[0] for name, values in parse_qs(query.decode("ascii")).items()}
    key = params.get("id", "")
    match scope["path"]:
        case "/sse":
            await sse(params, send)
        case "/watch":
            await watch(key, receive, send)
        case "/ticker":
            await ticker(key, send)
        case "/flood":
            await flood(key, params, send)
        case "/stats":
            with _lock:
                body = json.dumps(_records.get(key, {})).encode("ascii")
            await send({"type": "http.response.start", "status": 200})
            await send({"type": "http.response.body", "body": body})
        case path:
            raise LookupError(path)
