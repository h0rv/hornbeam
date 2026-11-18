# Copyright 2026 Benoit Chesneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import importlib
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Protocol

import erlang
from hornbeam_lifespan_runner import get_state

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
App = Callable[[Message, Receive, Send], Awaitable[None]]

_DEV_RELOAD = os.environ.get("HORNBEAM_DEV_RELOAD", "").lower() in ("1", "true", "yes")
_DISCONNECT_GRACE = 1.0
_INPUT_QUEUE_SIZE = 16
_INPUT_BACKPRESSURE_TIMEOUT = 0.25
_OUTPUT_TIMEOUT = 7.0
_MAX_OUTPUT_FRAME = 16777216
_MAX_PENDING_OUTPUT = 16
_sessions: dict[str, "WebSocketSession"] = {}


class NativeInput(Protocol):
    async def async_receive(self, *, native_only: bool = False) -> object: ...

    def close(self) -> None: ...


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8")
    raise TypeError("Expected text")


def _close_reason(value: object) -> bytes:
    # The wire limit is 123 bytes after the two-byte status code.
    return (
        _text(value)[:123]
        .encode("utf-8")[:123]
        .decode("utf-8", "ignore")
        .encode("utf-8")
    )


class WebSocketSession:
    """One app invocation and its bounded, independently watched input."""

    def __init__(
        self, owner: object, channel: NativeInput, transport_owner: object | None = None
    ) -> None:
        self.owner = owner
        self.transport_owner = owner if transport_owner is None else transport_owner
        self.output_lock = asyncio.Lock()
        self.pending_output = 0
        self.output_aborted = False
        self.channel = channel
        self.incoming: asyncio.Queue[Message] = asyncio.Queue(_INPUT_QUEUE_SIZE)
        self.accepted = False
        self.closed = False
        self.disconnected = False
        self.disconnect_code = 1006
        self.close_sent = asyncio.Event()

    def emit(self, message: object) -> None:
        erlang.send(self.owner, message)

    async def abort_output(self) -> None:
        if self.output_aborted:
            return
        self.output_aborted = True
        self.closed = True
        self.close_sent.set()
        try:
            async with asyncio.timeout(_OUTPUT_TIMEOUT):
                await erlang.async_call(
                    "hornbeam_ws_output", self.transport_owner, self.owner, b"abort"
                )
        except Exception:
            pass

    async def output(self, message: object, *, closing: bool = False) -> None:
        try:
            async with asyncio.timeout(_OUTPUT_TIMEOUT):
                result = await erlang.async_call(
                    "hornbeam_ws_output", self.transport_owner, self.owner, message
                )
            if result != 1 or self.disconnected or (self.closed and not closing):
                raise OSError("WebSocket transport is closed")
        except Exception as error:
            await self.abort_output()
            raise OSError("WebSocket output failed") from error

    def signal_disconnect(self, code: int) -> None:
        self.disconnected = True
        self.disconnect_code = code
        # Pending data is no longer useful once the peer is gone. Leave a
        # disconnect to wake an app that is currently blocked in receive().
        while not self.incoming.empty():
            self.incoming.get_nowait()
        self.incoming.put_nowait({"type": "websocket.disconnect", "code": code})

    async def receive(self) -> Message:
        if self.disconnected:
            return {"type": "websocket.disconnect", "code": self.disconnect_code}
        return await self.incoming.get()

    async def send(self, message: Message) -> None:
        if self.closed or self.disconnected:
            raise OSError("WebSocket connection is closed")
        if self.pending_output >= _MAX_PENDING_OUTPUT:
            await self.abort_output()
            raise OSError("WebSocket pending output limit exceeded")
        for field in ("text", "bytes", "subprotocol"):
            value = message.get(field)
            if isinstance(value, (str, bytes)) and len(value) > _MAX_OUTPUT_FRAME:
                await self.abort_output()
                raise OSError("WebSocket output frame limit exceeded")
        self.pending_output += 1
        try:
            async with self.output_lock:
                await self.send_message(message)
        except asyncio.CancelledError:
            await self.abort_output()
            raise
        finally:
            self.pending_output -= 1

    async def send_message(self, message: Message) -> None:
        if self.closed or self.disconnected:
            raise OSError("WebSocket connection is closed")
        kind = message.get("type")
        if kind == "websocket.accept":
            if self.accepted:
                raise RuntimeError("WebSocket already accepted")
            subprotocol = message.get("subprotocol")
            await self.output(
                (
                    b"websocket_accept",
                    b"" if subprotocol is None else _text(subprotocol).encode("utf-8"),
                )
            )
            self.accepted = True
        elif kind == "websocket.send":
            if not self.accepted:
                raise RuntimeError("Cannot send before accepting connection")
            text = message.get("text")
            data = message.get("bytes")
            if text is not None and data is None:
                text = _text(text)
                if len(text) > _MAX_OUTPUT_FRAME:
                    await self.abort_output()
                    raise OSError("WebSocket output frame limit exceeded")
                payload = text.encode("utf-8")
                frame_kind = b"text"
            elif isinstance(data, bytes) and text is None:
                payload = data
                frame_kind = b"binary"
            else:
                raise TypeError("websocket.send requires exactly one of text or bytes")
            if len(payload) > _MAX_OUTPUT_FRAME:
                await self.abort_output()
                raise OSError("WebSocket output frame limit exceeded")
            await self.output((b"websocket_send", frame_kind, payload))
        elif kind == "websocket.close":
            code = message.get("code", 1000)
            if not isinstance(code, int):
                raise TypeError("WebSocket close code must be an integer")
            await self.close(code, _close_reason(message.get("reason", "")))
        else:
            error = "Unsupported WebSocket ASGI message: {!r}".format(kind)
            raise RuntimeError(error)

    async def close(self, code: int, reason: bytes = b"") -> None:
        if self.closed or self.disconnected:
            return
        self.closed = True
        try:
            await self.output((b"websocket_close", code, reason), closing=True)
        finally:
            self.close_sent.set()

    async def pump_input(self) -> None:
        """Observe disconnect even when the app never calls receive again."""
        try:
            while True:
                event = await self.channel.async_receive(native_only=True)
                if not isinstance(event, tuple) or len(event) != 2:
                    raise TypeError("Invalid WebSocket input event")
                kind, data = event
                if kind in (b"disconnect", "disconnect"):
                    if not isinstance(data, int):
                        raise TypeError("Invalid disconnect code")
                    self.signal_disconnect(data)
                    return
                if not isinstance(data, bytes):
                    raise TypeError("WebSocket frame must use forced bytes")
                message: Message
                if kind in (b"text", "text"):
                    message = {
                        "type": "websocket.receive",
                        "text": data.decode("utf-8"),
                    }
                elif kind in (b"binary", "binary"):
                    message = {"type": "websocket.receive", "bytes": data}
                else:
                    raise TypeError("Unknown WebSocket input event")
                try:
                    # A native receive can complete synchronously for an entire
                    # burst. Yield to the app and apply bounded backpressure
                    # rather than rejecting its seventeenth ready frame.
                    async with asyncio.timeout(_INPUT_BACKPRESSURE_TIMEOUT):
                        await self.incoming.put(message)
                    await asyncio.sleep(0)
                except TimeoutError:
                    await self.send(
                        {
                            "type": "websocket.close",
                            "code": 1013,
                            "reason": "Application input queue full",
                        }
                    )
                    self.signal_disconnect(1013)
                    return
        except erlang.ChannelClosed:
            self.signal_disconnect(1006)

    async def run(self, app: App, scope: Message) -> None:
        self.incoming.put_nowait({"type": "websocket.connect"})

        # All children are retained, cancelled and awaited before this task
        # returns. No finished app is ever invoked a second time.
        async def invoke_app() -> None:
            await app(scope, self.receive, self.send)

        app_task = asyncio.create_task(invoke_app())
        input_task = asyncio.create_task(self.pump_input())
        close_task = asyncio.create_task(self.close_sent.wait())
        tasks = (app_task, input_task, close_task)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if input_task in done:
                input_task.result()
            if input_task in done or close_task in done:
                # Application close can race an awaited finally block. Allow
                # cooperative completion before cancelling an unresponsive app.
                await asyncio.wait((app_task,), timeout=_DISCONNECT_GRACE)
            if app_task.done():
                app_task.result()
                if not self.closed and not self.disconnected:
                    await self.send(
                        {
                            "type": "websocket.close",
                            "code": 1000 if self.accepted else 1011,
                        }
                    )
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            # Await each child even if another child failed during cleanup.
            await asyncio.gather(*tasks, return_exceptions=True)


async def handle_websocket(
    owner: object,
    app_module: str,
    app_callable: str,
    scope: Message,
    session_id: str,
    input_ref: object,
    output_owner: object | None = None,
) -> bytes:
    """Entry point submitted once by hornbeam_ws_handler via submit_task."""
    app_module = _text(app_module)
    app_callable = _text(app_callable)
    session_id = _text(session_id)
    session = WebSocketSession(
        owner if output_owner is None else output_owner,
        erlang.Channel(input_ref),
        owner,
    )
    _sessions[session_id] = session
    scope["session_id"] = session_id
    scope["state"] = get_state(scope.get("mount_id")).copy()
    try:
        extensions = scope.setdefault("extensions", {})
        if not isinstance(extensions, dict):
            raise TypeError("scope extensions must be a map")
        extensions["hornbeam.owner"] = owner
        if _DEV_RELOAD and app_module in sys.modules:
            del sys.modules[app_module]
        module = importlib.import_module(app_module)
        app = module.__dict__[app_callable]
        await session.run(app, scope)
    except asyncio.CancelledError:
        session.signal_disconnect(1006)
        raise
    except Exception:
        # Do not leak exception details (possibly containing secrets) on wire.
        if not session.closed and not session.disconnected:
            await session.send(
                {
                    "type": "websocket.close",
                    "code": 1011,
                    "reason": "Application error",
                }
            )
        raise
    finally:
        session.channel.close()
        _sessions.pop(session_id, None)
    return b"done"


def get_session_count() -> int:
    return len(_sessions)


def get_session_ids() -> list[str]:
    return list(_sessions)


def broadcast(session_id: str, topic: str, message: object) -> bool:
    """Preserve the legacy broadcast API without buffering until input arrives."""
    session = _sessions.get(_text(session_id))
    if session is None or session.closed or session.disconnected:
        return False
    session.emit((b"websocket_broadcast", topic, message))
    return True
