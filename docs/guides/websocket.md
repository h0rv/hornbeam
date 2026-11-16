---
title: WebSocket Guide
description: Real-time bidirectional communication with WebSocket
order: 12
---

# WebSocket Guide

Hornbeam provides full WebSocket support via ASGI, enabling real-time bidirectional communication for chat apps, live updates, multiplayer games, and more.

## Continuous application execution

Each ASGI WebSocket application runs once for the connection lifetime. An app may
send immediately after acceptance or push while no inbound frame is pending.
Input arrives over a bounded native channel; output goes through an acknowledged
transport relay instead of a fire-and-forget process mailbox. The send callback
waits for transport progress, with a five-second output deadline. Overlarge
frames, full input buffers, stalled output, or owner death close the session and
release the input channel. The default maximum frame payload is 16 MiB.

`scope['extensions']['hornbeam.owner']` is the owning Erlang session PID, not an
ASGI standard extension. ASGI remains the default protocol; applications that
consume raw native channels must opt into `websocket_protocol => channels`.
Arbitrary five-item JSON arrays are application messages, not mode switches.

### Dependency requirements

This implementation is not compatible with the currently pinned dependencies:

- `erlang_python` 5.0.0 lacks `Channel.async_receive(native_only=True)`.
- Livery 0.9.2 resolves `erlang_ws` 0.5.0, whose `ws_session` lacks
  `send_sync/3` and `abort/1`. Compilation does not detect these remote calls;
  the first output fails at runtime with `undef`. These APIs must acknowledge
  actual transport completion, work before activation, enter the closing state
  correctly, and abort blocked output with bounded transport cleanup.

Do not replace these operations with asynchronous `ws_session:send/2`: doing so
would lose backpressure and the output-acknowledgment contract. Both dependencies
need accepted APIs and explicit version pins before this feature can be submitted
upstream. The current branch preserves those gates instead of inventing a second
transport implementation inside Hornbeam.

### Focused verification

```sh
python -m pip install pytest
PYTHONPATH=priv python -m pytest -q test/python
# Require ws_session:send_sync/3 and ws_session:abort/1:
sh test/ws_activation_stubs/run.sh
sh test/ws_output_flow_stubs/run.sh
# Also requires the patched native bridge:
rebar3 ct --suite test/hornbeam_ws_continuous_patch_SUITE.erl
```

Python tests double the embedding callback boundary. The Erlang runners use real
ws sessions/codecs but isolate native task/channel doubles in fresh VMs; they
must not be loaded into a native-runtime VM. With `erlang_ws` 0.5.0 these focused
Erlang checks fail on the missing APIs; they are prerequisites, not passing native
evidence. The native network suite remains gated on both dependency changes.

## Basic WebSocket Handler

```python
# app.py
async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        # Accept the connection
        await send({'type': 'websocket.accept'})

        while True:
            message = await receive()

            if message['type'] == 'websocket.disconnect':
                break

            if message['type'] == 'websocket.receive':
                # Echo the message back
                text = message.get('text', '')
                await send({
                    'type': 'websocket.send',
                    'text': f'Echo: {text}'
                })
```

```erlang
hornbeam:start("app:application", #{worker_class => asgi}).
```

## WebSocket Scope

| Key | Type | Description |
|-----|------|-------------|
| `type` | str | `"websocket"` |
| `path` | str | URL path |
| `query_string` | bytes | Query string |
| `headers` | list | Request headers |
| `subprotocols` | list | Requested subprotocols |
| `client` | tuple | `(host, port)` |
| `server` | tuple | `(host, port)` |

## Message Types

### Receive Messages

| Type | Fields | Description |
|------|--------|-------------|
| `websocket.connect` | - | Client requesting connection |
| `websocket.receive` | `text` or `bytes` | Message from client |
| `websocket.disconnect` | `code` | Client disconnected |

### Send Messages

| Type | Fields | Description |
|------|--------|-------------|
| `websocket.accept` | `subprotocol` | Accept connection |
| `websocket.send` | `text` or `bytes` | Send to client |
| `websocket.close` | `code`, `reason` | Close connection |

## Chat Room with Pub/Sub

Use Hornbeam's Erlang pub/sub for multi-user chat:

```python
# chat.py
from hornbeam_erlang import publish, subscribe, unsubscribe
import asyncio
import json

async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        await send({'type': 'websocket.accept'})

        # Get room from path: /chat/room-name
        path = scope.get('path', '/chat/default')
        room = path.split('/')[-1] or 'default'
        topic = f'chat:{room}'

        # Subscribe to room
        subscribe(topic)

        try:
            # Create tasks for receiving from client and pub/sub
            await handle_chat(scope, receive, send, topic)
        finally:
            unsubscribe(topic)

async def handle_chat(scope, receive, send, topic):
    while True:
        message = await receive()

        if message['type'] == 'websocket.disconnect':
            break

        if message['type'] == 'websocket.receive':
            text = message.get('text', '')
            data = json.loads(text)

            # Broadcast to all subscribers
            publish(topic, {
                'type': 'message',
                'user': data.get('user', 'anonymous'),
                'text': data.get('text', '')
            })

        # Check for pub/sub messages
        pubsub_msg = check_pubsub()
        if pubsub_msg:
            await send({
                'type': 'websocket.send',
                'text': json.dumps(pubsub_msg)
            })
```

## Binary Data

Send and receive binary data:

```python
async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        await send({'type': 'websocket.accept'})

        while True:
            message = await receive()

            if message['type'] == 'websocket.disconnect':
                break

            if message['type'] == 'websocket.receive':
                # Handle binary data
                if 'bytes' in message:
                    data = message['bytes']
                    # Process binary (e.g., image, protobuf)
                    result = process_binary(data)
                    await send({
                        'type': 'websocket.send',
                        'bytes': result
                    })
                # Handle text
                elif 'text' in message:
                    await send({
                        'type': 'websocket.send',
                        'text': message['text']
                    })
```

## Subprotocols

Handle WebSocket subprotocols:

```python
async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        # Check requested subprotocols
        requested = scope.get('subprotocols', [])

        # Choose one we support
        if 'graphql-ws' in requested:
            subprotocol = 'graphql-ws'
        elif 'json' in requested:
            subprotocol = 'json'
        else:
            subprotocol = None

        await send({
            'type': 'websocket.accept',
            'subprotocol': subprotocol
        })

        # Handle based on protocol
        if subprotocol == 'graphql-ws':
            await handle_graphql(scope, receive, send)
        else:
            await handle_generic(scope, receive, send)
```

## Connection with ETS State

Track connected clients using ETS:

```python
from hornbeam_erlang import state_set, state_get, state_delete, state_incr
import uuid

async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        # Generate unique client ID
        client_id = str(uuid.uuid4())

        await send({'type': 'websocket.accept'})

        # Register client
        state_set(f'ws:client:{client_id}', {
            'connected_at': time.time(),
            'path': scope.get('path', '/')
        })
        state_incr('ws:active_connections')

        try:
            await handle_connection(scope, receive, send, client_id)
        finally:
            # Cleanup on disconnect
            state_delete(f'ws:client:{client_id}')
            state_incr('ws:active_connections', -1)

async def handle_connection(scope, receive, send, client_id):
    while True:
        message = await receive()
        if message['type'] == 'websocket.disconnect':
            break
        # Handle messages...
```

## Live Dashboard Example

Push real-time updates to dashboard:

```python
from hornbeam_erlang import state_get
import asyncio
import json

async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        await send({'type': 'websocket.accept'})

        # Start background task to push updates
        push_task = asyncio.create_task(
            push_metrics(send)
        )

        try:
            while True:
                message = await receive()
                if message['type'] == 'websocket.disconnect':
                    break
        finally:
            push_task.cancel()

async def push_metrics(send):
    while True:
        # Get metrics from ETS
        metrics = {
            'requests': state_get('metrics:requests') or 0,
            'errors': state_get('metrics:errors') or 0,
            'latency_p99': state_get('metrics:latency_p99') or 0,
        }

        await send({
            'type': 'websocket.send',
            'text': json.dumps(metrics)
        })

        await asyncio.sleep(1)  # Update every second
```

## Error Handling

```python
async def application(scope, receive, send):
    if scope['type'] == 'websocket':
        try:
            await send({'type': 'websocket.accept'})
            await handle_websocket(scope, receive, send)
        except Exception as e:
            # Close with error code
            await send({
                'type': 'websocket.close',
                'code': 1011,  # Internal error
                'reason': 'Internal server error'
            })
```

### Close Codes

| Code | Meaning |
|------|---------|
| 1000 | Normal closure |
| 1001 | Going away |
| 1002 | Protocol error |
| 1003 | Unsupported data |
| 1008 | Policy violation |
| 1011 | Internal error |

## Configuration

```erlang
hornbeam:start("app:application", #{
    worker_class => asgi,

    %% WebSocket settings
    websocket_timeout => 60000,        % Idle timeout (ms)
    websocket_max_frame_size => 16777216  % Max frame size (16MB)
}).
```

## FastAPI WebSocket

```python
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from hornbeam_erlang import state_incr

app = FastAPI()

@app.websocket("/ws/{room}")
async def websocket_endpoint(websocket: WebSocket, room: str):
    await websocket.accept()
    state_incr(f'room:{room}:users')

    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Room {room}: {data}")
    except WebSocketDisconnect:
        state_incr(f'room:{room}:users', -1)
```

## Starlette WebSocket

```python
from starlette.applications import Starlette
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket

async def websocket_handler(websocket: WebSocket):
    await websocket.accept()
    while True:
        text = await websocket.receive_text()
        await websocket.send_text(f'Echo: {text}')

app = Starlette(routes=[
    WebSocketRoute('/ws', websocket_handler),
])
```

## Testing WebSockets

### Using websocat

```bash
websocat ws://localhost:8642/ws
```

### Using Python

```python
import asyncio
import websockets

async def test():
    async with websockets.connect('ws://localhost:8642/ws') as ws:
        await ws.send('Hello!')
        response = await ws.recv()
        print(response)

asyncio.run(test())
```

## Next Steps

- [Erlang Integration](/docs/guides/erlang-integration) - ETS, RPC, Pub/Sub details
- [WebSocket Chat Example](/docs/examples/websocket-chat) - Full chat application
- [Configuration Reference](/docs/reference/configuration) - All WebSocket options
