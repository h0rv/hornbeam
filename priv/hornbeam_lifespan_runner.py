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

"""Persistent ASGI lifespan execution on Hornbeam's running event-loop pool.

The Erlang manager owns the control channel and the submitted coroutine. No
private loop is created or paused. Handlers use their mount's lifespan loop
so loop-bound application resources retain their affinity.
Request scopes shallow-copy the retained Python state in the shared interpreter,
preserving resource identity. The startup report also supplies a bridge-converted
snapshot to the Erlang manager; it is not the source for request state.
"""

import asyncio
import importlib
import logging
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Protocol

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class ASGIApp(Protocol):
    def __call__(self, scope: Message, receive: Receive, send: Send) -> Awaitable[None]: ...


_DEV_RELOAD = os.environ.get('HORNBEAM_DEV_RELOAD', '').lower() in ('1', 'true', 'yes')
_lifespan_state: Message = {}
_lifespan_states: dict[str, Message] = {}
# Cancellation is cooperative. Retain and observe tasks that refuse it rather
# than blocking the manager forever or letting tasks disappear unobserved.
_unfinished_tasks: set[asyncio.Task[None]] = set()
_CANCEL_GRACE = 0.1


def load_app(module_name: str, callable_name: str) -> ASGIApp:
    """Resolve the configured application, preserving development reload."""
    if _DEV_RELOAD and module_name in sys.modules:
        del sys.modules[module_name]
    module = importlib.import_module(module_name)
    return getattr(module, callable_name)


def _observe_task(task: asyncio.Task[None]) -> None:
    _unfinished_tasks.discard(task)
    if not task.cancelled():
        error = task.exception()
        if error is not None:
            logging.getLogger(__name__).error('Lifespan task cleanup failed: %s', error)


async def _cancel_task(task: asyncio.Task[None]) -> None:
    if not task.done():
        task.cancel()
        await asyncio.wait({task}, timeout=_CANCEL_GRACE)
    if task.done():
        _observe_task(task)
    else:
        _unfinished_tasks.add(task)
        task.add_done_callback(_observe_task)
        logging.getLogger(__name__).error('Lifespan task ignored cancellation')


async def _response_or_exit(
    response: asyncio.Future[Message], task: asyncio.Task[None], timeout: float,
    phase: str,
) -> Message:
    done, _ = await asyncio.wait({response, task}, timeout=timeout,
                                 return_when=asyncio.FIRST_COMPLETED)
    # Preserve explicit failure messages, but never report startup readiness
    # when the application has already exited in the same loop turn.
    if response in done:
        message = response.result()
        if phase == 'startup' and message.get('type') == 'lifespan.startup.complete' and task.done():
            if task.cancelled():
                reason = 'Application cancelled'
            else:
                error = task.exception()
                reason = str(error) if error is not None else 'Application exited after startup response'
            return {'type': 'lifespan.startup.failed', 'message': reason}
        return message
    if task in done:
        if task.cancelled():
            return {'type': f'lifespan.{phase}.failed', 'message': 'Application cancelled'}
        error = task.exception()
        if error is not None:
            return {'type': f'lifespan.{phase}.failed', 'message': str(error)}
        if phase == 'startup':
            return {'type': 'lifespan.not_supported'}
        return {'type': 'lifespan.shutdown.failed', 'message': 'Application exited without shutdown response'}
    return {'type': f'lifespan.{phase}.failed', 'message': f'{phase.title()} timeout'}


async def run_lifespan(
    pid: object, token: int, channel_ref: object, app_module: str,
    app_callable: str, mount_id: str | None, timeout_ms: int = 30000,
) -> None:
    """Own one application's lifespan until shutdown or control-channel closure.

    Protocol responses go directly to the manager. The native input channel also
    lets an Erlang timeout interrupt an application that is not in receive().
    """
    import erlang

    channel = erlang.Channel(channel_ref)
    task: asyncio.Task[None] | None = None
    control: asyncio.Task[object] | None = None
    loop = asyncio.get_running_loop()
    startup_response: asyncio.Future[Message] = loop.create_future()
    shutdown_response: asyncio.Future[Message] = loop.create_future()
    receive_queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=2)
    phase = 'startup'
    timeout = timeout_ms / 1000
    state = _lifespan_state if mount_id is None else {}

    def report(message: Message) -> None:
        erlang.send(pid, ('lifespan', token, message))

    async def receive() -> Message:
        return await receive_queue.get()

    async def send(message: Message) -> None:
        response = startup_response if phase == 'startup' else shutdown_response
        if response.done():
            raise RuntimeError('Duplicate lifespan response')
        response.set_result(message)

    try:
        command = await channel.async_receive(native_only=True)
        if command not in ('lifespan.startup', b'lifespan.startup'):
            raise RuntimeError('Expected lifespan.startup')
        app = load_app(app_module, app_callable)
        scope: Message = {
            'type': 'lifespan',
            'asgi': {'version': '3.0', 'spec_version': '2.4'},
            'state': state,
        }

        async def invoke() -> None:
            await app(scope, receive, send)

        task = asyncio.create_task(invoke())
        receive_queue.put_nowait({'type': 'lifespan.startup'})
        response = await _response_or_exit(startup_response, task, timeout, phase)
        kind = response.get('type')
        if kind != 'lifespan.startup.complete':
            if kind != 'lifespan.startup.failed':
                response = {'type': 'lifespan.not_supported'}
            report(response)
            return
        # Preserve the current state result contract, including scope replacement.
        state_result = scope.get('state', {})
        if not isinstance(state_result, dict):
            raise TypeError('Lifespan state must be a dict')
        state = state_result
        if mount_id is None:
            _replace_state(state)
        else:
            _lifespan_states[mount_id] = state
        report({'type': 'lifespan.startup.complete', 'state': state_result})

        # This await keeps the coroutine alive without pausing the pool loop.
        # Also detect app exit instead of leaving a zombie started lifespan.
        control = asyncio.create_task(channel.async_receive(native_only=True))
        done, _ = await asyncio.wait({control, task}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            error = None if task.cancelled() else task.exception()
            raise RuntimeError(f'Lifespan exited before shutdown: {error}')
        command = control.result()
        if command not in ('lifespan.shutdown', b'lifespan.shutdown'):
            raise RuntimeError('Expected lifespan.shutdown')
        phase = 'shutdown'
        deadline = loop.time() + timeout
        receive_queue.put_nowait({'type': 'lifespan.shutdown'})
        response = await _response_or_exit(shutdown_response, task, timeout, phase)
        if response.get('type') == 'lifespan.shutdown.complete' and not task.done():
            await asyncio.wait({task}, timeout=max(0, deadline - loop.time()))
            if not task.done():
                response = {'type': 'lifespan.shutdown.failed', 'message': 'Shutdown task timeout'}
        if task.done() and not task.cancelled() and task.exception() is not None:
            response = {'type': 'lifespan.shutdown.failed', 'message': str(task.exception())}
        await _cancel_task(task)
        report(response)
    except erlang.ChannelClosed:
        # Manager timed out or is terminating. Its caller already has an outcome.
        pass
    except Exception as error:
        if startup_response.done() and phase == 'startup':
            # Startup was already reported; async_result exposes unexpected exit.
            raise
        report({'type': f'lifespan.{phase}.failed', 'message': str(error)})
    finally:
        try:
            if control is not None:
                if not control.done():
                    control.cancel()
                try:
                    await control
                except (asyncio.CancelledError, erlang.ChannelClosed):
                    pass
        finally:
            # A failed native receive must not bypass application cleanup. Keep
            # its original exception while always releasing this mount's state.
            try:
                if task is not None and not task.done() and task not in _unfinished_tasks:
                    await _cancel_task(task)
            finally:
                if mount_id is None:
                    if _lifespan_state is state:
                        clear_state()
                elif _lifespan_states.get(mount_id) is state:
                    _lifespan_states.pop(mount_id, None)


def _replace_state(state: Message) -> None:
    global _lifespan_state
    _lifespan_state = state


def get_state(mount_id: str | None = None) -> Message:
    """Return this lifespan's Python state; request handlers must shallow-copy it."""
    if mount_id is None:
        return _lifespan_state
    return _lifespan_states.get(mount_id, {})


def set_state(key: str, value: object) -> None:
    _lifespan_state[key] = value


def clear_state() -> None:
    _replace_state({})
