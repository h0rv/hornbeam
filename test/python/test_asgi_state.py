import asyncio
import sys
from collections.abc import Awaitable, Callable
from types import ModuleType

import hornbeam_asgi_worker as worker
import pytest

Message = dict[str, object]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


@pytest.mark.parametrize("mount", [None, "first", "second"])
def test_request_state__should_be_shallow_local_copy(
    monkeypatch: pytest.MonkeyPatch, mount: str | None,
) -> None:
    bridge = ModuleType("erlang")
    ffi_calls: list[tuple[object, ...]] = []

    def call(*args: object) -> object:
        ffi_calls.append(args)
        return None

    monkeypatch.setattr(bridge, "call", call, raising=False)
    monkeypatch.setitem(sys.modules, "erlang", bridge)
    import hornbeam_lifespan_runner as runner

    resource: list[object] = []
    opaque = object()
    lifespan: Message = {"resource": resource, "opaque": opaque, "replace": "original"}
    monkeypatch.setattr(runner, "_lifespan_state", lifespan if mount is None else {})
    monkeypatch.setattr(runner, "_lifespan_states", {mount: lifespan} if mount else {})
    monkeypatch.setattr(worker, "HAS_ERLANG", True)
    monkeypatch.setattr(worker, "erlang", bridge)
    messages: list[object] = []
    monkeypatch.setattr(worker, "_erlang_send", lambda pid, msg: messages.append(msg))
    states: list[Message] = []
    request_object = object()

    async def app(scope: Message, receive: Receive, send: Send) -> None:
        state = scope["state"]
        assert isinstance(state, dict)
        assert state == lifespan
        assert state is not lifespan
        assert state["resource"] is resource
        assert state["opaque"] is opaque
        assert "_ls_connection_state" not in state
        states.append(state)
        state["_ls_connection_state"] = request_object
        state["replace"] = request_object
        del state["opaque"]
        resource.append(request_object)
        await send({"type": "http.response.start", "status": 200})
        await send({"type": "http.response.body", "body": b"ok"})

    monkeypatch.setattr(worker, "_get_app", lambda module, name: app)

    async def exercise() -> None:
        for _ in range(2):
            scope: Message = {"path": "/", "root_path": "", "mount_id": mount}
            await worker.handle_asgi("owner", "probe", "app", scope, "empty")

    asyncio.run(exercise())
    assert messages == [(b"start_response", 200, [], b"ok"), b"fin"] * 2
    assert len(states) == 2
    assert states[0] is not states[1]
    assert lifespan["replace"] == "original"
    assert lifespan["opaque"] is opaque
    assert "_ls_connection_state" not in lifespan
    assert resource == [request_object, request_object]
    assert ffi_calls == []


@pytest.mark.parametrize("mount", [None, "mounted"])
def test_lifespan_state__should_preserve_objects_and_clear_on_shutdown(
    monkeypatch: pytest.MonkeyPatch, mount: str | None,
) -> None:
    import hornbeam_lifespan_runner as runner

    opaque = object()
    resource: list[object] = []
    state: Message = {"opaque": opaque, "resource": resource}
    monkeypatch.setattr(runner, "_lifespan_state", {})
    monkeypatch.setattr(runner, "_lifespan_states", {})

    async def app(scope: Message, receive: Receive, send: Send) -> None:
        assert (await receive())["type"] == "lifespan.startup"
        scope["state"] = state
        await send({"type": "lifespan.startup.complete"})
        assert (await receive())["type"] == "lifespan.shutdown"
        await send({"type": "lifespan.shutdown.complete"})

    monkeypatch.setattr(runner, "load_app", lambda module, name: app)

    async def exercise() -> None:
        commands: asyncio.Queue[str] = asyncio.Queue()
        reports: asyncio.Queue[object] = asyncio.Queue()

        class Control:
            async def async_receive(self, *, native_only: bool) -> str:
                assert native_only
                return await commands.get()

        class ChannelClosed(Exception):
            pass

        bridge = ModuleType("erlang")
        monkeypatch.setattr(bridge, "Channel", lambda ref: ref, raising=False)
        monkeypatch.setattr(bridge, "ChannelClosed", ChannelClosed, raising=False)
        monkeypatch.setattr(
            bridge, "send", lambda pid, message: reports.put_nowait(message), raising=False,
        )
        monkeypatch.setitem(sys.modules, "erlang", bridge)
        async with asyncio.TaskGroup() as tasks:
            task = tasks.create_task(runner.run_lifespan(
                "manager", 1, Control(), "probe", "app", mount, 200,
            ))
            commands.put_nowait("lifespan.startup")
            report = await asyncio.wait_for(reports.get(), 1)
            assert report == ("lifespan", 1, {
                "type": "lifespan.startup.complete", "state": state,
            })
            assert runner.get_state(mount) is state
            assert runner.get_state(mount)["opaque"] is opaque
            assert runner.get_state(mount)["resource"] is resource
            assert runner.get_state("unknown") == {}
            if mount is not None:
                assert runner.get_state() == {}
            commands.put_nowait("lifespan.shutdown")
            await asyncio.wait_for(task, 1)
        assert runner.get_state(mount) == {}
        assert state["opaque"] is opaque

    asyncio.run(exercise())
