"""Exercise the public API through a real ASGI request and native loop."""

import asyncio

from hornbeam.services import Beam


async def application(scope, receive, send):
    assert scope["type"] == "http"
    beam = await Beam.connect()
    request_beam = beam.for_scope(scope)
    store = beam.store("native-regression")
    assert await beam.subscription_count() == 0
    async with request_beam.subscribe("native-regression") as subscription:
        for payload in (b"", b"valid UTF8", "é".encode(), b"\x00\xff"):
            await store.set("key", payload)
            stored = await store.get("key")
            assert type(stored) is bytes and stored == payload
            assert await beam.publish("native-regression", payload) == 1
            received = await asyncio.wait_for(subscription.receive(), 2)
            assert type(received) is bytes and received == payload
    assert await beam.subscription_count() == 0
    assert await beam.incr("count", namespace="native-regression") == 1
    await store.delete_all()
    assert await store.get("key") is None
    await send({"type": "http.response.start", "status": 200})
    await send({"type": "http.response.body", "body": b"ok"})
