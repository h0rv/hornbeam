---
title: Typed Runtime Services
description: Owned node-local byte storage, counters, and subscriptions
order: 24
---

# Typed runtime services

This additive API requires the Hornbeam embedding. It does not start an Erlang
runtime or substitute a Python dictionary when the embedding is unavailable.
The existing `hornbeam_erlang`, channels, and presence APIs remain available.

```python
from hornbeam.services import Beam

beam = await Beam.connect()  # Application-owned handle, e.g. in ASGI lifespan
store = beam.store("sessions")
await store.set("key", b"value", expires_in=60)
value = await store.get("key")  # bytes or None; renew_for=60 renews a hit
count = await beam.incr("requests", namespace="metrics")
```

Store values are bytes, including valid UTF-8; no automatic JSON or pickle
serialization occurs. Keys, namespaces, and topics are UTF-8 strings. Store
methods also include `exists`, `delete`, `delete_all`, and `expires_in`. TTLs are
seconds; `None` means no expiry when setting and missing/no expiry when querying.
Counters are separate from the byte store. All data is local to one Erlang node,
is not durable, and is cleared when services reset or Hornbeam stops.

## Ownership and subscriptions

```python
request_beam = beam.for_scope(scope)
async with request_beam.subscribe("updates", max_bytes=65536) as subscription:
    await beam.publish("updates", b"changed")
    message = await subscription.receive()
```

`for_scope` reads Hornbeam's nonstandard `scope['extensions']['hornbeam.owner']`
PID. This change injects it for HTTP requests; WebSocket ownership requires the
continuous-WebSocket transport proposal. Outside ASGI, `Beam(owner=pid)` accepts
an explicit runtime owner; `None` is rejected. `connect` obtains the application
owner through the single `hornbeam_services` callback.

Only one receive per subscription may be pending. `aclose` and context exit
acknowledge cleanup; owner death closes native waiters. Slow consumers get
`SubscriptionOverflow`, not silent drops. Owner shutdown or ordinary closure
raises `SubscriptionClosed` (`async for` ends normally for that condition).
Malformed envelopes and failed cleanup raise `RuntimeProtocolError`.
`subscription_count()` reports active subscriptions. A closed context cannot be
reopened, and retirement reclaims its token after cleanup is acknowledged.

## Bounds

The initial node-local limits are explicit, not a distributed quota service:

- UTF-8 namespace/topic: 256 bytes; key: 1024 bytes.
- Store value: 1 MiB; total store payloads: 64 MiB; cache/counter keys: 10,000 each.
- Published payload: 64 KiB; subscription budget: 1 KiB–1 MiB.
- 1024 owners, 32 active subscriptions/owner, 4096 active subscriptions/node,
  128 unretired tokens/owner.

Limits and namespace naming are API design choices for upstream review.

## Dependency and verification

Native subscription reads require `Channel.async_receive(native_only=True)`,
which pinned `erlang_python` 5.0.0 does not implement. This proposal must not be
submitted upstream until that dependency API is accepted and pinned. Pure Python
tests use explicit doubles and do not establish native-loop compatibility.

```sh
python -m pip install '.[test]'
PYTHONPATH=priv python -m pytest -q test/python/test_services.py
rebar3 eunit --module=hornbeam_services_mvp_tests,hornbeam_services_failure_cleanup_tests,hornbeam_services_retirement_tests,hornbeam_services_native_bytes_tests
# Requires the patched bridge:
rebar3 ct --suite test/hornbeam_services_native_SUITE.erl
```

The Python run normally skips the dependency-wrapper contract tests. To run them,
set `ERLANG_PYTHON_SOURCE` to an explicit checkout with the native-only/tagged
channel API; no sibling checkout is assumed. These tests load its Python wrapper
with the loop/NIF boundary doubled. The native Erlang tests use the real NIF and
cover bytes, quotas, overflow, owner death, registry loss, cleanup, and retirement.
The native network suite also calls `Beam.connect` and `for_scope` from a real
ASGI request and checks byte round trips, subscription delivery, and cleanup.
