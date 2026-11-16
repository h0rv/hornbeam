# Focused WebSocket transport regressions

Run these commands from the Hornbeam fork. They compile only the two WebSocket modules and this test. They use the existing ws dependency beams and do not run rebar.

```sh
sh test/ws_activation_stubs/run.sh
sh test/ws_output_flow_stubs/run.sh
```

Run in a fresh Erlang VM only. The native task, input channel, event loop pool, and pubsub modules here are test doubles. Their `.erl.stub` suffix keeps them out of ordinary source compilation. The real ws_session and frame codec handle output before activation, the closing state, and the close timeout. The transport is an observer double rather than a real network socket. These tests do not establish native bridge compatibility.

The Python lifecycle tests run separately:

```sh
python3 -m unittest discover \
    -s test/python -p test_ws_continuous_native_patch.py -v
```

Coverage includes immediate output before activation, real transport close timeout without peer acknowledgment, a 50 ms awaited application cleanup, a 100-frame fast consumer, and a consumer that resumes after backpressure.
