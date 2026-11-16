#!/bin/sh
# Run from the repository root in a fresh VM with isolated native doubles.
set -eu
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT HUP INT TERM
for file in test/ws_output_flow_stubs/*.erl.stub; do
    name=${file##*/}
    cp "$file" "$out/${name%.stub}"
done
cp test/ws_activation_stubs/py_event_loop_pool.erl.stub "$out/py_event_loop_pool.erl"
cp test/ws_activation_stubs/hornbeam_pubsub.erl.stub "$out/hornbeam_pubsub.erl"
erlc -Werror -pa _build/default/lib/ws/ebin -o "$out" \
    src/hornbeam_ws_handler.erl src/hornbeam_websocket.erl "$out"/*.erl
erl -noshell -pa _build/default/lib/ws/ebin -pa "$out" \
    -eval 'case eunit:test(hornbeam_ws_output_flow_tests, [verbose]) of ok -> halt(0); _ -> halt(1) end.'
