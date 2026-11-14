-module(hornbeam_ws_output_flow_tests).
-include_lib("eunit/include/eunit.hrl").
-export([send/2, activate/1, close/1, classify/2]).

send(Observer, Data) ->
    Observer ! {sending, self(), iolist_to_binary(Data)},
    receive {release, Result} -> Result end.
activate(Observer) -> Observer ! {activated, self()}, ok.
close(_) -> ok.
classify(_, _) -> ignore.

start(Max) ->
    persistent_term:put(ws_output_observer, self()),
    {ok, Owner} = ws_session:start(#{transport => ?MODULE, handle => self(),
        role => server, handler => hornbeam_ws_handler,
        handler_opts => #{scope => #{<<"path">> => <<"/flow">>},
            session_id => term_to_binary(make_ref()), max_frame_size => Max,
            app_module => <<"unused">>, app_callable => <<"app">>},
        idle_timeout => infinity, close_timeout => 50}),
    receive {submitted, Owner, Relay, Scope} ->
        ?assertEqual(Owner, maps:get(<<"hornbeam.owner">>, maps:get(<<"extensions">>, Scope))),
        {Owner, Relay}
    after 1000 -> error(no_submission) end.

call(Owner, Relay, Message) ->
    Parent = self(),
    spawn(fun() -> Parent ! {output_result, hornbeam_ws_handler:output([Owner, Relay, Message])} end).

entered(Owner) ->
    receive {sending, Owner, Bytes} -> Bytes after 1000 -> error(no_transport_send) end.
result(Expected) ->
    receive {output_result, Value} -> ?assertEqual(Expected, Value)
    after 1000 -> error(no_callback_reply) end.

ready_wait_output_awaits_transport_test() ->
    {Owner, Relay} = start(1024),
    try
        ?assertEqual(1, hornbeam_ws_handler:output([Owner, Relay, {<<"websocket_accept">>, <<>>}])),
        call(Owner, Relay, {<<"websocket_send">>, <<"text">>, <<"hello">>}),
        ?assertEqual(iolist_to_binary(ws_frame:encode({text, <<"hello">>}, server)), entered(Owner)),
        receive {output_result, _} -> error(early_callback_reply) after 20 -> ok end,
        ?assertEqual({message_queue_len, 0}, process_info(Owner, message_queue_len)),
        ?assertEqual({message_queue_len, 0}, process_info(Relay, message_queue_len)),
        Owner ! {release, ok},
        result(1),
        ws_session:activate(Owner),
        receive {activated, Owner} -> ok after 1000 -> error(activation_lost) end
    after exit(Owner, kill) end.

close_waits_then_runs_close_timeout_test() ->
    {Owner, Relay} = start(1024),
    Monitor = erlang:monitor(process, Owner),
    call(Owner, Relay, {<<"websocket_close">>, 1000, <<>>}),
    _ = entered(Owner),
    receive {output_result, _} -> error(early_close_ack) after 20 -> ok end,
    Owner ! {release, ok},
    result(1),
    receive {'DOWN', Monitor, process, Owner, normal} -> ok
    after 1000 -> exit(Owner, kill), error(close_timeout_lost) end.

frame_limit_kills_actual_owner_test() ->
    {Owner, Relay} = start(3),
    Monitor = erlang:monitor(process, Owner),
    ?assertEqual(0, hornbeam_ws_handler:output([Owner, Relay,
        {<<"websocket_send">>, <<"binary">>, <<"four">>}])),
    receive {'DOWN', Monitor, process, Owner, killed} -> ok
    after 1000 -> exit(Owner, kill), error(owner_retained) end,
    receive {input_closed, _} -> ok after 1000 -> error(input_retained) end.

abort_interrupts_blocked_transport_test() ->
    {Owner, Relay} = start(1024),
    Monitor = erlang:monitor(process, Owner),
    call(Owner, Relay, {<<"websocket_send">>, <<"text">>, <<"blocked">>}),
    _ = entered(Owner),
    ?assertEqual(0, hornbeam_ws_handler:output([Owner, Relay, <<"abort">>])),
    result(0),
    receive {'DOWN', Monitor, process, Owner, killed} -> ok
    after 1000 -> error(owner_retained) end,
    receive {input_closed, _} -> ok after 1000 -> error(input_retained) end.

blocked_transport_deadline_test_() ->
    {timeout, 8, fun() ->
        {Owner, Relay} = start(1024),
        Monitor = erlang:monitor(process, Owner),
        Started = erlang:monotonic_time(millisecond),
        call(Owner, Relay, {<<"websocket_send">>, <<"text">>, <<"timeout">>}),
        _ = entered(Owner),
        receive {output_result, Value} -> ?assertEqual(0, Value)
        after 6500 -> exit(Owner, kill), error(no_deadline) end,
        Elapsed = erlang:monotonic_time(millisecond) - Started,
        ?assert(Elapsed >= 4900),
        ?assert(Elapsed < 6500),
        receive {'DOWN', Monitor, process, Owner, killed} -> ok
        after 1000 -> error(owner_retained) end,
        receive {input_closed, _} -> ok after 1000 -> error(input_retained) end
    end}.
