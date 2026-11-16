-module(hornbeam_ws_output_flow_tests).
-include_lib("eunit/include/eunit.hrl").
-export([send/2, send/3, abort/1, activate/1, close/1, classify/2]).

send(Observer, Data) -> send(Observer, Data, 5000).
send(Observer, Data, Timeout) ->
    Observer ! {sending, self(), iolist_to_binary(Data)},
    receive {release, Result} -> Result
    after Timeout -> {error, timeout}
    end.
abort(_) -> ok.
activate(Observer) -> Observer ! {activated, self()}, ok.
close(_) -> ok.
classify(_, _) -> ignore.

start(Max) -> start(Max, ?MODULE, self()).

start(Max, Transport, Handle) ->
    persistent_term:put(ws_output_observer, self()),
    {ok, Owner} = ws_session:start(#{transport => Transport, handle => Handle,
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

frame_limit_aborts_actual_owner_test() ->
    {Owner, Relay} = start(3),
    Monitor = erlang:monitor(process, Owner),
    ?assertEqual(0, hornbeam_ws_handler:output([Owner, Relay,
        {<<"websocket_send">>, <<"binary">>, <<"four">>}])),
    receive {'DOWN', Monitor, process, Owner, normal} -> ok
    after 1000 -> exit(Owner, kill), error(owner_retained) end,
    receive {input_closed, _} -> ok after 1000 -> error(input_retained) end.

abort_awaits_bounded_transport_cleanup_test_() ->
    {timeout, 8, fun() ->
    {Owner, Relay} = start(1024),
    Monitor = erlang:monitor(process, Owner),
    call(Owner, Relay, {<<"websocket_send">>, <<"text">>, <<"blocked">>}),
    _ = entered(Owner),
    ?assertEqual(0, hornbeam_ws_handler:output([Owner, Relay, <<"abort">>])),
    result(0),
    receive {'DOWN', Monitor, process, Owner, {shutdown, {transport_error, timeout}}} -> ok
    after 1000 -> error(owner_retained) end,
    receive {input_closed, _} -> ok after 1000 -> error(input_retained) end
    end}.

tcp_relay_deadline_releases_socket_and_owner_test_() ->
    {timeout, 9, fun() ->
        {ok, Listener} = gen_tcp:listen(0, [binary, {active, false}, {recbuf, 1024}]),
        {ok, {_, Port}} = inet:sockname(Listener),
        {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port,
            [binary, {active, false}, {recbuf, 1024}]),
        {ok, Socket} = gen_tcp:accept(Listener),
        ok = inet:setopts(Socket, [{sndbuf, 1024}]),
        {Owner, Relay} = start(16777216, ws_transport_gen_tcp, Socket),
        ok = gen_tcp:controlling_process(Socket, Owner),
        Ref = monitor(process, Owner),
        RelayRef = monitor(process, Relay),
        try
            ws_session:activate(Owner),
            Message = {<<"websocket_send">>, <<"binary">>, binary:copy(<<0>>, 8388608)},
            ?assertEqual(1, hornbeam_ws_handler:output([Owner, Relay, Message])),
            T0 = erlang:monotonic_time(millisecond),
            ?assertEqual(0, hornbeam_ws_handler:output([Owner, Relay, Message])),
            receive {'DOWN', Ref, process, Owner, _} -> ok
            after 1000 -> error(owner_retained) end,
            receive {'DOWN', RelayRef, process, Relay, _} -> ok
            after 1000 -> error(relay_retained) end,
            Elapsed = erlang:monotonic_time(millisecond) - T0,
            io:format("Hornbeam TCP relay timeout and cleanup: ~p ms~n", [Elapsed]),
            ?assert(Elapsed < 6500),
            ?assertEqual(undefined, erlang:port_info(Socket)),
            receive {input_closed, _} -> ok after 1000 -> error(input_retained) end
        after
            exit(Owner, kill),
            exit(Socket, kill),
            gen_tcp:close(Client),
            gen_tcp:close(Listener)
        end
    end}.

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
        receive {'DOWN', Monitor, process, Owner, {shutdown, {transport_error, timeout}}} -> ok
        after 1000 -> error(owner_retained) end,
        receive {input_closed, _} -> ok after 1000 -> error(input_retained) end
    end}.
