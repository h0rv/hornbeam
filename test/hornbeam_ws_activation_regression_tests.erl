%% Real ws_session state machine/codec with transport and native task doubles.
%% Run in a fresh VM: test/ws_activation_stubs must not shadow a live runtime.
-module(hornbeam_ws_activation_regression_tests).
-include_lib("eunit/include/eunit.hrl").
-export([send/2, activate/1, close/1, classify/2, controlling_process/2]).

send(Observer, Data) -> Observer ! {transport_send, self(), iolist_to_binary(Data)}, ok.
activate(Observer) -> Observer ! {transport_active, self()}, ok.
close(Observer) -> Observer ! {transport_closed, self()}, ok.
classify(_Message, _Handle) -> ignore.
controlling_process(_Handle, _Pid) -> ok.

start(CloseNow) ->
    {ok, Session} = ws_session:start(#{
        transport => ?MODULE, handle => self(), role => server,
        handler => hornbeam_ws_handler, req => #{},
        handler_opts => #{session_id => term_to_binary(make_ref()),
            scope => #{<<"path">> => <<"/activation">>, <<"close_now">> => CloseNow},
            app_module => <<"double">>, app_callable => <<"app">>},
        idle_timeout => infinity, close_timeout => 100}),
    Session.

next_frame(Session) ->
    receive {transport_send, Session, Bytes} -> Bytes
    after 1000 -> error(frame_timeout)
    end.

initial_output_survives_ready_wait_test() ->
    Session = start(false),
    try
        %% Do not activate until output has traversed ready_wait itself.
        ?assertEqual(iolist_to_binary(ws_frame:encode({text, <<"immediate">>}, server)),
                     next_frame(Session)),
        ?assertEqual(iolist_to_binary(ws_frame:encode({binary, <<0, 255>>}, server)),
                     next_frame(Session)),
        {ready_wait, _} = sys:get_state(Session),
        ws_session:activate(Session),
        receive {transport_active, Session} -> ok after 1000 -> error(not_active) end,
        {open, _} = sys:get_state(Session)
    after ws_session:stop(Session)
    end.

application_close_starts_real_close_timeout_test() ->
    Session = start(true),
    Monitor = erlang:monitor(process, Session),
    _ = next_frame(Session),
    _ = next_frame(Session),
    ?assertEqual(iolist_to_binary(ws_frame:encode({close, 1008, <<"policy">>}, server)),
                 next_frame(Session)),
    %% No activation and no peer close response. A mere reply frame would
    %% leave ready_wait/open forever instead of entering this state.
    {closing, _} = sys:get_state(Session),
    receive {'DOWN', Monitor, process, Session, normal} -> ok
    after 1000 -> ws_session:stop(Session), error(close_timeout_not_armed)
    end.

native_direct_close_uses_transport_command_test() ->
    Session = start(false),
    _ = next_frame(Session),
    _ = next_frame(Session),
    ws_session:activate(Session),
    receive {transport_active, Session} -> ok after 1000 -> error(not_active) end,
    Monitor = erlang:monitor(process, Session),
    Session ! {websocket_close, 1000, <<>>},
    ?assertEqual(iolist_to_binary(ws_frame:encode({close, 1000, <<>>}, server)),
                 next_frame(Session)),
    {closing, _} = sys:get_state(Session),
    receive {'DOWN', Monitor, process, Session, normal} -> ok
    after 1000 -> ws_session:stop(Session), error(close_timeout_not_armed)
    end.
