%% Copyright 2026 Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%%% @doc Continuous ASGI WebSocket execution, or explicit native channels.
%%% websocket_protocol => channels opts into Hornbeam's native protocol.
%%% The default is asgi: no application frame is inspected for routing.
%%% The transport upgrades before init/2, so application handshake denial,
%%% headers and subprotocol selection still require a separate transport API.
-module(hornbeam_ws_handler).
-behaviour(ws_handler).
-export([init/2, handle_in/2, handle_info/2, terminate/2, output/1]).

-define(OUTPUT_TIMEOUT, 5000).
-define(ASGI_FULLSWEEP_AFTER, 0).

-record(state, {
    session_id :: binary(),
    transport_pid :: pid(),
    mode = asgi :: asgi | channels,
    accepted = false :: boolean(),
    closing = false :: boolean(),
    subprotocol :: binary() | undefined,
    channel_pid :: pid() | undefined,
    input :: reference() | undefined,
    input_guard :: pid() | undefined,
    task_ref :: reference() | undefined,
    max_output_frame = 16777216 :: pos_integer()
}).

init(Req, Opts) ->
    Scope0 = maps:get(scope, Opts),
    Scope1 = case maps:get(peer, Req, undefined) of
        {Ip, Port} ->
            Scope0#{<<"client">> => {hornbeam_request:format_ip(Ip), Port}};
        _ -> Scope0
    end,
    Extensions = maps:get(<<"extensions">>, Scope1, #{}),
    Scope = Scope1#{<<"extensions">> => Extensions#{<<"hornbeam.owner">> => self()}},
    SessionId = maps:get(session_id, Opts),
    State = #state{session_id = SessionId, transport_pid = self(),
                   mode = maps:get(websocket_protocol, Opts, asgi)},
    hornbeam_websocket:register_session(SessionId, self()),
    case State#state.mode of
        asgi -> start_asgi(Scope, Opts, State);
        channels -> start_channels(State);
        _ ->
            hornbeam_websocket:unregister_session(SessionId),
            {stop, invalid_websocket_protocol, State}
    end.

handle_in({text, Data}, #state{mode = channels, channel_pid = Pid} = State) ->
    handle_channel_message(text, Data, Pid, State);
handle_in({binary, _Data}, #state{mode = channels} = State) ->
    %% Native channels remain text-only.
    {ok, State};
handle_in({text, Data}, State) ->
    handle_receive(text, Data, State);
handle_in({binary, Data}, State) ->
    handle_receive(binary, Data, State);
handle_in(_Frame, State) ->
    %% ws_session answers ping before invoking the handler.
    {ok, State}.

%% Direct output from the running Python task. Binary tags match erlang.send
%% conventions in hornbeam_asgi_worker. Never wait for an inbound frame.
handle_info({<<"websocket_accept">>, Subprotocol}, State) ->
    {ok, State#state{accepted = true, subprotocol = Subprotocol}};
handle_info({<<"websocket_send">>, <<"text">>, Data}, #state{closing = false} = State) ->
    {reply, {text, Data}, State};
handle_info({<<"websocket_send">>, <<"binary">>, Data}, #state{closing = false} = State) ->
    {reply, {binary, Data}, State};
handle_info({<<"websocket_close">>, Code, Reason}, State) ->
    close_reply(Code, Reason, State);
handle_info({<<"websocket_broadcast">>, Topic, Message}, State) ->
    hornbeam_pubsub:publish(Topic, Message),
    {ok, State};
handle_info({async_result, Ref, Result}, #state{task_ref = Ref} = State) ->
    close_input(State),
    State1 = State#state{input = undefined, input_guard = undefined, task_ref = undefined},
    case {State1#state.closing, Result} of
        {true, _} -> {ok, State1};
        {false, {ok, _}} -> close_reply(1000, <<>>, State1);
        {false, {error, Reason}} ->
            error_logger:error_msg("WebSocket task failed: ~p~n", [Reason]),
            close_reply(1011, <<"Application error">>, State1)
    end;
%% Preserve direct output from native channels and existing pubsub callers.
handle_info({websocket_send, text, Data}, #state{closing = false} = State) ->
    {reply, {text, Data}, State};
handle_info({websocket_send, binary, Data}, #state{closing = false} = State) ->
    {reply, {binary, Data}, State};
handle_info({websocket_close, Code, Reason}, State) ->
    close_reply(Code, Reason, State);
handle_info({websocket_close, Code}, State) ->
    close_reply(Code, <<>>, State);
handle_info({pubsub, _Topic, Message}, #state{mode = asgi, closing = false} = State)
  when is_map(Message) ->
    {reply, {text, json:encode(Message)}, State};
handle_info({pubsub, _Topic, Message}, #state{mode = asgi, closing = false} = State)
  when is_binary(Message) ->
    {reply, {text, Message}, State};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{channel_pid = Pid} = State) ->
    close_reply(1011, <<"Channel terminated">>, State#state{channel_pid = undefined});
handle_info(_Info, State) ->
    {ok, State}.

terminate(Reason, #state{session_id = SessionId, input = Input,
                         channel_pid = ChannelPid} = State) ->
    hornbeam_websocket:unregister_session(SessionId),
    case Input of
        undefined -> ok;
        _ ->
            %% Queue the peer's code before EOF. If the bounded channel is
            %% already full, EOF still wakes the independent Python pump.
            _ = py_channel:send(Input, {<<"disconnect">>, reason_to_code(Reason)}),
            close_input(State)
    end,
    case ChannelPid of
        undefined -> ok;
        _ ->
            try gen_server:stop(ChannelPid, normal, 5000)
            catch _:_ -> ok
            end
    end,
    ok;
terminate(_Reason, _State) ->
    ok.

%%% Internal functions

start_asgi(Scope, Opts, State) ->
    process_flag(fullsweep_after, ?ASGI_FULLSWEEP_AFTER),
    %% Two maximum-sized frames plus term encoding overhead. Python's local
    %% queue is also bounded; sustained backpressure closes with 1013, not blocking
    %% the socket process or silently dropping application messages.
    MaxFrame = maps:get(max_frame_size, Opts, 16777216),
    case py_channel:new(#{max_size => MaxFrame * 2 + 65536}) of
        {ok, Input} ->
            Owner = self(),
            TaskRef = make_ref(),
            RelayState = State#state{input = Input, task_ref = TaskRef,
                                     max_output_frame = MaxFrame},
            Guard = spawn_opt(fun() -> relay_output(Owner, RelayState) end,
                              [{fullsweep_after, ?ASGI_FULLSWEEP_AFTER}]),
            hornbeam_websocket:register_task(Guard),
            State1 = State#state{input = Input, input_guard = Guard,
                                 task_ref = TaskRef},
            case submit_asgi(Scope, Opts, State1) of
                {ok, State2} ->
                    %% Keep the legacy path-based pubsub API in ASGI mode.
                    {bytes, Path} = maps:get(<<"raw_path">>, Scope,
                        {bytes, maps:get(<<"path">>, Scope, <<>>)}),
                    case path_to_topic(Path) of
                        undefined -> ok;
                        Topic -> hornbeam_pubsub:subscribe_path(Topic)
                    end,
                    {ok, State2};
                {error, Reason} ->
                    close_input(State1),
                    Guard ! {async_result, TaskRef, {ok, not_submitted}},
                    hornbeam_websocket:unregister_session(State#state.session_id),
                    error_logger:error_msg("WebSocket init error: ~p~n", [Reason]),
                    {stop, ws_init_failed, State1}
            end;
        {error, Reason} ->
            hornbeam_websocket:unregister_session(State#state.session_id),
            {stop, {ws_input_failed, Reason}, State}
    end.

submit_asgi(Scope, Opts, #state{input = Input, session_id = SessionId,
                               input_guard = Relay, task_ref = TaskRef} = State) ->
    try
        {ok, LoopRef} = hornbeam_lifespan:get_loop(maps:get(mount_id, Opts, undefined)),
        ok = py_nif:submit_task(LoopRef, Relay, TaskRef,
            <<"hornbeam_websocket_runner">>, <<"handle_websocket">>,
            [self(), maps:get(app_module, Opts), maps:get(app_callable, Opts),
             Scope, SessionId, Input, Relay], #{}),
        {ok, State#state{task_ref = TaskRef}}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

output([Owner, Relay, <<"abort">>]) when is_pid(Owner), is_pid(Relay) ->
    _ = ws_session:abort(Owner),
    0;
output([Owner, Relay, Message]) when is_pid(Owner), is_pid(Relay) ->
    try gen_server:call(Relay, {output, Message}, ?OUTPUT_TIMEOUT + 1000)
    catch
        exit:_ ->
            _ = ws_session:abort(Owner),
            0
    end.

relay_output(Owner, State) ->
    Ref = erlang:monitor(process, Owner),
    try relay_output(Owner, Ref, State)
    after hornbeam_websocket:unregister_task(self()) end.

relay_output(Owner, Ref, #state{input = Input, task_ref = TaskRef} = State) ->
    receive
        shutdown ->
            py_channel:close(Input),
            ws_session:close(Owner, 1001),
            erlang:demonitor(Ref, [flush]),
            drain_output(TaskRef);
        {'DOWN', Ref, process, Owner, _} ->
            py_channel:close(Input),
            hornbeam_websocket:unregister_session(State#state.session_id),
            drain_output(TaskRef);
        stop ->
            erlang:demonitor(Ref, [flush]),
            py_channel:close(Input),
            drain_output(TaskRef);
        {'$gen_call', From, {output, Message}} ->
            case relay_send(Owner, Message, State) of
                {ok, State1} ->
                    gen_server:reply(From, 1),
                    relay_output(Owner, Ref, State1);
                {error, _} ->
                    _ = ws_session:abort(Owner),
                    py_channel:close(Input),
                    hornbeam_websocket:unregister_session(State#state.session_id),
                    gen_server:reply(From, 0),
                    drain_output(TaskRef)
            end;
        Info ->
            case relay_send(Owner, Info, State) of
                {ok, State1} ->
                    case Info of
                        {async_result, TaskRef, _} ->
                            erlang:demonitor(Ref, [flush]),
                            ok;
                        _ -> relay_output(Owner, Ref, State1)
                    end;
                {error, _} ->
                    _ = ws_session:abort(Owner),
                    py_channel:close(Input),
                    hornbeam_websocket:unregister_session(State#state.session_id),
                    drain_output(TaskRef)
            end
    end.

drain_output(TaskRef) ->
    drain_output(TaskRef, erlang:monotonic_time(millisecond) + ?OUTPUT_TIMEOUT).

drain_output(TaskRef, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {async_result, TaskRef, {ok, _}} -> ok;
        {async_result, TaskRef, {error, Reason}} ->
            error_logger:error_msg("WebSocket task failed during cleanup: ~p~n", [Reason]);
        {'$gen_call', From, {output, _}} ->
            gen_server:reply(From, 0),
            drain_output(TaskRef, Deadline)
    after Remaining ->
        logger:warning("WebSocket application cleanup timed out")
    end.

relay_send(_Owner, {<<"websocket_send">>, _, Data},
           #state{max_output_frame = Max}) when byte_size(Data) > Max ->
    {error, frame_too_large};
relay_send(_Owner, {<<"websocket_send">>, _, _}, #state{closing = true}) ->
    {error, closing};
relay_send(_Owner, {<<"websocket_accept">>, _}, #state{closing = true}) ->
    {error, closing};
relay_send(Owner, Info, State) ->
    case handle_info(Info, State) of
        {ok, Next = #state{closing = true}} when State#state.closing =:= false ->
            case ws_session:send_sync(Owner, [], ?OUTPUT_TIMEOUT) of
                {error, closing} -> {ok, Next};
                {error, _} = Error -> Error;
                ok -> {error, close_not_started}
            end;
        {ok, Next} -> {ok, Next};
        {reply, Frames, Next} ->
            case ws_session:send_sync(Owner, Frames, ?OUTPUT_TIMEOUT) of
                ok -> {ok, Next};
                {error, _} = Error -> Error
            end
    end.

close_input(#state{input = undefined}) -> ok;
close_input(#state{input = Input, input_guard = Guard}) ->
    py_channel:close(Input),
    case Guard of
        undefined -> ok;
        _ -> Guard ! stop
    end,
    ok.

handle_receive(_Type, _Data, #state{closing = true} = State) ->
    {ok, State};
handle_receive(_Type, _Data, #state{input = undefined} = State) ->
    close_reply(1011, <<"Application finished">>, State);
handle_receive(Type, Data, #state{input = Input} = State) ->
    %% Force bytes even for valid UTF-8 binary frames. Text is decoded once
    %% in Python; arbitrary five-item JSON arrays remain application input.
    case py_channel:send(Input, {atom_to_binary(Type), {bytes, Data}}) of
        ok -> {ok, State};
        busy ->
            close_input(State),
            close_reply(1013, <<"Application input channel full">>, State);
        {error, _} ->
            close_reply(1011, <<"Application finished">>, State)
    end.

close_reply(_Code, _Reason, #state{closing = true} = State) ->
    {ok, State};
close_reply(Code, Reason, #state{transport_pid = Owner} = State) ->
    %% A reply frame alone leaves ws_session open with close_sent=false.
    %% Use its close command to enter closing and arm the close timeout.
    %% EOF lets Python finish cooperatively, with its own bounded grace period.
    close_input(State#state{input_guard = undefined}),
    ws_session:close(Owner, Code, Reason),
    {ok, State#state{closing = true}}.

path_to_topic(<<"/", Rest/binary>>) ->
    binary:replace(Rest, <<"/">>, <<":">>, [global]);
path_to_topic(_) -> undefined.

reason_to_code({remote, Code, _Reason}) when is_integer(Code) -> Code;
reason_to_code(remote) -> 1005;
reason_to_code(normal) -> 1000;
reason_to_code(shutdown) -> 1001;
reason_to_code(timeout) -> 1002;
reason_to_code({transport_error, _}) -> 1006;
reason_to_code(_) -> 1006.

%%% Explicit native channel protocol (never starts an ASGI application).

start_channels(#state{session_id = SessionId} = State) ->
    case hornbeam_channel:start_link(self(), SessionId) of
        {ok, Pid} ->
            _ = erlang:monitor(process, Pid),
            {ok, State#state{channel_pid = Pid, accepted = true}};
        {error, Reason} ->
            hornbeam_websocket:unregister_session(SessionId),
            error_logger:error_msg("Failed to start channel process: ~p~n", [Reason]),
            {stop, ws_channel_failed, State}
    end.

handle_channel_message(text, Data, Pid, State) ->
    try json:decode(Data) of
        Message when is_list(Message), length(Message) =:= 5 ->
            hornbeam_channel:handle_message(Pid, Message),
            {ok, State};
        _ -> close_reply(1007, <<"Invalid channel message">>, State)
    catch
        _:_ -> close_reply(1007, <<"Invalid JSON">>, State)
    end.
