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

%%% @doc ASGI handler.
%%%
%%% Requests run in their own livery request process. The Python task is
%%% submitted to the event loop pool with this process's pid; response
%%% events (full_response/start_response/chunk/fin/error) arrive as messages,
%%% request-body chunks as `{livery_body, Ref, _}' messages, and client
%%% disconnects as `{livery_disconnect, Ref, Reason}' - all handled by
%%% one selective receive, first as a stream_deferred resolver (choosing
%%% status and headers) and then as the stream producer.
%%%
%%% @end
-module(hornbeam_asgi).

-export([handle/2, output/1, await_exit/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Threshold for passing the request body inline (64KB)
%% Bodies smaller than this are read synchronously and passed as a binary
-define(ASGI_BODY_BUFFER_THRESHOLD, 65536).

%%% ============================================================================
%%% Entry point (called from hornbeam_handler)
%%% ============================================================================

-spec handle(livery_req:req(), map()) -> livery_resp:resp().
handle(Req, HandlerState) ->
    ReqInfo = hornbeam_handler:build_request_info(Req, HandlerState),
    ReqInfo1 = hornbeam_http_hooks:run_on_request(ReqInfo),

    AppModule = maps:get(app_module, HandlerState),
    AppCallable = maps:get(app_callable, HandlerState),
    TimeoutMs = maps:get(timeout, HandlerState, 30000),

    %% Build ASGI scope
    Scope = hornbeam_request:build_asgi_scope(Req, HandlerState),

    %% Body exists if: Content-Length > 0, or Transfer-Encoding is present
    Method = livery_req:method(Req),
    ContentLength = hornbeam_handler:get_content_length(Req),
    TransferEncoding = livery_req:header(<<"transfer-encoding">>, Req),
    HasBody = has_request_body(Method, ContentLength, TransferEncoding),

    %% Small bodies with known Content-Length are read synchronously and
    %% passed directly; larger/streaming bodies flow through a byte
    %% channel fed by this process's receive loop
    {BodyArg, BodyRef} = case {HasBody, livery_req:body(Req)} of
        {false, _} ->
            {empty, undefined};
        {true, empty} ->
            {empty, undefined};
        {true, {buffered, Data}} ->
            {{body, {bytes, iolist_to_binary(Data)}}, undefined};
        {true, {stream, Reader}} when is_integer(ContentLength),
                                      ContentLength =< ?ASGI_BODY_BUFFER_THRESHOLD ->
            case livery_body:read_all(Reader, TimeoutMs, infinity) of
                {ok, Body, _Reader1} ->
                    {{body, {bytes, Body}}, undefined};
                {error, _Reason, _Reader1} ->
                    %% Truncated client body: hand Python what we have
                    {{body, {bytes, <<>>}}, undefined}
            end;
        {true, {stream, Reader}} ->
            {ok, Ch} = py_byte_channel:new(),
            {{channel, Ch}, livery_body:ref(Reader)}
    end,

    {ok, LoopRef} = hornbeam_lifespan:get_loop(maps:get(mount_id, HandlerState, undefined)),
    ok = py_nif:submit_task(LoopRef, self(), undefined,
        <<"hornbeam_asgi_worker">>, <<"handle_asgi">>,
        [self(), AppModule, AppCallable, Scope, BodyArg], #{}),

    St = #{
        req => Req,
        req_info => ReqInfo1,
        channel => body_channel(BodyArg),
        body_ref => BodyRef,
        timeout_ms => TimeoutMs
    },
    livery_resp:stream_deferred(fun() -> wait_response(St) end).

%%% ============================================================================
%%% Resolver: wait for the response head while pumping the request body
%%% ============================================================================

wait_response(#{req_info := ReqInfo, timeout_ms := TimeoutMs} = St) ->
    BodyRef = pump_ref(St),
    receive
        {livery_body, BodyRef, {data, Data}} ->
            case push_to_channel(St, Data) of
                ok -> wait_response(St);
                {error, interrupted} -> wait_response(St#{body_ref := undefined});
                {error, Reason} -> hornbeam_handler:error_decision(Reason, ReqInfo)
            end;
        {livery_body, BodyRef, _EofOrError} ->
            %% eof/trailers/reset/error all end the request body; a reset
            %% surfaces to Python as EOF on its body reads
            maybe_close_channel(maps:get(channel, St)),
            wait_response(St#{body_ref := undefined});
        {hornbeam_http_output, From, Ref,
         {<<"start_response">>, StatusCode, Headers, FirstChunk}} ->
            start_stream(StatusCode, Headers, FirstChunk, {From, Ref}, St);
        {<<"full_response">>, StatusCode, Headers, Body} ->
            cleanup(St),
            {Status, SafeHeaders} = response_head(StatusCode, Headers),
            {full, Status, SafeHeaders, hornbeam_handler:to_binary(Body)};
        {<<"early_hints">>, Headers} ->
            %% Best-effort: h3 (and HTTP/1.0 clients) have no interim
            %% responses, and livery answers {error, unsupported}
            _ = livery_req:inform(103, hornbeam_handler:convert_headers(Headers),
                                  maps:get(req, St)),
            wait_response(St);
        <<"fin">> ->
            cleanup(St),
            hornbeam_handler:error_decision(response_finished, ReqInfo);
        {<<"error">>, Reason} ->
            cleanup(St),
            hornbeam_handler:error_decision(Reason, ReqInfo);
        {async_result, _Ref, {ok, _}} ->
            wait_response(St);
        {async_result, _Ref, {error, Reason}} ->
            cleanup(St),
            hornbeam_handler:error_decision(Reason, ReqInfo);
        {livery_disconnect, _Ref, _Reason} ->
            %% Client gone before the response started; the emit of this
            %% decision fails on the closed stream, which livery treats
            %% as a peer disconnect
            cleanup(St),
            {full, 500, [], <<>>}
    after TimeoutMs ->
        cleanup(St),
        hornbeam_handler:error_decision(timeout, ReqInfo)
    end.

%%% ============================================================================
%%% Producer: stream chunks to the client
%%% ============================================================================

start_stream(StatusCode, Headers, FirstChunk, Ack, St) ->
    {Status, SafeHeaders} = response_head(StatusCode, Headers),
    {stream, Status, SafeHeaders,
     fun(Emit) -> emit_chunk(Emit, FirstChunk, Ack, St) end}.

response_head(StatusCode, Headers) ->
    {hornbeam_handler:parse_status_code(StatusCode),
     hornbeam_handler:convert_headers(hornbeam_handler:filter_hop_by_hop(Headers))}.

%% Ack is the Python sender waiting in output/1, or none for a chunk sent
%% without flow control. Replying only after Emit returns is what holds a
%% streaming app to the client's read rate.
emit_chunk(Emit, Data, Ack, St) ->
    Result = case hornbeam_handler:to_binary(Data) of
        <<>> -> ok;
        Body -> Emit(Body)
    end,
    case Result of
        ok ->
            ack(Ack, ok),
            stream_loop(Emit, St);
        {error, _} ->
            ack(Ack, closed),
            stream_done(St)
    end.

ack(none, _Result) -> ok;
ack({From, Ref}, Result) -> From ! {Ref, Result}, ok.

%% Streams have no idle timeout: SSE stays quiet between events, and the
%% stream ends when the app finishes or the client disconnects.
stream_loop(Emit, St) ->
    BodyRef = pump_ref(St),
    receive
        {livery_body, BodyRef, {data, Data}} ->
            case push_to_channel(St, Data) of
                ok -> stream_loop(Emit, St);
                {error, interrupted} -> stream_loop(Emit, St#{body_ref := undefined});
                {error, Reason} -> {error, Reason}
            end;
        {livery_body, BodyRef, _EofOrError} ->
            maybe_close_channel(maps:get(channel, St)),
            stream_loop(Emit, St#{body_ref := undefined});
        {hornbeam_http_output, From, Ref, {<<"chunk">>, Data}} ->
            emit_chunk(Emit, Data, {From, Ref}, St);
        {<<"chunk">>, Data} ->
            emit_chunk(Emit, Data, none, St);
        <<"fin">> ->
            cleanup(St),
            ok;
        {<<"early_hints">>, _} ->
            %% Too late: the final response head is already on the wire
            stream_loop(Emit, St);
        {<<"error">>, Reason} ->
            %% Response already started: truncate the stream explicitly.
            cleanup(St),
            {error, Reason};
        {async_result, _Ref, {error, Reason}} ->
            cleanup(St),
            {error, Reason};
        {async_result, _Ref, _} ->
            stream_loop(Emit, St);
        {livery_disconnect, _Ref, _Reason} ->
            stream_done(St)
    end.

%% @private
%% Stop producing: the stream will take no more data. Either the client
%% disconnected, or the stream rejects a body at all - an h2/h3 response
%% to HEAD answers `{error, invalid_stream_state}' rather than swallowing
%% the bytes the way HTTP/1.1 does. Close the byte channel either way so
%% Python's body reads hit EOF (its sends to this pid are dropped once we
%% return).
stream_done(St) ->
    cleanup(St),
    {error, closed}.

%%% ============================================================================
%%% Python callbacks (registered in hornbeam:register_python_callbacks/0)
%%% ============================================================================

%% @doc Deliver one streaming response message and wait until it is written.
%% Returns 1 once written, 0 when the client is gone.
-spec output([pid() | tuple()]) -> 0 | 1.
output([Pid, Message]) when is_pid(Pid) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {hornbeam_http_output, self(), Ref, Message},
    receive
        {Ref, Result} ->
            erlang:demonitor(Ref, [flush]),
            case Result of
                ok -> 1;
                closed -> 0
            end;
        {'DOWN', Ref, process, Pid, _} -> 0
    end.

%% @doc Return once the request process has exited: the client
%% disconnected or the response is over.
-spec await_exit([pid()]) -> 1.
await_exit([Pid]) when is_pid(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, _} -> 1
    end.

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

%% @private
%% Only pump raw body messages while a channel is live; otherwise use a
%% fresh ref that matches nothing.
pump_ref(#{body_ref := Ref}) when is_reference(Ref) -> Ref;
pump_ref(_) -> make_ref().

%% @private
body_channel({channel, Ch}) -> Ch;
body_channel(_) -> undefined.

%% @private
cleanup(St) ->
    maybe_close_channel(maps:get(channel, St)).

%% A full body channel must not trap the request process indefinitely.
%% Preserve terminal messages for the normal resolver/producer receive loop.
%% The monotonic deadline covers all retries, rather than resetting per retry.
push_to_channel(#{timeout_ms := TimeoutMs} = St, Data) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    Result = push_to_channel(St, Data, Deadline),
    case Result of
        ok -> ok;
        {error, _} -> cleanup(St), Result
    end.

push_to_channel(#{channel := Channel} = St, Data, Deadline) ->
    case py_byte_channel:send(Channel, Data) of
        ok -> ok;
        busy -> wait_body_capacity(St, Data, Deadline);
        {error, Reason} -> {error, {body_channel, Reason}}
    end.

wait_body_capacity(St, Data, Deadline) ->
    Remaining = Deadline - erlang:monotonic_time(millisecond),
    receive
        {livery_disconnect, _, _} = Msg -> body_interrupted(Msg);
        {<<"full_response">>, _, _, _} = Msg -> body_interrupted(Msg);
        <<"fin">> = Msg -> body_interrupted(Msg);
        {<<"error">>, _} = Msg -> body_interrupted(Msg);
        {async_result, _, {error, _}} = Msg -> body_interrupted(Msg)
    after max(0, min(10, Remaining)) ->
        case erlang:monotonic_time(millisecond) >= Deadline of
            true -> {error, body_channel_timeout};
            false -> push_to_channel(St, Data, Deadline)
        end
    end.

body_interrupted(Msg) ->
    self() ! Msg,
    {error, interrupted}.

-ifdef(TEST).
full_response_interrupts_body_capacity_wait_test() ->
    Response = {<<"full_response">>, 413, [], <<>>},
    self() ! Response,
    ?assertEqual({error, interrupted},
                 wait_body_capacity(#{}, <<>>, erlang:monotonic_time(millisecond))),
    receive
        Response -> ok
    after 0 ->
        ?assert(false)
    end.
-endif.

maybe_close_channel(undefined) -> ok;
maybe_close_channel(Channel) ->
    try
        case py_byte_channel:info(Channel) of
            #{closed := true} -> ok;
            _ ->
                _ = py_byte_channel:close(Channel),
                ok
        end
    catch
        _:_ -> ok
    end.

%% @private
%% Check if request has a body based on method, content-length, and transfer-encoding
%% Per HTTP spec: body exists if Content-Length > 0 OR Transfer-Encoding is present
has_request_body(_, _, TE) when TE =/= undefined -> true;  %% Transfer-Encoding present
has_request_body(_, 0, _) -> false;                         %% Content-Length: 0
has_request_body(_, CL, _) when is_integer(CL), CL > 0 -> true;  %% Content-Length > 0
has_request_body(<<"GET">>, _, _) -> false;
has_request_body(<<"HEAD">>, _, _) -> false;
has_request_body(<<"DELETE">>, _, _) -> false;
has_request_body(<<"OPTIONS">>, _, _) -> false;
has_request_body(_, undefined, undefined) -> false.         %% No CL, no TE = no body
