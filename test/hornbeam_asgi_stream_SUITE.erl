%% Real-socket ASGI streaming: SSE delivery, disconnect reaching the app,
%% and bounded buffering for clients that stop reading.
-module(hornbeam_asgi_stream_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         sse_events_arrive_as_sent/1, sse_survives_idle_gaps/1,
         idle_stream_receives_disconnect/1, busy_stream_send_fails_after_disconnect/1,
         slow_reader_bounds_buffering/1, slow_reader_disconnect_stops_app/1]).

%% The request timeout and h1 keep-alive are both shorter than the SSE
%% idle gap, so a stream that stays open proves neither applies to it.
-define(TIMEOUT_MS, 1000).
-define(MB, 1048576).

all() ->
    [sse_events_arrive_as_sent, sse_survives_idle_gaps,
     idle_stream_receives_disconnect, busy_stream_send_fails_after_disconnect,
     slow_reader_bounds_buffering, slow_reader_disconnect_stops_app].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(hornbeam),
    {ok, Socket} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    LibDir = filename:dirname(filename:dirname(code:which(hornbeam))),
    SourceDir = case file:read_link(filename:join(LibDir, "src")) of
        {ok, RelPath} ->
            filename:dirname(filename:absname(filename:join(LibDir, RelPath)));
        {error, _} -> LibDir
    end,
    ok = hornbeam:start("stream_app:application", #{
        bind => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
        worker_class => asgi,
        context_mode => worker,
        lifespan => off,
        timeout => ?TIMEOUT_MS,
        keepalive => 1,
        pythonpath => [list_to_binary(filename:join(SourceDir, "test/test_apps"))]
    }),
    [{port, Port} | Config].

end_per_suite(_Config) ->
    hornbeam:stop(),
    application:stop(hornbeam),
    ok.

sse_events_arrive_as_sent(Config) ->
    S = open(Config, <<"/sse?count=3&gap_ms=300">>),
    Start = erlang:monotonic_time(millisecond),
    Arrivals = [begin
                    recv_until(S, event(I)),
                    erlang:monotonic_time(millisecond) - Start
                end || I <- [0, 1, 2]],
    [First, Second, Third] = Arrivals,
    %% Each event is written when sent, not buffered until the stream ends.
    ?assert(First < 250, Arrivals),
    ?assert(Second - First >= 250, Arrivals),
    ?assert(Third - Second >= 250, Arrivals),
    recv_until(S, <<"0\r\n\r\n">>),
    gen_tcp:close(S).

sse_survives_idle_gaps(Config) ->
    S = open(Config, <<"/sse?count=2&gap_ms=2500">>),
    recv_until(S, event(0)),
    recv_until(S, event(1)),
    recv_until(S, <<"0\r\n\r\n">>),
    gen_tcp:close(S).

idle_stream_receives_disconnect(Config) ->
    Id = unique_id(),
    S = open(Config, <<"/watch?id=", Id/binary>>),
    recv_until(S, event(0)),
    ok = gen_tcp:close(S),
    await_stats(Config, Id, fun(#{<<"received">> := Type}) -> Type =:= <<"http.disconnect">>;
                               (_) -> false end).

busy_stream_send_fails_after_disconnect(Config) ->
    Id = unique_id(),
    S = open(Config, <<"/ticker?id=", Id/binary>>),
    recv_until(S, event(1)),
    ok = gen_tcp:close(S),
    #{<<"send_error">> := true} = await_stats(Config, Id, fun done/1).

slow_reader_bounds_buffering(Config) ->
    Id = unique_id(),
    Total = 64 * ?MB,
    S = open(Config, flood_path(Id, Total), [{recbuf, 16384}]),
    %% The client reads nothing more. Once kernel and transport buffers are
    %% full, send() must stop returning instead of queueing in the server.
    Stalled = await_stall(Config, Id),
    ?assert(Stalled < 16 * ?MB, Stalled),
    ?assertEqual(Total, read_chunked(S)),
    Final = await_stats(Config, Id, fun done/1),
    ?assertEqual(Total, maps:get(<<"sent">>, Final)),
    ?assertNot(maps:is_key(<<"send_error">>, Final)),
    gen_tcp:close(S).

slow_reader_disconnect_stops_app(Config) ->
    Id = unique_id(),
    Total = 64 * ?MB,
    S = open(Config, flood_path(Id, Total), [{recbuf, 16384}]),
    Stalled = await_stall(Config, Id),
    ok = gen_tcp:close(S),
    Final = await_stats(Config, Id, fun done/1),
    ?assertEqual(true, maps:get(<<"send_error">>, Final)),
    ?assert(maps:get(<<"sent">>, Final) < Total),
    ?assert(maps:get(<<"sent">>, Final) - Stalled < 16 * ?MB).

%%% Helpers

event(I) -> iolist_to_binary(["data: ", integer_to_list(I), "\n\n"]).

flood_path(Id, Total) ->
    iolist_to_binary(["/flood?id=", Id, "&chunk=65536&total=", integer_to_list(Total)]).

done(#{<<"done">> := true}) -> true;
done(_) -> false.

unique_id() -> integer_to_binary(erlang:unique_integer([positive])).

open(Config, Path) -> open(Config, Path, []).

open(Config, Path, Opts) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}, {packet, raw} | Opts]),
    ok = gen_tcp:send(S, [<<"GET ">>, Path, <<" HTTP/1.1\r\nHost: localhost\r\n\r\n">>]),
    Head = recv_until(S, <<"\r\n\r\n">>),
    ?assertMatch(<<"HTTP/1.1 200", _/binary>>, Head),
    S.

%% Returns everything up to and including Pattern; keeps the rest buffered
%% in the process dictionary for the next read on the same socket.
recv_until(S, Pattern) ->
    Buf = buffered(S),
    case binary:match(Buf, Pattern) of
        {Pos, Len} ->
            End = Pos + Len,
            put({buf, S}, binary:part(Buf, End, byte_size(Buf) - End)),
            binary:part(Buf, 0, End);
        nomatch ->
            {ok, Data} = gen_tcp:recv(S, 0, 5000),
            put({buf, S}, <<Buf/binary, Data/binary>>),
            recv_until(S, Pattern)
    end.

recv_exact(S, N) ->
    Buf = buffered(S),
    case Buf of
        <<Data:N/binary, Rest/binary>> ->
            put({buf, S}, Rest),
            Data;
        _ ->
            {ok, More} = gen_tcp:recv(S, 0, 5000),
            put({buf, S}, <<Buf/binary, More/binary>>),
            recv_exact(S, N)
    end.

buffered(S) ->
    case get({buf, S}) of
        undefined -> <<>>;
        Buf -> Buf
    end.

%% Decode a chunked body, returning its length.
read_chunked(S) -> read_chunked(S, 0).

read_chunked(S, Acc) ->
    Line = recv_until(S, <<"\r\n">>),
    [Hex | _] = binary:split(Line, [<<";">>, <<"\r\n">>]),
    case binary_to_integer(Hex, 16) of
        0 ->
            <<"\r\n">> = recv_until(S, <<"\r\n">>),
            Acc;
        Size ->
            _ = recv_exact(S, Size + 2),
            read_chunked(S, Acc + Size)
    end.

stats(Config, Id) ->
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_list(?config(port, Config)),
                            "/stats?id=", Id]),
    {ok, #{status := 200, body := {full, Body}}} = hornbeam_test_http:request(get, Url),
    json:decode(Body).

await_stats(Config, Id, Pred) ->
    await_stats(Config, Id, Pred, erlang:monotonic_time(millisecond) + 5000).

await_stats(Config, Id, Pred, Deadline) ->
    Stats = stats(Config, Id),
    case Pred(Stats) of
        true -> Stats;
        false ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(20),
                    await_stats(Config, Id, Pred, Deadline);
                false ->
                    ct:fail({stats_timeout, Id, Stats})
            end
    end.

%% Bytes the app had handed to send() once that count stops growing for
%% 300 ms while the client is not reading.
await_stall(Config, Id) ->
    await_stats(Config, Id, fun(#{<<"sent">> := Sent}) -> Sent > 0; (_) -> false end),
    await_stall(Config, Id, -1, erlang:monotonic_time(millisecond) + 5000).

await_stall(Config, Id, Previous, Deadline) ->
    #{<<"sent">> := Sent, <<"done">> := Done} = stats(Config, Id),
    ?assertNot(Done, {sent, Sent}),
    case Sent =:= Previous of
        true -> Sent;
        false ->
            ?assert(erlang:monotonic_time(millisecond) < Deadline, {sent, Sent}),
            timer:sleep(300),
            await_stall(Config, Id, Sent, Deadline)
    end.
