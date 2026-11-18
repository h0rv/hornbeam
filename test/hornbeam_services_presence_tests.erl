-module(hornbeam_services_presence_tests).
-include_lib("eunit/include/eunit.hrl").

presence_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun snapshots_and_acknowledged_close/0,
        fun initial_snapshot_overflow/0,
        fun bounded_pending_diffs/0,
        fun slow_reader_overflow/0,
        fun owner_death/0,
        fun service_death/0,
        fun presence_engine_death/0,
        fun canceled_and_invalid_opens/0
    ]}.

setup() ->
    {ok, State} = hornbeam_state:start_link(), unlink(State),
    {ok, Pubsub} = hornbeam_pubsub:start_link(), unlink(Pubsub),
    {ok, Presence} = hornbeam_presence:start_link(), unlink(Presence),
    {ok, Services} = hornbeam_services:start_link(), unlink(Services),
    {State, Pubsub}.

cleanup({State, Pubsub}) ->
    lists:foreach(fun(Name) ->
        case whereis(Name) of undefined -> ok; Pid -> gen_server:stop(Pid) end
    end, [hornbeam_services, hornbeam_presence]),
    gen_server:stop(Pubsub), gen_server:stop(State),
    gen_server:stop(hornbeam_pubsub_scope).

value(Action, Args) ->
    {<<"ok">>, Value} = hornbeam_services:call(Action, Args), Value.
open(Owner, Token, Key, Payload, Budget) ->
    value(<<"presence">>, [Owner, <<Token:128>>, <<"online">>, Key, Payload, Budget]).
close(Owner, Token) -> value(<<"unsubscribe">>, [Owner, <<Token:128>>]).
status(Owner, Token) -> value(<<"subscription_status">>, [Owner, <<Token:128>>]).

snapshot(Channel, Expected) ->
    Deadline = erlang:monotonic_time(millisecond) + 3000,
    await(fun() ->
        case py_nif:channel_try_receive(Channel) of
            {ok, {<<"presence">>, Rows}} ->
                case [{Key, Bytes} || {Key, _, {bytes, Bytes}} <- Rows] of
                    Expected -> {ok, Rows};
                    _ -> retry
                end;
            {error, empty} -> retry;
            Other -> error({unexpected_channel_result, Other})
        end
    end, Deadline).

eventually(Predicate) ->
    await(fun() -> case Predicate() of true -> {ok, ok}; false -> retry end end,
          erlang:monotonic_time(millisecond) + 3000).
await(Fun, Deadline) ->
    case Fun() of
        {ok, Value} -> Value;
        retry ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true -> receive after 1 -> await(Fun, Deadline) end;
                false -> error(presence_timeout)
            end
    end.

snapshots_and_acknowledged_close() ->
    Bytes = <<0, 255, "hello">>,
    A = open(self(), 1, <<"same-user">>, Bytes, 65536),
    [_] = snapshot(A, [{<<"same-user">>, Bytes}]),
    B = open(self(), 2, <<"same-user">>, <<>>, 65536),
    Rows = snapshot_rows(A, 2),
    ?assertEqual([<<>>, Bytes], lists:sort([P || {_, _, {bytes, P}} <- Rows])),
    ?assertEqual(2, length(lists:usort([Ref || {_, Ref, _} <- Rows]))),
    ?assertEqual(Rows, snapshot_rows(B, 2)),
    ?assertEqual(B, open(self(), 2, <<"ignored">>, <<"ignored">>, 65536)),
    true = close(self(), 2),
    ?assertEqual(1, length(maps:get(<<"metas">>, maps:get(<<"same-user">>, hornbeam_presence:list(<<"online">>))))),
    _ = snapshot(A, [{<<"same-user">>, Bytes}]),
    true = close(self(), 1),
    ?assertEqual(#{}, hornbeam_presence:list(<<"online">>)),
    ?assertEqual(<<"closed">>, value(<<"subscription_finalize">>, [self(), <<1:128>>])).

snapshot_rows(Channel, Count) ->
    await(fun() ->
        case py_nif:channel_try_receive(Channel) of
            {ok, {<<"presence">>, Rows}} when length(Rows) =:= Count -> {ok, Rows};
            _ -> retry
        end
    end, erlang:monotonic_time(millisecond) + 3000).

initial_snapshot_overflow() ->
    Ch = open(self(), 1, <<"large">>, binary:copy(<<$x>>, 2048), 1024),
    eventually(fun() -> status(self(), 1) =:= <<"overflow">> end),
    ?assertMatch(#{closed := true, size := 0}, py_channel:info(Ch)),
    eventually(fun() -> hornbeam_presence:list(<<"online">>) =:= #{} end).

bounded_pending_diffs() ->
    Ch = open(self(), 1, <<"one">>, <<>>, 1024), _ = snapshot_rows(Ch, 1),
    [Guardian] = hornbeam_pubsub:get_members(<<"online">>),
    true = erlang:suspend_process(Guardian),
    try
        lists:foreach(fun(_) ->
            1 = hornbeam_pubsub:publish_local(<<"online">>, {presence_diff, #{}})
        end, lists:seq(1, 10000)),
        ?assertEqual({messages, [presence_changed]}, process_info(Guardian, messages)),
        ?assertMatch(#{size := 0}, py_channel:info(Ch))
    after erlang:resume_process(Guardian) end,
    _ = snapshot_rows(Ch, 1), ok.

slow_reader_overflow() ->
    Ch = open(self(), 1, <<"one">>, binary:copy(<<$x>>, 256), 1024),
    fill(Ch),
    eventually(fun() -> status(self(), 1) =:= <<"overflow">> end),
    ?assertMatch(#{closed := true}, py_channel:info(Ch)),
    eventually(fun() -> hornbeam_presence:list(<<"online">>) =:= #{} end).

fill(Channel) ->
    case py_channel:info(Channel) of
        #{closed := true} -> ok;
        #{size := Before} ->
            _ = hornbeam_pubsub:publish_local(<<"online">>, {presence_diff, #{}}),
            eventually(fun() ->
                #{size := Size, closed := Closed} = py_channel:info(Channel),
                Closed orelse Size > Before
            end),
            fill(Channel)
    end.

owner_death() ->
    Owner = spawn(fun() -> receive stop -> ok end end),
    Ch = open(Owner, 1, <<"one">>, <<>>, 1024), _ = snapshot_rows(Ch, 1),
    exit(Owner, kill),
    eventually(fun() -> maps:get(closed, py_channel:info(Ch)) end),
    eventually(fun() -> hornbeam_presence:list(<<"online">>) =:= #{} end),
    ?assertEqual(0, value(<<"subscription_count">>, [])).

service_death() ->
    Ch = open(self(), 1, <<"one">>, <<>>, 1024), _ = snapshot_rows(Ch, 1),
    exit(whereis(hornbeam_services), kill),
    eventually(fun() -> maps:get(closed, py_channel:info(Ch)) end),
    eventually(fun() -> hornbeam_presence:list(<<"online">>) =:= #{} end).

presence_engine_death() ->
    Ch = open(self(), 1, <<"one">>, <<>>, 1024), _ = snapshot_rows(Ch, 1),
    exit(whereis(hornbeam_presence), kill),
    eventually(fun() -> maps:get(closed, py_channel:info(Ch)) end),
    eventually(fun() -> value(<<"subscription_count">>, []) =:= 0 end).

canceled_and_invalid_opens() ->
    true = close(self(), 1),
    ?assertEqual({<<"error">>, <<"closed">>}, hornbeam_services:call(<<"presence">>,
        [self(), <<1:128>>, <<"online">>, <<"one">>, <<>>, 1024])),
    ?assertEqual({<<"error">>, <<"invalid_arguments">>}, hornbeam_services:call(<<"presence">>,
        [self(), <<2:128>>, <<"online">>, <<>>, <<>>, 1024])),
    ?assertEqual(#{}, hornbeam_presence:list(<<"online">>)).
