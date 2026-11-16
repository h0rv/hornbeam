%% Focused tests use the real py_channel NIF, not a channel mock.
-module(hornbeam_services_mvp_tests).
-include_lib("eunit/include/eunit.hrl").

services_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun cache_bytes_and_namespaces/0,
        fun cache_expiry_and_renewal/0,
        fun cache_writer_renewal_order/0,
        fun cache_quotas/0,
        fun counters_are_atomic_and_separate/0,
        fun malformed_calls/0,
        fun duplicate_and_independent_subscriptions/0,
        fun close_before_open_traces/0,
        fun token_and_active_quotas/0,
        fun direct_delivery_preserves_ordinary_members/0,
        fun concurrent_overflow_has_one_control_message/0,
        fun bounded_queue_drains_after_overflow/0,
        fun terminal_reason_is_not_overwritten/0,
        fun invalid_public_payload_closes_native_waiter/0,
        fun owner_death_closes_native_waiter/0,
        fun guardian_death_closes_native_waiter/0,
        fun service_death_closes_native_waiter/0,
        fun application_owner/0
    ]}.

setup() ->
    {ok, State} = hornbeam_state:start_link(), unlink(State),
    {ok, Pubsub} = hornbeam_pubsub:start_link(), unlink(Pubsub),
    {ok, Services} = hornbeam_services:start_link(), unlink(Services),
    {State, Pubsub}.
cleanup({State, Pubsub}) ->
    case whereis(hornbeam_services) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid)
    end,
    gen_server:stop(Pubsub),
    gen_server:stop(State),
    case whereis(hornbeam_pubsub_scope) of
        undefined -> ok;
        Scope -> gen_server:stop(Scope)
    end.

call(Action, Args) -> hornbeam_services:call(Action, Args).
ok_value(Action, Args) ->
    {<<"ok">>, Value} = call(Action, Args), Value.
token(N) -> <<N:128>>.
open(Owner, N, Topic, Size) ->
    ok_value(<<"subscribe">>, [Owner, token(N), Topic, Size]).
close(Owner, N) -> ok_value(<<"unsubscribe">>, [Owner, token(N)]).
status(Owner, N) -> ok_value(<<"subscription_status">>, [Owner, token(N)]).

cache_bytes_and_namespaces() ->
    Values = [<<>>, <<"valid utf8">>, <<0, 255, 128>>, <<"é"/utf8>>],
    lists:foreach(fun(B) ->
        true = ok_value(<<"cache_set">>, [<<"a:b">>, <<"c">>, B, -1]),
        ?assertEqual({bytes, B}, ok_value(<<"cache_get">>, [<<"a:b">>, <<"c">>, -1]))
    end, Values),
    true = ok_value(<<"cache_set">>, [<<"a">>, <<"b:c">>, <<"other">>, -1]),
    true = ok_value(<<"cache_clear">>, [<<"a:b">>]),
    ?assertEqual({<<"missing">>, <<>>}, call(<<"cache_get">>, [<<"a:b">>, <<"c">>, -1])),
    ?assertEqual({bytes, <<"other">>}, ok_value(<<"cache_get">>, [<<"a">>, <<"b:c">>, -1])),
    true = ok_value(<<"cache_exists">>, [<<"a">>, <<"b:c">>]),
    true = ok_value(<<"cache_delete">>, [<<"a">>, <<"b:c">>]),
    false = ok_value(<<"cache_exists">>, [<<"a">>, <<"b:c">>]),
    true = ok_value(<<"cache_delete">>, [<<"a">>, <<"b:c">>]).

cache_expiry_and_renewal() ->
    NS = <<"ttl">>, K = <<"k">>,
    true = ok_value(<<"cache_set">>, [NS, K, <<"v">>, 0]),
    ?assertEqual({<<"missing">>, <<>>}, call(<<"cache_ttl">>, [NS, K])),
    true = ok_value(<<"cache_set">>, [NS, K, <<"v">>, 30]),
    timer:sleep(40),
    ?assertEqual({<<"missing">>, <<>>}, call(<<"cache_get">>, [NS, K, 10000])),
    true = ok_value(<<"cache_set">>, [NS, K, <<"v">>, 1000]),
    {bytes, <<"v">>} = ok_value(<<"cache_get">>, [NS, K, 10000]),
    ?assert(ok_value(<<"cache_ttl">>, [NS, K]) > 1000),
    true = ok_value(<<"cache_set">>, [NS, K, <<"persistent">>, -1]),
    {bytes, <<"persistent">>} = ok_value(<<"cache_get">>, [NS, K, 0]),
    ?assertEqual(-1, ok_value(<<"cache_ttl">>, [NS, K])),
    true = ok_value(<<"cache_set">>, [NS, K, <<"v">>, 1000]),
    {bytes, <<"v">>} = ok_value(<<"cache_get">>, [NS, K, 0]),
    ?assertEqual(false, ok_value(<<"cache_exists">>, [NS, K])).

cache_writer_renewal_order() ->
    Parent = self(),
    true = ok_value(<<"cache_set">>, [<<"race">>, <<"k">>, <<"old">>, 100000]),
    Pids = [spawn_monitor(fun() ->
        {bytes, _} = ok_value(<<"cache_get">>, [<<"race">>, <<"k">>, 100000]),
        Parent ! {ready_renewer, self()},
        receive continue -> ok end,
        lists:foreach(fun(_) ->
            {bytes, <<"last">>} = ok_value(<<"cache_get">>, [<<"race">>, <<"k">>, 0])
        end, lists:seq(1, 100))
    end) || _ <- lists:seq(1, 8)],
    lists:foreach(fun({Pid, _}) ->
        receive {ready_renewer, Pid} -> ok after 1000 -> error(timeout) end
    end, Pids),
    true = ok_value(<<"cache_set">>, [<<"race">>, <<"k">>, <<"last">>, -1]),
    lists:foreach(fun({Pid, _}) -> Pid ! continue end, Pids),
    lists:foreach(fun({Pid, Ref}) ->
        receive {'DOWN', Ref, process, Pid, normal} -> ok after 5000 -> error(timeout) end
    end, Pids),
    {bytes, <<"last">>} = ok_value(<<"cache_get">>, [<<"race">>, <<"k">>, 0]),
    ?assertEqual(-1, ok_value(<<"cache_ttl">>, [<<"race">>, <<"k">>])).

cache_quotas() ->
    ?assertMatch({<<"error">>, _}, call(<<"cache_set">>,
        [<<"n">>, <<"k">>, binary:copy(<<0>>, 1048577), -1])),
    lists:foreach(fun(N) ->
        true = ok_value(<<"cache_set">>, [<<"quota">>, integer_to_binary(N), <<>>, -1])
    end, lists:seq(1, 10000)),
    ?assertEqual({<<"error">>, <<"cache_quota">>},
        call(<<"cache_set">>, [<<"quota">>, <<"extra">>, <<>>, -1])),
    true = ok_value(<<"cache_set">>, [<<"quota">>, <<"1">>, <<"replace">>, -1]),
    true = ok_value(<<"cache_clear">>, [<<"quota">>]),
    true = ok_value(<<"cache_set">>, [<<"quota">>, <<"extra">>, <<>>, -1]).

counters_are_atomic_and_separate() ->
    true = ok_value(<<"cache_set">>, [<<"n">>, <<"k">>, <<"bytes">>, -1]),
    Pids = [spawn_monitor(fun() ->
        lists:foreach(fun(_) ->
            ok_value(<<"counter_incr">>, [<<"n">>, <<"k">>, 1])
        end, lists:seq(1, 50))
    end) || _ <- lists:seq(1, 20)],
    lists:foreach(fun({Pid, Ref}) ->
        receive {'DOWN', Ref, process, Pid, normal} -> ok after 5000 -> error(timeout) end
    end, Pids),
    ?assertEqual(1000, ok_value(<<"counter_incr">>, [<<"n">>, <<"k">>, 0])),
    ?assertEqual(997, ok_value(<<"counter_incr">>, [<<"n">>, <<"k">>, -3])),
    ?assertEqual({bytes, <<"bytes">>}, ok_value(<<"cache_get">>, [<<"n">>, <<"k">>, -1])),
    true = ok_value(<<"cache_clear">>, [<<"n">>]),
    ?assertEqual(997, ok_value(<<"counter_incr">>, [<<"n">>, <<"k">>, 0])),
    Services = whereis(hornbeam_services),
    gen_server:stop(Services),
    {ok, Restarted} = hornbeam_services:start_link(), unlink(Restarted),
    ?assertEqual(997, ok_value(<<"counter_incr">>, [<<"n">>, <<"k">>, 0])).

malformed_calls() ->
    Cases = [
        {<<"cache_set">>, [<<"n">>, <<"k">>, <<>>, -2]},
        {<<"cache_get">>, [<<"n">>, <<"k">>, -2]},
        {<<"cache_set">>, [<<"n">>, <<"k">>, not_bytes, 10]},
        {<<"cache_get">>, [binary:copy(<<0>>, 257), <<"k">>, -1]},
        {<<"cache_get">>, [<<"n">>, binary:copy(<<0>>, 1025), -1]},
        {<<"subscribe">>, [self(), token(1), <<>>, 1024]},
        {<<"subscribe">>, [self(), token(1), <<255>>, 1024]},
        {<<"subscribe">>, [self(), token(1), <<"t">>, 1023]},
        {<<"subscribe">>, [self(), token(1), <<"t">>, 1048577]},
        {<<"subscribe">>, [self(), <<"short">>, <<"t">>, 1024]},
        {<<"subscribe">>, [not_a_pid, token(1), <<"t">>, 1024]},
        {<<"unsubscribe">>, [self(), <<"short">>]},
        {<<"publish">>, [<<"t">>, binary:copy(<<0>>, 65537)]},
        {<<"publish">>, [<<"t">>, invalid]},
        {<<"unknown">>, []}
    ],
    lists:foreach(fun({A, Args}) ->
        ?assertMatch({<<"error">>, _}, call(A, Args))
    end, Cases),
    ?assertMatch({<<"error">>, _}, hornbeam_services:callback([])),
    ?assertEqual({<<"ok">>, 0}, hornbeam_services:callback(["subscription_count", []])).

duplicate_and_independent_subscriptions() ->
    Owner = self(), Topic = <<"duplicates">>,
    Ch = open(Owner, 1, Topic, 1024),
    ?assertEqual(Ch, open(Owner, 1, Topic, 1024)),
    ?assertEqual(1, length(hornbeam_pubsub:get_members(Topic))),
    ?assertEqual({<<"ok">>, Ch},
        call(<<"subscribe">>, [Owner, token(1), <<"other">>, 2048])),
    ?assertEqual([], hornbeam_pubsub:get_members(<<"other">>)),
    Other = spawn(fun owner_loop/0),
    Ch2 = open(Other, 1, Topic, 1024),
    ?assertNotEqual(Ch, Ch2),
    ?assertEqual(2, ok_value(<<"publish">>, [Topic, <<>>])),
    ?assertEqual({ok, {<<"event">>, {bytes, <<>>}}}, py_nif:channel_try_receive(Ch)),
    ?assertEqual({ok, {<<"event">>, {bytes, <<>>}}}, py_nif:channel_try_receive(Ch2)),
    true = close(Owner, 1), true = close(Owner, 1),
    ?assertEqual(1, length(hornbeam_pubsub:get_members(Topic))),
    ?assertEqual(<<"closed">>, status(Owner, 1)),
    ?assertEqual(<<"open">>, status(Other, 1)),
    Other ! stop.

%% Exhaustive short lifecycle traces compare the real service with a tiny
%% reference model. The invariant is that a closed token never opens again.
close_before_open_traces() ->
    Traces = traces(5),
    lists:foreach(fun({N, Actions}) ->
        _ = lists:foldl(fun(Action, Model) ->
            case {Action, Model} of
                {open, absent} ->
                    Ch = open(self(), N, <<"traces">>, 1024), {opened, Ch};
                {open, {opened, Ch}} ->
                    ?assertEqual(Ch, open(self(), N, <<"traces">>, 1024)), Model;
                {open, closed} ->
                    ?assertEqual({<<"error">>, <<"closed">>},
                        call(<<"subscribe">>, [self(), token(N), <<"traces">>, 1024])), closed;
                {close, _} -> true = close(self(), N), closed
            end
        end, absent, Actions),
        true = close(self(), N)
    end, lists:zip(lists:seq(1, length(Traces)), Traces)),
    ?assertEqual(0, ok_value(<<"subscription_count">>, [])).
traces(0) -> [[]];
traces(N) -> [[A | Rest] || A <- [open, close], Rest <- traces(N - 1)].

token_and_active_quotas() ->
    Owner = spawn(fun owner_loop/0),
    lists:foreach(fun(N) -> true = close(Owner, N) end, lists:seq(1, 128)),
    ?assertEqual({<<"error">>, <<"token_quota">>},
        call(<<"unsubscribe">>, [Owner, token(129)])),
    ?assertEqual({<<"error">>, <<"closed">>},
        call(<<"subscribe">>, [Owner, token(1), <<"quota">>, 1024])),
    true = close(Owner, 1),
    lists:foreach(fun(N) -> open(self(), N, <<"quota">>, 1024) end, lists:seq(1, 32)),
    ?assertEqual({<<"error">>, <<"subscription_quota">>},
        call(<<"subscribe">>, [self(), token(33), <<"quota">>, 1024])),
    ?assertEqual(32, ok_value(<<"subscription_count">>, [])),
    true = close(self(), 1),
    _ = open(self(), 34, <<"quota">>, 1024),
    %% A failed attempted creation is a tombstone too, never a late leak.
    ?assertEqual({<<"error">>, <<"closed">>},
        call(<<"subscribe">>, [self(), token(33), <<"quota">>, 1024])),
    Owner ! stop.

direct_delivery_preserves_ordinary_members() ->
    Topic = <<"direct">>,
    Ch = open(self(), 1, Topic, 65536),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        hornbeam_pubsub:subscribe(Topic),
        ?assertEqual(2, hornbeam_pubsub:publish(Topic, <<"valid utf8">>)),
        receive {pubsub, Topic, <<"valid utf8">>} -> ok after 1000 -> error(timeout) end,
        ?assertEqual({ok, {<<"event">>, {bytes, <<"valid utf8">>}}},
            py_nif:channel_try_receive(Ch)),
        ?assertEqual({message_queue_len, 0}, process_info(Guardian, message_queue_len)),
        hornbeam_pubsub:unsubscribe(Topic)
    after erlang:resume_process(Guardian) end,
    true = close(self(), 1).

concurrent_overflow_has_one_control_message() ->
    Topic = <<"overflow">>, Ch = open(self(), 1, Topic, 1024),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        Pids = [spawn_monitor(fun() ->
            lists:foreach(fun(_) ->
                1 = hornbeam_pubsub:publish(Topic, binary:copy(<<0>>, 2048))
            end, lists:seq(1, 50))
        end) || _ <- lists:seq(1, 20)],
        lists:foreach(fun({Pid, Ref}) ->
            receive {'DOWN', Ref, process, Pid, normal} -> ok after 5000 -> error(timeout) end
        end, Pids),
        ?assertEqual(<<"overflow">>, status(self(), 1)),
        ?assertEqual({message_queue_len, 1}, process_info(Guardian, message_queue_len)),
        ?assertEqual({messages, [subscription_terminal]}, process_info(Guardian, messages)),
        ?assertMatch(#{closed := true, size := 0}, py_channel:info(Ch)),
        ?assertEqual(0, ok_value(<<"subscription_count">>, []))
    after erlang:resume_process(Guardian) end,
    true = close(self(), 1),
    ?assertEqual(<<"overflow">>, status(self(), 1)).

bounded_queue_drains_after_overflow() ->
    Topic = <<"drain">>, Ch = open(self(), 1, Topic, 1024),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        lists:foreach(fun(_) ->
            1 = hornbeam_pubsub:publish(Topic, binary:copy(<<$x>>, 100))
        end, lists:seq(1, 100)),
        #{size := Size, closed := true} = py_channel:info(Ch),
        ?assert(Size > 0 andalso Size =< 1024),
        ?assertEqual(<<"overflow">>, status(self(), 1)),
        ?assertEqual({message_queue_len, 1}, process_info(Guardian, message_queue_len)),
        ?assert(drain_events(Ch, 0) > 0)
    after erlang:resume_process(Guardian) end.
drain_events(Ch, Count) ->
    case py_nif:channel_try_receive(Ch) of
        {ok, {<<"event">>, {bytes, B}}} when is_binary(B), byte_size(B) =:= 100 ->
            drain_events(Ch, Count + 1);
        {error, closed} -> Count
    end.

terminal_reason_is_not_overwritten() ->
    lists:foreach(fun({N, First, Expected}) ->
        Topic = integer_to_binary(N), _ = open(self(), N, Topic, 1024),
        [Guardian] = hornbeam_pubsub:get_members(Topic),
        true = erlang:suspend_process(Guardian),
        try
            1 = hornbeam_pubsub:publish(Topic, First),
            _ = hornbeam_pubsub:publish(Topic, invalid),
            _ = hornbeam_pubsub:publish(Topic, binary:copy(<<0>>, 2048)),
            true = close(self(), N),
            ?assertEqual(Expected, status(self(), N))
        after erlang:resume_process(Guardian) end
    end, [{1, invalid, <<"invalid_payload">>},
          {2, binary:copy(<<0>>, 2048), <<"overflow">>}]).

invalid_public_payload_closes_native_waiter() ->
    Topic = <<"invalid">>, Ch = open(self(), 1, Topic, 1024),
    Waiter = native_waiter(Ch),
    ?assertEqual(1, hornbeam_pubsub:publish(Topic, #{bad => value})),
    expect_closed(Waiter),
    ?assertEqual(<<"invalid_payload">>, status(self(), 1)),
    ?assertMatch(#{closed := true}, py_channel:info(Ch)).

owner_death_closes_native_waiter() ->
    Owner = spawn(fun owner_loop/0),
    Ch = open(Owner, 1, <<"owner_death">>, 1024),
    Waiter = native_waiter(Ch),
    exit(Owner, kill), expect_closed(Waiter),
    eventually(fun() -> ok_value(<<"subscription_count">>, []) =:= 0 end),
    ?assert(lists:member(status(Owner, 1), [<<"owner_down">>, <<"closed">>])),
    true = close(Owner, 1), true = close(Owner, 1),
    ?assertEqual({<<"error">>, <<"owner_down">>},
        call(<<"subscribe">>, [Owner, token(2), <<"owner_death">>, 1024])).

guardian_death_closes_native_waiter() ->
    Topic = <<"guardian_death">>, Ch = open(self(), 1, Topic, 1024),
    Waiter = native_waiter(Ch),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    exit(Guardian, kill), expect_closed(Waiter),
    ?assertEqual(<<"closed">>, status(self(), 1)),
    ?assertEqual(0, ok_value(<<"subscription_count">>, [])).

service_death_closes_native_waiter() ->
    Ch = open(self(), 1, <<"service_death">>, 1024),
    Waiter = native_waiter(Ch),
    Service = whereis(hornbeam_services), Ref = monitor(process, Service),
    exit(Service, kill),
    receive {'DOWN', Ref, process, Service, killed} -> ok after 1000 -> error(timeout) end,
    expect_closed(Waiter),
    ?assertMatch(#{closed := true}, py_channel:info(Ch)),
    {ok, Restarted} = hornbeam_services:start_link(), unlink(Restarted),
    ?assertEqual(<<"closed">>, status(self(), 1)),
    ?assertEqual(0, ok_value(<<"subscription_count">>, [])).

application_owner() ->
    true = register(hornbeam_sup, self()),
    try ?assertEqual(self(), ok_value(<<"owner">>, []))
    after unregister(hornbeam_sup) end.

owner_loop() -> receive stop -> ok end.
native_waiter(Ch) ->
    Parent = self(),
    Pid = spawn(fun() ->
        ok = py_nif:channel_register_sync_waiter(Ch), Parent ! {ready, self()},
        receive Message -> Parent ! {waiter, self(), Message} after 5000 -> exit(timeout) end
    end),
    receive {ready, Pid} -> Pid after 1000 -> error(timeout) end.
expect_closed(Pid) ->
    receive {waiter, Pid, channel_closed} -> ok after 2000 -> error(no_native_close_notification) end.
eventually(F) -> eventually(F, 100).
eventually(F, Left) ->
    case F() of
        true -> ok;
        false when Left > 0 -> receive after 1 -> eventually(F, Left - 1) end;
        false -> error(eventual_assertion_failed)
    end.
