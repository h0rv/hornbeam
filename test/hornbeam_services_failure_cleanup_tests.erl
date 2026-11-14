%% Focused regressions with the real bounded channel NIF.
-module(hornbeam_services_failure_cleanup_tests).
-include_lib("eunit/include/eunit.hrl").

failure_cleanup_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun stop_clears_application_resources/0,
        fun reset_clears_accounting_and_counter_quota/0,
        fun registry_loss_never_falls_back_to_mailbox/0,
        fun stale_membership_after_reset_fails_closed/0,
        fun publisher_death_after_terminal_cas_is_reaped/0,
        fun wire_wrappers_and_boolean_acknowledgements/0
    ]}.

setup() ->
    {ok, State} = hornbeam_state:start_link(), unlink(State),
    {ok, Pubsub} = hornbeam_pubsub:start_link(), unlink(Pubsub),
    start_services(),
    {State, Pubsub}.
start_services() ->
    {ok, Services} = hornbeam_services:start_link(), unlink(Services).
cleanup({State, Pubsub}) ->
    gen_server:stop(hornbeam_services),
    gen_server:stop(Pubsub), gen_server:stop(State),
    gen_server:stop(hornbeam_pubsub_scope).
value(Action, Args) ->
    {<<"ok">>, Value} = hornbeam_services:call(Action, Args), Value.
open(Topic) -> value(<<"subscribe">>, [self(), <<1:128>>, Topic, 1024]).

stop_clears_application_resources() ->
    Topic = <<"private-stop">>, Ch = open(Topic),
    true = value(<<"cache_set">>, [<<"private">>, <<"key">>, <<"secret">>, -1]),
    41 = value(<<"counter_incr">>, [<<"private">>, <<"count">>, 41]),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        ok = hornbeam:stop(),
        ?assertMatch(#{closed := true}, py_channel:info(Ch)),
        ?assertEqual([], hornbeam_pubsub:get_members(Topic)),
        ?assertEqual({<<"missing">>, <<>>},
            hornbeam_services:call(<<"cache_get">>, [<<"private">>, <<"key">>, -1])),
        ?assertEqual(0, value(<<"subscription_count">>, [])),
        ?assertEqual(1, value(<<"counter_incr">>, [<<"private">>, <<"count">>, 1])),
        %% A new listener uses the same application owner and may reuse tokens.
        New = open(Topic),
        ?assertNotEqual(Ch, New),
        ok = hornbeam:stop(), ok = hornbeam:stop()
    after erlang:resume_process(Guardian) end.

reset_clears_accounting_and_counter_quota() ->
    true = value(<<"cache_set">>, [<<"n">>, <<"k">>, binary:copy(<<0>>, 1048576), -1]),
    lists:foreach(fun(N) ->
        1 = value(<<"counter_incr">>, [<<"n">>, integer_to_binary(N), 1])
    end, lists:seq(1, 10000)),
    ?assertEqual({<<"error">>, <<"counter_quota">>},
        hornbeam_services:call(<<"counter_incr">>, [<<"n">>, <<"extra">>, 1])),
    ok = hornbeam_state:set(unrelated, kept),
    ok = hornbeam_services:reset(),
    %% Record layout is checked only here to catch stale quota accounting.
    {state, Cache, CacheBytes, Counters, Owners, Guardians} = sys:get_state(hornbeam_services),
    ?assertEqual({0, 0, 0, #{}, #{}},
        {ets:info(Cache, size), CacheBytes, ets:info(Counters, size), Owners, Guardians}),
    ?assertEqual(kept, hornbeam_state:get(unrelated)),
    ?assertEqual(1, value(<<"counter_incr">>, [<<"n">>, <<"extra">>, 1])),
    ?assertEqual(1, value(<<"counter_incr">>, [<<"n">>, <<"1">>, 1])),
    true = value(<<"cache_set">>, [<<"n">>, <<"k">>, <<"new">>, -1]).

registry_loss_never_falls_back_to_mailbox() ->
    Topic = <<"registry-loss">>, Ch = open(Topic),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        Service = whereis(hornbeam_services), Ref = monitor(process, Service),
        exit(Service, kill),
        receive {'DOWN', Ref, process, Service, killed} -> ok
        after 1000 -> error(service_did_not_die) end,
        ?assertEqual(undefined, ets:whereis(hornbeam_services_managed)),
        flood(Topic),
        start_services(),
        flood(Topic),
        %% Only the service-monitor DOWN notification, never application data.
        ?assertMatch({messages, [{'DOWN', _, process, Service, killed}]},
            process_info(Guardian, messages)),
        ?assertMatch(#{size := 0, max_size := 1024}, py_channel:info(Ch)),
        ?assertEqual(managed, hornbeam_services:managed_delivery(Guardian, other_topic, <<"x">>))
    after erlang:resume_process(Guardian) end,
    eventually(fun() -> maps:get(closed, py_channel:info(Ch)) end).
flood(Topic) ->
    Payload = binary:copy(<<$x>>, 65536),
    lists:foreach(fun(_) -> 1 = hornbeam_pubsub:publish(Topic, Payload) end,
                  lists:seq(1, 1000)).

stale_membership_after_reset_fails_closed() ->
    Topic = <<"stale-reset">>, _ = open(Topic),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    true = erlang:suspend_process(Guardian),
    try
        ok = hornbeam_services:reset(),
        ?assertEqual([], ets:lookup(hornbeam_services_managed, Guardian)),
        ?assertEqual(true, hornbeam_pubsub:is_managed(Guardian)),
        %% Simulate a publisher that already captured the old topic member.
        lists:foreach(fun(_) ->
            ?assertEqual(managed,
                hornbeam_services:managed_delivery(Guardian, Topic, binary:copy(<<0>>, 65536)))
        end, lists:seq(1, 1000)),
        ?assertEqual({messages, [subscription_terminal]}, process_info(Guardian, messages))
    after erlang:resume_process(Guardian) end.

publisher_death_after_terminal_cas_is_reaped() ->
    Topic = <<"dead-publisher">>, Ch = open(Topic),
    [Guardian] = hornbeam_pubsub:get_members(Topic),
    [{Guardian, Topic, Ch, Flag}] = ets:lookup(hornbeam_services_managed, Guardian),
    true = erlang:suspend_process(Guardian),
    ok = sys:suspend(hornbeam_services),
    try
        Parent = self(),
        %% Inject exactly the crash boundary after terminal/4's successful CAS.
        {Publisher, Ref} = spawn_monitor(fun() ->
            ok = atomics:compare_exchange(Flag, 1, 1, 3),
            Parent ! terminal_committed,
            receive never -> ok end
        end),
        receive terminal_committed -> ok after 1000 -> error(timeout) end,
        exit(Publisher, kill),
        receive {'DOWN', Ref, process, Publisher, killed} -> ok
        after 1000 -> error(timeout) end,
        ?assertMatch(#{closed := false}, py_channel:info(Ch)),
        ok = sys:resume(hornbeam_services),
        eventually(fun() -> maps:get(closed, py_channel:info(Ch)) end),
        ?assertEqual(<<"overflow">>, value(<<"subscription_status">>, [self(), <<1:128>>])),
        %% Repeated reaps must not emit repeated control messages.
        whereis(hornbeam_services) ! reap_terminal,
        _ = sys:get_state(hornbeam_services),
        ?assertEqual({messages, [subscription_terminal]}, process_info(Guardian, messages))
    after
        sys:resume(hornbeam_services), erlang:resume_process(Guardian)
    end.

wire_wrappers_and_boolean_acknowledgements() ->
    Topic = <<"wire">>, Ch = open(Topic),
    lists:foreach(fun(B) ->
        ?assertEqual(true, value(<<"cache_set">>, [<<"n">>, <<"k">>, B, -1])),
        ?assertEqual({bytes, B}, value(<<"cache_get">>, [<<"n">>, <<"k">>, -1])),
        1 = value(<<"publish">>, [Topic, B]),
        ?assertEqual({ok, {<<"event">>, {bytes, B}}}, py_nif:channel_try_receive(Ch))
    end, [<<>>, <<"utf8">>, <<"é"/utf8>>, <<0, 255>>]),
    ?assertEqual(true, value(<<"cache_delete">>, [<<"n">>, <<"k">>])),
    ?assertEqual(true, value(<<"cache_clear">>, [<<"n">>])),
    ?assertEqual(true, value(<<"unsubscribe">>, [self(), <<1:128>>])).

eventually(F) -> eventually(F, 2000).
eventually(F, Remaining) ->
    case F() of
        true -> ok;
        false when Remaining > 0 ->
            receive after 1 -> eventually(F, Remaining - 1) end;
        false -> error(eventual_assertion_failed)
    end.
