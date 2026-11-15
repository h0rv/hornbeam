%% Real pg membership and bounded native channels; no timing assumptions.
-module(hornbeam_services_retirement_tests).
-include_lib("eunit/include/eunit.hrl").

retirement_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun application_owner_churn/0,
        fun unobserved_closes_hold_quota_until_finalized/0,
        fun terminal_status_survives_until_retirement/0,
        fun canceled_open_cannot_be_retired/0,
        fun bounded_lifecycle_interleavings/0
    ]}.

setup() ->
    {ok, Storage} = hornbeam_state:start_link(), unlink(Storage),
    {ok, Pubsub} = hornbeam_pubsub:start_link(), unlink(Pubsub),
    {ok, Services} = hornbeam_services:start_link(), unlink(Services),
    {Storage, Pubsub}.
cleanup({Storage, Pubsub}) ->
    gen_server:stop(hornbeam_services),
    gen_server:stop(Pubsub), gen_server:stop(Storage),
    gen_server:stop(hornbeam_pubsub_scope).

value(Action, N) ->
    {<<"ok">>, Value} = hornbeam_services:call(Action, [self(), <<N:128>>]), Value.
open(N) -> hornbeam_services:call(<<"subscribe">>, [self(), <<N:128>>, <<"retire">>, 1024]).
state() -> sys:get_state(hornbeam_services).

application_owner_churn() ->
    true = register(hornbeam_sup, self()),
    try
        ?assertEqual({<<"ok">>, self()}, hornbeam_services:call(<<"owner">>, [])),
        %% Keep another subscription alive so a stale DOWN could reinsert into
        %% an existing owner, not merely encounter an absent owner map.
        {<<"ok">>, _} = open(0),
        lists:foreach(fun(N) ->
            {<<"ok">>, Channel} = open(N),
            2 = hornbeam_pubsub:publish(<<"retire">>, <<"event">>),
            ?assertEqual({ok, {<<"event">>, {bytes, <<"event">>}}},
                py_nif:channel_try_receive(Channel)),
            true = value(<<"unsubscribe">>, N),
            ?assertEqual(<<"closed">>, value(<<"subscription_finalize">>, N)),
            {state, _, _, _, Owners, Guardians} = state(),
            #{tokens := Tokens} = maps:get(self(), Owners),
            ?assertEqual({1, 1, 1},
                {map_size(Tokens), map_size(Guardians), ets:info(hornbeam_services_managed, size)}),
            ?assertEqual(1, length(hornbeam_pubsub:get_members(<<"retire">>))),
            {monitors, Monitors} = process_info(whereis(hornbeam_services), monitors),
            ?assertEqual(2, length(Monitors)),
            %% Drain the sentinel too, so it never overflows during churn.
            #{channel := Sentinel} = maps:get(<<0:128>>, Tokens),
            {ok, _} = py_nif:channel_try_receive(Sentinel)
        end, lists:seq(1, 512)),
        true = value(<<"unsubscribe">>, 0),
        <<"closed">> = value(<<"subscription_finalize">>, 0),
        {state, _, _, _, Owners, Guardians} = state(),
        ?assertEqual({#{}, #{}}, {Owners, Guardians}),
        ?assert(is_process_alive(self()))
    after unregister(hornbeam_sup) end.

unobserved_closes_hold_quota_until_finalized() ->
    lists:foreach(fun(N) ->
        {<<"ok">>, _} = open(N),
        true = value(<<"unsubscribe">>, N)
    end, lists:seq(1, 128)),
    ?assertEqual({<<"ok">>, 0}, hornbeam_services:call(<<"subscription_count">>, [])),
    %% This is the original failure: 0 active, yet the same live owner cannot
    %% open its 129th subscription until it acknowledges the retained results.
    ?assertEqual({<<"error">>, <<"token_quota">>}, open(129)),
    lists:foreach(fun(N) ->
        <<"closed">> = value(<<"subscription_status">>, N),
        <<"closed">> = value(<<"subscription_finalize">>, N)
    end, lists:seq(1, 128)),
    ?assertMatch({<<"ok">>, _}, open(129)).

terminal_status_survives_until_retirement() ->
    {<<"ok">>, Channel} = open(1),
    ?assertEqual({<<"error">>, <<"subscription_not_closed">>},
        hornbeam_services:call(<<"subscription_finalize">>, [self(), <<1:128>>])),
    ?assertMatch(#{closed := false}, py_channel:info(Channel)),
    1 = hornbeam_pubsub:publish(<<"retire">>, binary:copy(<<0>>, 2048)),
    ?assertEqual(<<"overflow">>, value(<<"subscription_status">>, 1)),
    true = value(<<"unsubscribe">>, 1),
    ?assertEqual(<<"overflow">>, value(<<"subscription_status">>, 1)),
    ?assertEqual(<<"overflow">>, value(<<"subscription_finalize">>, 1)),
    ?assertMatch(#{closed := true}, py_channel:info(Channel)),
    ?assertEqual(<<"closed">>, value(<<"subscription_finalize">>, 1)),
    ?assertEqual([], hornbeam_pubsub:get_members(<<"retire">>)),
    {state, _, _, _, Owners, Guardians} = state(),
    ?assertEqual({#{}, #{}}, {Owners, Guardians}).

canceled_open_cannot_be_retired() ->
    true = value(<<"unsubscribe">>, 1),
    <<"closed">> = value(<<"subscription_finalize">>, 1),
    Owner = self(),
    {Late, Ref} = spawn_monitor(fun() ->
        Reply = hornbeam_services:call(<<"subscribe">>,
            [Owner, <<1:128>>, <<"retire">>, 1024]),
        Owner ! {late_open_result, self(), Reply}
    end),
    receive {late_open_result, Late, Reply} ->
        ?assertEqual({<<"error">>, <<"closed">>}, Reply)
    after 1000 -> error(late_open_timeout) end,
    receive {'DOWN', Ref, process, Late, normal} -> ok
    after 1000 -> error(late_sender_timeout) end,
    {state, _, _, _, Owners, Guardians} = state(),
    #{tokens := Tokens} = maps:get(self(), Owners),
    ?assertEqual({1, 0}, {map_size(Tokens), map_size(Guardians)}).

%% Exhaust every legal ordering of one unique open, one close, consumer
%% retirement, guardian DOWN, and publisher reaping. Different senders impose
%% no ordering except: DOWN needs an open, retirement needs both replies.
%% The model distinguishes canceled opens from acknowledged opens. Invariants:
%% cancellation never resurrects; retirement never loses a terminal reason;
%% late DOWN/reap never reinserts a retired token or its monitor bookkeeping.
bounded_lifecycle_interleavings() ->
    Traces = [T || T <- permutations([open, close, retire, down, reap]),
        before(open, down, T), before(open, retire, T), before(close, retire, T)],
    ?assertEqual(25, length(Traces)),
    lists:foreach(fun(Trace) ->
        ok = hornbeam_services:reset(),
        %% An unrelated token keeps the owner metadata alive after retirement.
        {<<"ok">>, _} = open(0),
        {_Model, Guardian} = lists:foldl(fun step/2, {absent, none}, Trace),
        case Guardian of
            none -> ok;
            {Pid, _} -> erlang:resume_process(Pid)
        end
    end, Traces).

step(open, {absent, none}) ->
    {<<"ok">>, _} = open(1),
    {state, _, _, _, _, Guardians} = state(),
    [{Ref, #{pid := Pid}}] = [{R, E} || {R, #{token := <<1:128>>} = E} <- maps:to_list(Guardians)],
    true = erlang:suspend_process(Pid),
    {opened, {Pid, Ref}};
step(open, {canceled, none} = State) ->
    ?assertEqual({<<"error">>, <<"closed">>}, open(1)), State;
step(close, {Model, Guardian}) ->
    true = value(<<"unsubscribe">>, 1),
    {case Model of absent -> canceled; _ -> closed end, Guardian};
step(retire, {Model, Guardian}) ->
    <<"closed">> = value(<<"subscription_finalize">>, 1),
    Next = case Model of canceled -> canceled; closed -> retired end,
    assert_model(Next),
    {Next, Guardian};
step(down, {Model, none} = State) ->
    assert_model(Model), State;
step(down, {Model, {Pid, Ref} = Guardian}) ->
    whereis(hornbeam_services) ! {'DOWN', Ref, process, Pid, normal},
    Next = case Model of opened -> closed; _ -> Model end,
    assert_model(Next), {Next, Guardian};
step(reap, {Model, Guardian}) ->
    gen_server:cast(hornbeam_services, reap_terminal),
    assert_model(Model), {Model, Guardian}.

assert_model(Model) ->
    {state, _, _, _, Owners, Guardians} = state(),
    #{tokens := Tokens} = maps:get(self(), Owners),
    Expected = case Model of absent -> 1; retired -> 1; _ -> 2 end,
    ?assertEqual(Expected, map_size(Tokens)),
    case Model of
        retired ->
            ?assertEqual(1, map_size(Guardians)),
            ?assertEqual(1, ets:info(hornbeam_services_managed, size));
        _ -> ok
    end.

%% Also exercise the real application supervisor returned by Beam.connect(),
%% not just a test process registered under the application owner's name.
supervised_application_owner_test_() ->
    {timeout, 30, fun supervised_application_owner/0}.

supervised_application_owner() ->
    _ = application:load(hornbeam),
    Settings = [{erlang_python, num_contexts},
                {erlang_python, event_loop_pool_size}, {hornbeam, context_pool_size}],
    Previous = [{App, Key, application:get_env(App, Key)} || {App, Key} <- Settings],
    lists:foreach(fun({App, Key}) -> application:set_env(App, Key, 1) end, Settings),
    {ok, Started} = application:ensure_all_started(hornbeam),
    try
        {<<"ok">>, Owner} = hornbeam_services:call(<<"owner">>, []),
        ?assertEqual(whereis(hornbeam_sup), Owner),
        lists:foreach(fun(N) ->
            Args = [Owner, <<N:128>>],
            {<<"ok">>, Channel} = hornbeam_services:call(<<"subscribe">>,
                Args ++ [<<"application-churn">>, 1024]),
            1 = hornbeam_pubsub:publish(<<"application-churn">>, <<"event">>),
            ?assertEqual({ok, {<<"event">>, {bytes, <<"event">>}}},
                py_nif:channel_try_receive(Channel)),
            {<<"ok">>, true} = hornbeam_services:call(<<"unsubscribe">>, Args),
            {<<"ok">>, <<"closed">>} =
                hornbeam_services:call(<<"subscription_finalize">>, Args),
            {state, _, _, _, Owners, Guardians} = state(),
            ?assertEqual({#{}, #{}}, {Owners, Guardians}),
            ?assertEqual(0, ets:info(hornbeam_services_managed, size)),
            ?assertEqual([], hornbeam_pubsub:get_members(<<"application-churn">>)),
            ?assert(is_process_alive(Owner))
        end, lists:seq(1, 512))
    after
        lists:foreach(fun application:stop/1, lists:reverse(Started)),
        lists:foreach(fun
            ({App, Key, undefined}) -> application:unset_env(App, Key);
            ({App, Key, {ok, Value}}) -> application:set_env(App, Key, Value)
        end, Previous)
    end.

before(A, B, Trace) ->
    length(lists:takewhile(fun(E) -> E =/= A end, Trace)) <
        length(lists:takewhile(fun(E) -> E =/= B end, Trace)).
permutations([]) -> [[]];
permutations(Items) -> [[Item | Tail] || Item <- Items,
    Tail <- permutations(Items -- [Item])].
