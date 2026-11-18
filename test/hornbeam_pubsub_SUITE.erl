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

%%% @doc Tests for hornbeam pub/sub messaging.
-module(hornbeam_pubsub_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    peer_subscribers/2,
    peer_slow_subscriber/3,
    test_remote_overflow/1,
    test_node_loss/1,
    test_cross_node_publish/1,
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    test_subscribe_unsubscribe/1,
    test_publish_single_subscriber/1,
    test_publish_multiple_subscribers/1,
    test_get_members/1,
    test_multiple_topics/1,
    test_subscriber_receives_messages/1,
    test_unsubscribed_no_messages/1,
    test_binary_topic/1,
    test_tuple_topic/1,
    test_concurrent_session_registration/1,
    test_session_table_survives_registrant/1
]).

all() ->
    [{group, pubsub}].

groups() ->
    [{pubsub, [sequence], [
        test_session_table_survives_registrant,
        test_concurrent_session_registration,
        test_subscribe_unsubscribe,
        test_publish_single_subscriber,
        test_publish_multiple_subscribers,
        test_get_members,
        test_multiple_topics,
        test_subscriber_receives_messages,
        test_unsubscribed_no_messages,
        test_binary_topic,
        test_tuple_topic,
        test_cross_node_publish,
        test_remote_overflow,
        test_node_loss
    ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(hornbeam),
    Config.

end_per_suite(_Config) ->
    application:stop(hornbeam),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Clean up any subscriptions for this process
    ok.

%%% ============================================================================
%%% Test cases
%%% ============================================================================

test_session_table_survives_registrant(_Config) ->
    Owner = whereis(hornbeam_pubsub),
    ?assertEqual(Owner, ets:info(hornbeam_ws_sessions, owner)),
    Parent = self(),
    Session = term_to_binary(make_ref()),
    {Pid, Ref} = spawn_monitor(fun() ->
        ok = hornbeam_websocket:register_session(Session, self()),
        Parent ! {registered, self()},
        receive stop -> ok end
    end),
    try
        receive {registered, Pid} -> ok after 5000 -> error(registration_timeout) end,
        ?assertEqual([{Session, Pid}], ets:lookup(hornbeam_ws_sessions, Session)),
        Pid ! stop,
        receive {'DOWN', Ref, process, Pid, normal} -> ok
        after 5000 -> error(registrant_exit_timeout) end,
        ?assertEqual(Owner, ets:info(hornbeam_ws_sessions, owner)),
        ?assertEqual([{Session, Pid}], ets:lookup(hornbeam_ws_sessions, Session)),
        ok = hornbeam_websocket:unregister_session(Session),
        ?assertEqual([], ets:lookup(hornbeam_ws_sessions, Session)),
        ?assertEqual({error, session_not_found}, hornbeam_websocket:subscribe(Session, missing))
    after
        exit(Pid, kill),
        erlang:demonitor(Ref, [flush]),
        hornbeam_websocket:unregister_session(Session)
    end.

test_concurrent_session_registration(_Config) ->
    Parent = self(),
    Sessions = [begin
        Session = term_to_binary(make_ref()),
        {Pid, Ref} = spawn_monitor(fun() ->
            Parent ! {ready, self()},
            receive register -> ok end,
            ok = hornbeam_websocket:register_session(Session, self())
        end),
        {Session, Pid, Ref}
    end || _ <- lists:seq(1, 256)],
    try
        lists:foreach(fun({_, Pid, _}) ->
            receive {ready, Pid} -> ok after 5000 -> error(barrier_timeout) end
        end, Sessions),
        lists:foreach(fun({_, Pid, _}) -> Pid ! register end, Sessions),
        lists:foreach(fun({Session, Pid, Ref}) ->
            receive {'DOWN', Ref, process, Pid, Reason} -> ?assertEqual(normal, Reason)
            after 5000 -> error(registration_timeout) end,
            ?assertEqual([{Session, Pid}], ets:lookup(hornbeam_ws_sessions, Session))
        end, Sessions),
        ?assertEqual(whereis(hornbeam_pubsub), ets:info(hornbeam_ws_sessions, owner))
    after
        lists:foreach(fun({Session, Pid, Ref}) ->
            exit(Pid, kill),
            erlang:demonitor(Ref, [flush]),
            hornbeam_websocket:unregister_session(Session)
        end, Sessions)
    end.

test_subscribe_unsubscribe(_Config) ->
    Topic = test_topic_1,

    %% Initially no members
    ?assertEqual([], hornbeam_pubsub:get_members(Topic)),

    %% Subscribe
    ok = hornbeam_pubsub:subscribe(Topic),
    Members = hornbeam_pubsub:get_members(Topic),
    ?assertEqual(1, length(Members)),
    ?assert(lists:member(self(), Members)),

    %% Unsubscribe
    ok = hornbeam_pubsub:unsubscribe(Topic),
    ?assertEqual([], hornbeam_pubsub:get_members(Topic)).

test_publish_single_subscriber(_Config) ->
    Topic = test_topic_2,

    %% Subscribe
    ok = hornbeam_pubsub:subscribe(Topic),

    %% Publish
    Count = hornbeam_pubsub:publish(Topic, hello),
    ?assertEqual(1, Count),

    %% Check message received
    receive
        {pubsub, Topic, hello} -> ok
    after 1000 ->
        ct:fail("Did not receive published message")
    end,

    %% Cleanup
    ok = hornbeam_pubsub:unsubscribe(Topic).

test_publish_multiple_subscribers(_Config) ->
    Topic = test_topic_3,
    Self = self(),

    %% Spawn additional subscribers
    Pid1 = spawn_link(fun() -> subscriber_loop(Self) end),
    Pid2 = spawn_link(fun() -> subscriber_loop(Self) end),

    %% Subscribe all
    ok = hornbeam_pubsub:subscribe(Topic),
    ok = hornbeam_pubsub:subscribe(Topic, Pid1),
    ok = hornbeam_pubsub:subscribe(Topic, Pid2),

    %% Verify 3 members
    Members = hornbeam_pubsub:get_members(Topic),
    ?assertEqual(3, length(Members)),

    %% Publish
    Count = hornbeam_pubsub:publish(Topic, broadcast_msg),
    ?assertEqual(3, Count),

    %% Check all received
    receive
        {pubsub, Topic, broadcast_msg} -> ok
    after 1000 ->
        ct:fail("Main process did not receive message")
    end,

    %% Check spawned processes received
    receive
        {subscriber_received, Pid1, broadcast_msg} -> ok
    after 1000 ->
        ct:fail("Pid1 did not receive message")
    end,

    receive
        {subscriber_received, Pid2, broadcast_msg} -> ok
    after 1000 ->
        ct:fail("Pid2 did not receive message")
    end,

    %% Cleanup
    ok = hornbeam_pubsub:unsubscribe(Topic),
    ok = hornbeam_pubsub:unsubscribe(Topic, Pid1),
    ok = hornbeam_pubsub:unsubscribe(Topic, Pid2),
    Pid1 ! stop,
    Pid2 ! stop.

test_get_members(_Config) ->
    Topic = test_topic_4,

    %% Subscribe self
    ok = hornbeam_pubsub:subscribe(Topic),

    %% get_members should include self
    Members = hornbeam_pubsub:get_members(Topic),
    ?assert(lists:member(self(), Members)),

    %% get_local_members should also include self (same node)
    LocalMembers = hornbeam_pubsub:get_local_members(Topic),
    ?assert(lists:member(self(), LocalMembers)),

    %% Cleanup
    ok = hornbeam_pubsub:unsubscribe(Topic).

test_multiple_topics(_Config) ->
    Topic1 = multi_topic_1,
    Topic2 = multi_topic_2,

    %% Subscribe to both topics
    ok = hornbeam_pubsub:subscribe(Topic1),
    ok = hornbeam_pubsub:subscribe(Topic2),

    %% Verify membership in both
    ?assert(lists:member(self(), hornbeam_pubsub:get_members(Topic1))),
    ?assert(lists:member(self(), hornbeam_pubsub:get_members(Topic2))),

    %% Publish to Topic1
    hornbeam_pubsub:publish(Topic1, msg1),

    receive
        {pubsub, Topic1, msg1} -> ok
    after 1000 ->
        ct:fail("Did not receive Topic1 message")
    end,

    %% Publish to Topic2
    hornbeam_pubsub:publish(Topic2, msg2),

    receive
        {pubsub, Topic2, msg2} -> ok
    after 1000 ->
        ct:fail("Did not receive Topic2 message")
    end,

    %% Cleanup
    ok = hornbeam_pubsub:unsubscribe(Topic1),
    ok = hornbeam_pubsub:unsubscribe(Topic2).

test_subscriber_receives_messages(_Config) ->
    Topic = receive_test_topic,

    ok = hornbeam_pubsub:subscribe(Topic),

    %% Publish multiple messages
    hornbeam_pubsub:publish(Topic, msg_a),
    hornbeam_pubsub:publish(Topic, msg_b),
    hornbeam_pubsub:publish(Topic, msg_c),

    %% Should receive all in order
    receive {pubsub, Topic, msg_a} -> ok after 1000 -> ct:fail("Missing msg_a") end,
    receive {pubsub, Topic, msg_b} -> ok after 1000 -> ct:fail("Missing msg_b") end,
    receive {pubsub, Topic, msg_c} -> ok after 1000 -> ct:fail("Missing msg_c") end,

    ok = hornbeam_pubsub:unsubscribe(Topic).

test_unsubscribed_no_messages(_Config) ->
    Topic = unsub_test_topic,

    ok = hornbeam_pubsub:subscribe(Topic),
    ok = hornbeam_pubsub:unsubscribe(Topic),

    %% Publish after unsubscribe
    Count = hornbeam_pubsub:publish(Topic, should_not_receive),
    ?assertEqual(0, Count),

    %% Ensure no message received
    receive
        {pubsub, Topic, _} -> ct:fail("Should not receive message after unsubscribe")
    after 100 ->
        ok
    end.

test_binary_topic(_Config) ->
    Topic = <<"binary_topic">>,

    ok = hornbeam_pubsub:subscribe(Topic),
    hornbeam_pubsub:publish(Topic, binary_msg),

    receive
        {pubsub, <<"binary_topic">>, binary_msg} -> ok
    after 1000 ->
        ct:fail("Did not receive binary topic message")
    end,

    ok = hornbeam_pubsub:unsubscribe(Topic).

test_tuple_topic(_Config) ->
    Topic = {room, <<"chat123">>},

    ok = hornbeam_pubsub:subscribe(Topic),
    hornbeam_pubsub:publish(Topic, {chat, <<"Hello!">>}),

    receive
        {pubsub, {room, <<"chat123">>}, {chat, <<"Hello!">>}} -> ok
    after 1000 ->
        ct:fail("Did not receive tuple topic message")
    end,

    ok = hornbeam_pubsub:unsubscribe(Topic).

%%% ============================================================================
%%% Helper functions
%%% ============================================================================

start_peer() ->
    case node() of
        nonode@nohost ->
            os:cmd("epmd -daemon"),
            {ok, _} = net_kernel:start([list_to_atom(peer:random_name()), shortnames]);
        _ -> ok
    end,
    {ok, Peer, Node} = peer:start_link(#{name => peer:random_name(),
        connection => standard_io, args => ["+S", "2", "-pa" | code:get_path()]}),
    {ok, _} = peer:call(Peer, hornbeam_pubsub, start_link, []),
    {Peer, Node}.

test_cross_node_publish(_Config) ->
    Topic = <<"cross-node">>,
    {Peer, Node} = start_peer(),
    try
        {Ref, []} = pg:monitor(hornbeam_pubsub_scope, Topic),
        try
            Remote = spawn(Node, ?MODULE, peer_subscribers, [self(), Topic]),
            receive {ready, Remote} -> ok after 5000 -> ct:fail(peer_not_ready) end,
            await_members(Ref, Topic, 2),
            ok = hornbeam_pubsub:subscribe(Topic),
            lists:foreach(fun(Bytes) ->
                ?assertEqual(3, hornbeam_pubsub:publish(Topic, Bytes)),
                receive {pubsub, Topic, Bytes} -> ok
                after 5000 -> ct:fail(local_delivery_timeout) end,
                receive {subscriber_received, Remote, Bytes} -> ok
                after 5000 -> ct:fail(remote_plain_timeout) end,
                receive {channel_received, Remote, Bytes} -> ok
                after 5000 -> ct:fail(remote_channel_timeout) end
            end, [<<>>, <<"hello">>, <<0, 255, 128>>]),
            ?assertEqual(0, hornbeam_pubsub:publish(<<"no-members">>, ignored))
        after
            pg:demonitor(hornbeam_pubsub_scope, Ref),
            hornbeam_pubsub:unsubscribe(Topic)
        end
    after
        peer:stop(Peer)
    end.

test_remote_overflow(_Config) ->
    {Peer, Node} = start_peer(),
    try
        lists:foreach(fun({Topic, Limit, Suspend, Count}) ->
            {Ref, []} = pg:monitor(hornbeam_pubsub_scope, Topic),
            try
                Owner = spawn(Node, ?MODULE, peer_slow_subscriber, [self(), Topic, Limit]),
                receive {ready, Owner} -> ok after 5000 -> ct:fail(peer_not_ready) end,
                await_members(Ref, Topic, 1),
                Relay = peer:call(Peer, erlang, whereis, [hornbeam_pubsub]),
                case Suspend of true -> ok = sys:suspend(Relay); false -> ok end,
                {Micros, _} = timer:tc(fun() ->
                    lists:foreach(fun(_) -> hornbeam_pubsub:publish(Topic, <<"payload">>) end,
                                  lists:seq(1, Count))
                end),
                ?assert(Micros < 5000000),
                case Suspend of
                    false ->
                        {message_queue_len, Queued} = peer:call(Peer, erlang, process_info,
                            [Relay, message_queue_len]),
                        ?assert(Queued =< 1024);
                    true -> ok
                end,
                case Suspend of true -> ok = sys:resume(Relay); false -> ok end,
                ?assertEqual({error, unknown_request},
                    gen_server:call({hornbeam_pubsub, Node}, barrier, 10000)),
                ?assertEqual({message_queue_len, 0},
                    peer:call(Peer, erlang, process_info, [Relay, message_queue_len])),
                Owner ! {inspect, self()},
                receive
                    {overflow, Owner, #{closed := true, size := Size}, 0} ->
                        ?assert(Size =< Limit),
                        case Suspend of true -> ?assertEqual(0, Size); false -> ?assert(Size > 0) end
                after 5000 -> ct:fail(no_overflow_cleanup)
                end
            after pg:demonitor(hornbeam_pubsub_scope, Ref) end
        end, [{<<"slow-remote">>, 1024, false, 20000},
              {<<"relay-overload">>, 1048576, true, 1100}])
    after peer:stop(Peer) end.

test_node_loss(_Config) ->
    Topic = <<"peer-loss">>,
    {Peer, Node} = start_peer(),
    {Ref, []} = pg:monitor(hornbeam_pubsub_scope, Topic),
    try
        Remote = spawn(Node, ?MODULE, peer_subscribers, [self(), Topic]),
        receive {ready, Remote} -> ok after 5000 -> ct:fail(peer_not_ready) end,
        await_members(Ref, Topic, 2),
        ok = hornbeam_pubsub:subscribe(Topic),
        ok = peer:stop(Peer),
        await_leaves(Ref, Topic, Node, 2),
        ?assertEqual(1, hornbeam_pubsub:publish(Topic, still_here)),
        receive {pubsub, Topic, still_here} -> ok
        after 1000 -> ct:fail(local_delivery_timeout) end
    after
        pg:demonitor(hornbeam_pubsub_scope, Ref),
        hornbeam_pubsub:unsubscribe(Topic),
        catch peer:stop(Peer)
    end.

await_leaves(_Ref, _Topic, _Node, 0) -> ok;
await_leaves(Ref, Topic, Node, Remaining) ->
    receive
        {Ref, leave, Topic, Pids} ->
            await_leaves(Ref, Topic, Node,
                Remaining - length([P || P <- Pids, node(P) =:= Node]))
    after 5000 -> ct:fail(remote_leave_timeout)
    end.

peer_slow_subscriber(Parent, Topic, Limit) ->
    case whereis(hornbeam_services) of
        undefined ->
            ok = py_nif:init(),
            {ok, _} = hornbeam_state:start_link(),
            {ok, _} = hornbeam_services:start_link();
        _ -> ok
    end,
    {<<"ok">>, Channel} = hornbeam_services:call(<<"subscribe">>,
        [self(), <<1:128>>, Topic, Limit]),
    [Guardian] = hornbeam_pubsub:get_local_members(Topic),
    ok = hornbeam_services:managed_overflow(self(), Topic),
    ok = hornbeam_services:managed_overflow(Guardian, wrong_topic),
    #{closed := false} = py_channel:info(Channel),
    Mon = monitor(process, Guardian),
    Parent ! {ready, self()},
    receive {inspect, From} ->
        receive {'DOWN', Mon, process, Guardian, _} -> ok
        after 5000 -> error(guardian_not_reaped) end,
        {<<"ok">>, <<"overflow">>} = hornbeam_services:call(<<"subscription_status">>,
            [self(), <<1:128>>]),
        {<<"ok">>, Active} = hornbeam_services:call(<<"subscription_count">>, []),
        From ! {overflow, self(), py_channel:info(Channel), Active},
        receive stop -> ok end
    end.

await_members(_Ref, _Topic, 0) -> ok;
await_members(Ref, Topic, Remaining) ->
    receive
        {Ref, join, Topic, Pids} ->
            await_members(Ref, Topic, Remaining - length(Pids))
    after 5000 -> ct:fail(remote_membership_timeout)
    end.

peer_subscribers(Parent, Topic) ->
    ok = py_nif:init(),
    {ok, _} = hornbeam_state:start_link(),
    {ok, _} = hornbeam_services:start_link(),
    {<<"ok">>, Channel} = hornbeam_services:call(<<"subscribe">>,
        [self(), <<1:128>>, Topic, 1024]),
    ok = hornbeam_pubsub:subscribe(Topic),
    ok = py_nif:channel_register_sync_waiter(Channel),
    Parent ! {ready, self()},
    peer_subscriber_loop(Parent, Topic, Channel).

peer_subscriber_loop(Parent, Topic, Channel) ->
    receive
        {pubsub, Topic, Bytes} ->
            Parent ! {subscriber_received, self(), Bytes},
            peer_subscriber_loop(Parent, Topic, Channel);
        channel_data_ready ->
            {ok, {<<"event">>, {bytes, Bytes}}} = py_nif:channel_try_receive(Channel),
            Parent ! {channel_received, self(), Bytes},
            ok = py_nif:channel_register_sync_waiter(Channel),
            peer_subscriber_loop(Parent, Topic, Channel)
    end.

subscriber_loop(Parent) ->
    receive
        {pubsub, _Topic, Msg} ->
            Parent ! {subscriber_received, self(), Msg},
            subscriber_loop(Parent);
        stop ->
            ok
    end.
