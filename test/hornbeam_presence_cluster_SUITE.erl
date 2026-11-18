%%% @doc Presence across nodes: several tabs per user, late joins,
%%% network splits and node loss. Every node runs only the pub/sub and
%%% presence servers; the Common Test node drives them over the peer
%%% control channel and is not part of the cluster.
-module(hornbeam_presence_cluster_SUITE).

-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([test_multiple_tabs/1, test_late_join/1, test_network_split/1,
         test_node_loss/1]).
-export([start_services/0, tab/3, watch/1]).

-define(TOPIC, <<"room:lobby">>).
-define(TIMEOUT, 5000).

all() ->
    [test_multiple_tabs, test_late_join, test_network_split, test_node_loss].

init_per_suite(Config) ->
    _ = os:cmd("epmd -daemon"),
    Config.

end_per_suite(_Config) ->
    ok.

%%% ============================================================================
%%% Test cases
%%% ============================================================================

%% One user open in two tabs on one node and a third tab on another node
%% is one key with three metas everywhere; closing a tab removes only its
%% meta, and every node announces each change once.
test_multiple_tabs(_Config) ->
    [A, B] = Nodes = start_cluster(2),
    try
        Watcher = start_watcher(B),
        Tab1 = start_tab(A, <<"user:1">>, <<"a-1">>),
        _Tab2 = start_tab(A, <<"user:1">>, <<"a-2">>),
        _Tab3 = start_tab(B, <<"user:1">>, <<"b-1">>),
        Three = #{<<"user:1">> => [<<"a-1">>, <<"a-2">>, <<"b-1">>]},
        await_all(Nodes, Three),
        close_tab(A, Tab1),
        Two = #{<<"user:1">> => [<<"a-2">>, <<"b-1">>]},
        await_all(Nodes, Two),
        Diffs = watcher_diffs(Watcher, Nodes, Two),
        ?assertEqual(#{joins => [<<"a-1">>, <<"a-2">>, <<"b-1">>],
                       leaves => [<<"a-1">>]}, Diffs)
    after
        stop_cluster(Nodes)
    end.

%% A node that joins after users are present sees all of them, whether
%% presence starts before or after the node connects, and its own users
%% reach the nodes that were already running.
test_late_join(_Config) ->
    [A, B] = Nodes = start_cluster(2),
    try
        _ = start_tab(A, <<"user:a">>, <<"a">>),
        _ = start_tab(B, <<"user:b">>, <<"b">>),
        Before = #{<<"user:a">> => [<<"a">>], <<"user:b">> => [<<"b">>]},
        await_all(Nodes, Before),
        ConnectedFirst = start_node(),
        connect(ConnectedFirst, Nodes),
        start_presence(ConnectedFirst),
        PresenceFirst = start_node(),
        start_presence(PresenceFirst),
        connect(PresenceFirst, [ConnectedFirst | Nodes]),
        All = [ConnectedFirst, PresenceFirst | Nodes],
        await_all(All, Before),
        _ = start_tab(ConnectedFirst, <<"user:c">>, <<"c">>),
        _ = start_tab(PresenceFirst, <<"user:d">>, <<"d">>),
        await_all(All, Before#{<<"user:c">> => [<<"c">>], <<"user:d">> => [<<"d">>]}),
        stop_cluster([ConnectedFirst, PresenceFirst])
    after
        stop_cluster(Nodes)
    end.

%% While split, each side lists only the users it can reach and keeps
%% tracking changes on its own side; after the split heals both sides
%% agree on the users that are still present.
test_network_split(_Config) ->
    [A, B] = Nodes = start_cluster(2),
    try
        Gone = start_tab(A, <<"user:gone">>, <<"a-gone">>),
        _ = start_tab(A, <<"user:a">>, <<"a">>),
        _ = start_tab(B, <<"user:b">>, <<"b">>),
        await_all(Nodes, #{<<"user:gone">> => [<<"a-gone">>],
                           <<"user:a">> => [<<"a">>], <<"user:b">> => [<<"b">>]}),
        split(A, B),
        await_list(A, #{<<"user:gone">> => [<<"a-gone">>], <<"user:a">> => [<<"a">>]}),
        await_list(B, #{<<"user:b">> => [<<"b">>]}),
        close_tab(A, Gone),
        _ = start_tab(B, <<"user:new">>, <<"b-new">>),
        await_list(A, #{<<"user:a">> => [<<"a">>]}),
        await_list(B, #{<<"user:b">> => [<<"b">>], <<"user:new">> => [<<"b-new">>]}),
        connect(A, [B]),
        await_all(Nodes, #{<<"user:a">> => [<<"a">>], <<"user:b">> => [<<"b">>],
                           <<"user:new">> => [<<"b-new">>]})
    after
        stop_cluster(Nodes)
    end.

%% Users on a node that stops leave everywhere else. The node can then
%% come back under the same name and its new users appear again.
test_node_loss(_Config) ->
    [A, B, C] = Nodes = start_cluster(3),
    try
        _ = start_tab(A, <<"user:a">>, <<"a">>),
        _ = start_tab(B, <<"user:b">>, <<"b">>),
        _ = start_tab(C, <<"user:c">>, <<"c">>),
        await_all(Nodes, #{<<"user:a">> => [<<"a">>], <<"user:b">> => [<<"b">>],
                           <<"user:c">> => [<<"c">>]}),
        Watcher = start_watcher(A),
        Name = node_name(B),
        stop_node(B),
        Survivors = #{<<"user:a">> => [<<"a">>], <<"user:c">> => [<<"c">>]},
        await_all([A, C], Survivors),
        ?assertEqual(#{joins => [], leaves => [<<"b">>]},
                     watcher_diffs(Watcher, [A, C], Survivors)),
        Back = start_node(Name),
        start_presence(Back),
        connect(Back, [A, C]),
        _ = start_tab(Back, <<"user:b">>, <<"b-again">>),
        await_all([A, Back, C], Survivors#{<<"user:b">> => [<<"b-again">>]}),
        stop_cluster([Back, A, C])
    after
        stop_cluster(Nodes)
    end.

%%% ============================================================================
%%% Cluster helpers (run on the Common Test node)
%%% ============================================================================

start_cluster(Count) ->
    Nodes = [start_node() || _ <- lists:seq(1, Count)],
    lists:foreach(fun start_presence/1, Nodes),
    lists:foreach(fun({Node, Others}) -> connect(Node, Others) end,
                  pairs(Nodes)),
    Nodes.

pairs([]) -> [];
pairs([Node | Rest]) -> [{Node, Rest} | pairs(Rest)].

start_node() ->
    start_node(peer:random_name()).

%% Nodes connect only when a test says so, so `global' cannot rebuild a
%% split by meshing the cluster behind the test's back.
start_node(Name) ->
    {ok, Peer, Node} = peer:start(#{name => Name, connection => standard_io,
        args => ["-connect_all", "false", "-pa" | code:get_path()]}),
    {Peer, Node}.

node_name({_Peer, Node}) ->
    [Name, _Host] = string:split(atom_to_list(Node), "@"),
    Name.

start_presence({Peer, _Node}) ->
    ok = peer:call(Peer, ?MODULE, start_services, []).

connect({Peer, _Node}, Others) ->
    lists:foreach(fun({_, Other}) ->
        true = peer:call(Peer, net_kernel, connect_node, [Other])
    end, Others).

split({PeerA, _}, {_, NodeB}) ->
    true = peer:call(PeerA, erlang, disconnect_node, [NodeB]).

stop_node({Peer, _Node}) ->
    catch peer:stop(Peer),
    ok.

stop_cluster(Nodes) ->
    lists:foreach(fun stop_node/1, Nodes).

start_tab({Peer, _Node}, Key, Tab) ->
    peer:call(Peer, ?MODULE, tab, [?TOPIC, Key, Tab]).

close_tab({Peer, _Node}, Tab) ->
    true = peer:call(Peer, erlang, exit, [Tab, kill]).

await_all(Nodes, Expected) ->
    lists:foreach(fun(Node) -> await_list(Node, Expected) end, Nodes).

%% Waits on the node's own presence_diff stream, so it returns as soon as
%% the node reports the expected users and fails with what it last saw.
await_list(Node, Expected) ->
    Watcher = start_watcher(Node),
    _ = watcher_diffs(Watcher, [], Expected),
    ok.

start_watcher({Peer, _Node}) ->
    {Peer, peer:call(Peer, ?MODULE, watch, [?TOPIC])}.

%% The diffs the watcher saw, once its node lists `Expected'. A call to
%% each node in `Barrier' afterwards flushes any diff they sent it.
watcher_diffs({Peer, Watcher}, Barrier, Expected) ->
    BarrierNodes = [Node || {_, Node} <- Barrier],
    case peer:call(Peer, gen_server, call,
                   [Watcher, {diffs, normalize_expected(Expected), BarrierNodes},
                    ?TIMEOUT * 2],
                   ?TIMEOUT * 3) of
        {ok, Diffs} -> Diffs;
        {timeout, Last} -> ct:fail({presence_not_converged, Expected, Last})
    end.

normalize_expected(Expected) ->
    maps:map(fun(_Key, Tabs) -> lists:sort(Tabs) end, Expected).

%%% ============================================================================
%%% Peer-side helpers
%%% ============================================================================

start_services() ->
    {ok, _} = gen_server:start({local, hornbeam_pubsub}, hornbeam_pubsub, [], []),
    {ok, _} = gen_server:start({local, hornbeam_presence}, hornbeam_presence, [], []),
    ok.

tab(Topic, Key, Tab) ->
    Caller = self(),
    Pid = spawn(fun() ->
        ok = hornbeam_presence:track(Topic, self(), Key, #{<<"tab">> => Tab}),
        Caller ! {tracked, self()},
        receive stop -> ok end
    end),
    receive {tracked, Pid} -> Pid after ?TIMEOUT -> error(track_timeout) end.

%% Subscribed before it returns, so the watcher sees every later diff on
%% its node.
watch(Topic) ->
    Caller = self(),
    Pid = spawn(fun() ->
        ok = hornbeam_pubsub:subscribe(Topic, self()),
        Caller ! {watching, self()},
        receive
            {'$gen_call', From, {diffs, Expected, Barrier}} ->
                Deadline = erlang:monotonic_time(millisecond) + ?TIMEOUT,
                gen_server:reply(From,
                                 await_expected(Topic, Expected, Barrier, Deadline, []))
        end
    end),
    receive {watching, Pid} -> Pid after ?TIMEOUT -> error(watch_timeout) end.

await_expected(Topic, Expected, Barrier, Deadline, Diffs) ->
    Listed = listed(Topic),
    case Listed =:= Expected of
        true ->
            _ = [gen_server:call({hornbeam_presence, Node}, {list, Topic})
                 || Node <- Barrier],
            {ok, summarize(drain(Topic, Diffs))};
        false ->
            receive
                {pubsub, Topic, {presence_diff, Diff}} ->
                    await_expected(Topic, Expected, Barrier, Deadline, [Diff | Diffs])
            after max(0, Deadline - erlang:monotonic_time(millisecond)) ->
                {timeout, Listed}
            end
    end.

drain(Topic, Diffs) ->
    receive
        {pubsub, Topic, {presence_diff, Diff}} -> drain(Topic, [Diff | Diffs])
    after 0 -> Diffs
    end.

listed(Topic) ->
    maps:map(fun(_Key, #{<<"metas">> := Metas}) ->
        lists:sort([Tab || #{<<"tab">> := Tab} <- Metas])
    end, hornbeam_presence:list(Topic)).

summarize(Diffs) ->
    Tabs = fun(Field) ->
        lists:sort([Tab || #{Field := Entries} <- Diffs,
                           #{<<"metas">> := Metas} <- maps:values(Entries),
                           #{<<"tab">> := Tab} <- Metas])
    end,
    #{joins => Tabs(<<"joins">>), leaves => Tabs(<<"leaves">>)}.
