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

%%% @doc Pub/Sub messaging for Python apps.
%%%
%%% This module provides publish/subscribe messaging backed by Erlang's pg
%%% (process groups). It enables:
%%% - Real-time updates to web clients
%%% - Event broadcasting
%%% - Cross-node messaging (distributed pg)
%%%
%%% Note: This is primarily for Erlang-side subscriptions. For Python
%%% web handlers, use WebSocket connections with topic routing.
-module(hornbeam_pubsub).

-behaviour(gen_server).

-export([
    start_link/0,
    start_path_batcher/0,
    subscribe/1,
    subscribe/2,
    subscribe_path/1,
    subscribe_managed/2,
    is_managed/1,
    unsubscribe/1,
    unsubscribe/2,
    publish/2,
    publish_local/2,
    get_members/1,
    get_local_members/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(PATH_BATCHER, hornbeam_pubsub_path_batcher).
-define(SCOPE, hornbeam_pubsub_scope).
-define(MANAGED_GROUP, {?MODULE, managed_guardians}).
-define(MAX_RELAY_QUEUE, 1024).
-define(PATH_BATCH_SIZE, 128).

-record(state, {path_joins = #{}, path_count = 0, path_timer = undefined}).

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Start the pubsub manager.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

start_path_batcher() ->
    gen_server:start_link({local, ?PATH_BATCHER}, ?MODULE, path_batcher, []).

%% @doc Subscribe the calling process to a topic.
-spec subscribe(Topic :: term()) -> ok.
subscribe(Topic) ->
    subscribe(Topic, self()).

%% @doc Subscribe a specific process to a topic.
-spec subscribe(Topic :: term(), Pid :: pid()) -> ok.
subscribe(Topic, Pid) ->
    pg:join(?SCOPE, Topic, Pid),
    ok.

-spec subscribe_path(binary()) -> ok.
subscribe_path(Topic) ->
    gen_server:call(?PATH_BATCHER, {subscribe_path, Topic}, infinity).

%% Mark before exposing topic membership. The marker is owned by pg, not
%% the service ETS owner, and is removed only when the guardian dies. Even
%% after unsubscribe/reset, stale publisher snapshots must fail closed.
subscribe_managed(Topic, Pid) ->
    ok = pg:join(?SCOPE, ?MANAGED_GROUP, Pid),
    subscribe(Topic, Pid).

is_managed(Pid) ->
    lists:member(Pid, pg:get_members(?SCOPE, ?MANAGED_GROUP)).

%% @doc Unsubscribe the calling process from a topic.
-spec unsubscribe(Topic :: term()) -> ok.
unsubscribe(Topic) ->
    unsubscribe(Topic, self()).

%% @doc Unsubscribe a specific process from a topic.
-spec unsubscribe(Topic :: term(), Pid :: pid()) -> ok.
unsubscribe(Topic, Pid) ->
    pg:leave(?SCOPE, Topic, Pid),
    ok.

%% @doc Publish a message to all subscribers of a topic.
%% Returns the number of processes the message was sent to.
-spec publish(Topic :: term(), Message :: term()) -> non_neg_integer().
publish(Topic, Message) ->
    Members = pg:get_members(?SCOPE, Topic),
    Managed = maps:from_keys(pg:get_members(?SCOPE, ?MANAGED_GROUP), true),
    {Local, Nodes} = lists:foldl(fun(Pid, {LocalAcc, NodeAcc}) ->
        case maps:is_key(Pid, Managed) of
            false ->
                Pid ! {pubsub, Topic, Message},
                {LocalAcc, NodeAcc};
            true when node(Pid) =:= node() -> {[Pid | LocalAcc], NodeAcc};
            true -> {LocalAcc, NodeAcc#{node(Pid) => true}}
        end
    end, {[], #{}}, Members),
    deliver_managed(Local, Topic, Message),
    lists:foreach(fun(Node) ->
        {?SERVER, Node} ! {publish, Topic, Message}
    end, maps:keys(Nodes)),
    length(Members).

%% @doc Publish a message to the subscribers of a topic on this node only.
%% Returns the number of processes the message was sent to.
-spec publish_local(Topic :: term(), Message :: term()) -> non_neg_integer().
publish_local(Topic, Message) ->
    Members = pg:get_local_members(?SCOPE, Topic),
    Managed = maps:from_keys(pg:get_local_members(?SCOPE, ?MANAGED_GROUP), true),
    {ManagedMembers, Plain} = lists:partition(fun(Pid) -> maps:is_key(Pid, Managed) end,
                                              Members),
    lists:foreach(fun(Pid) -> Pid ! {pubsub, Topic, Message} end, Plain),
    deliver_managed(ManagedMembers, Topic, Message),
    length(Members).

deliver_managed(Members, Topic, Message) ->
    lists:foreach(fun(Pid) ->
        hornbeam_services:managed_delivery(Pid, Topic, Message)
    end, Members).

%% @doc Get all members subscribed to a topic (all nodes).
-spec get_members(Topic :: term()) -> [pid()].
get_members(Topic) ->
    pg:get_members(?SCOPE, Topic).

%% @doc Get local members subscribed to a topic.
-spec get_local_members(Topic :: term()) -> [pid()].
get_local_members(Topic) ->
    pg:get_local_members(?SCOPE, Topic).

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init([]) ->
    %% Start pg scope for hornbeam pubsub
    %% pg:start returns {ok, Pid} or {error, {already_started, Pid}}
    case pg:start(?SCOPE) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end,
    {ok, #{}};
init(path_batcher) ->
    {ok, #state{}}.

handle_call({subscribe_path, Topic}, From,
            #state{path_joins = Joins, path_count = Count, path_timer = Timer} = State) ->
    Next = State#state{
        path_joins = maps:update_with(Topic, fun(Waiting) -> [From | Waiting] end, [From], Joins),
        path_count = Count + 1
    },
    case Count + 1 of
        ?PATH_BATCH_SIZE ->
            erlang:cancel_timer(Timer),
            {noreply, flush_path_joins(Next)};
        1 ->
            {noreply, Next#state{path_timer = erlang:start_timer(1, self(), path_joins)}};
        _ ->
            {noreply, Next}
    end;
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({timeout, Timer, path_joins}, #state{path_timer = Timer} = State) ->
    {noreply, flush_path_joins(State)};
handle_info({publish, Topic, Message}, #{} = State) ->
    Managed = maps:from_keys(pg:get_local_members(?SCOPE, ?MANAGED_GROUP), true),
    Members = [Pid || Pid <- pg:get_local_members(?SCOPE, Topic),
                      maps:is_key(Pid, Managed)],
    case process_info(self(), message_queue_len) of
        {message_queue_len, Length} when Length > ?MAX_RELAY_QUEUE ->
            lists:foreach(fun(Pid) ->
                hornbeam_services:managed_overflow(Pid, Topic)
            end, Members);
        _ -> deliver_managed(Members, Topic, Message)
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

flush_path_joins(State = #state{path_joins = Joins}) ->
    %% pg rewrites the group's membership lists on every call. Join a
    %% burst together, but acknowledge only after membership is visible.
    maps:foreach(fun(Topic, Waiting) ->
        ok = pg:join(?SCOPE, Topic, [Pid || {Pid, _} <- Waiting]),
        lists:foreach(fun(From) -> gen_server:reply(From, ok) end, Waiting)
    end, Joins),
    State#state{path_joins = #{}, path_count = 0, path_timer = undefined}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
