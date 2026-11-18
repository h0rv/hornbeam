%% Copyright 2026 Hornbeam contributors
%% Licensed under the Apache License, Version 2.0.
%% Generic, node-local byte storage and owned native subscriptions.
-module(hornbeam_services).
-behaviour(gen_server).

-export([start_link/0, reset/0, callback/1, call/2, managed_delivery/3, managed_overflow/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(MANAGED, hornbeam_services_managed).
-define(MAX_OWNERS, 1024).
-define(MAX_TOKENS, 128).
-define(MAX_OWNER_ACTIVE, 32).
-define(MAX_ACTIVE, 4096).
-define(MAX_KEYS, 10000).
-define(MAX_VALUE, 1048576).
-define(MAX_CACHE_BYTES, 67108864).
-define(OPEN, 1).
-define(CLOSED, 2).
-define(OVERFLOW, 3).
-define(OWNER_DOWN, 4).
-define(INVALID_PAYLOAD, 5).

-record(state, {cache, cache_bytes = 0, counters, owners = #{}, guardians = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Release application data without restarting the supervised service.
reset() -> gen_server:call(?MODULE, reset, infinity).

%% Registered as one callback. Python calls async_call(name, action, args).
callback([Action, Args]) -> call(Action, Args);
callback(_) -> failure(<<"invalid_arguments">>).

call(Action, Args) when is_list(Action) ->
    try unicode:characters_to_binary(Action) of
        Binary when is_binary(Binary) -> call(Binary, Args);
        _ -> failure(<<"invalid_arguments">>)
    catch error:badarg -> failure(<<"invalid_arguments">>) end;
call(<<"publish">>, [Topic, Bytes]) ->
    case valid_topic(Topic) andalso valid_payload(Bytes) of
        true ->
            try success(hornbeam_pubsub:publish(Topic, Bytes))
            catch error:badarg -> failure(<<"unavailable">>) end;
        false -> failure(<<"invalid_arguments">>)
    end;
call(Action, Args) when is_binary(Action), is_list(Args) ->
    try gen_server:call(?MODULE, {Action, Args}, infinity)
    catch exit:_ -> failure(<<"unavailable">>) end;
call(_, _) -> failure(<<"invalid_arguments">>).

%% Called in the publishing process, never through the guardian mailbox.
%% A pg marker outlives this service's ETS table. Registry loss must never
%% turn a managed guardian into an unbounded ordinary mailbox subscriber.
managed_delivery(Pid, Topic, Message) ->
    case managed_lookup(Pid) of
        [{Pid, Topic, Channel, Flag}] ->
            case {atomics:get(Flag, 1), atomics:get(Flag, 3), Message} of
                {?OPEN, 1, {presence_diff, _}} -> presence_changed(Pid, Flag);
                {?OPEN, 0, _} -> deliver(Pid, Channel, Flag, Message);
                _ -> ok
            end,
            managed;
        _ -> managed
    end.

managed_lookup(Pid) ->
    try ets:lookup(?MANAGED, Pid)
    catch error:badarg -> [] end.

managed_overflow(Pid, Topic) ->
    case managed_lookup(Pid) of
        [{Pid, Topic, Channel, Flag}] -> terminal(Pid, Channel, Flag, ?OVERFLOW);
        _ -> ok
    end.

deliver(Pid, Channel, Flag, Message) ->
    case valid_payload(Message) of
        false -> terminal(Pid, Channel, Flag, ?INVALID_PAYLOAD);
        true ->
            %% Force application bytes even when they happen to be valid UTF-8.
            %% The converter removes the {bytes, B} transport wrapper.
            case py_channel:send(Channel, {<<"event">>, {bytes, Message}}) of
                ok -> ok;
                busy -> terminal(Pid, Channel, Flag, ?OVERFLOW);
                {error, _} -> terminal(Pid, Channel, Flag, ?CLOSED)
            end
    end.

terminal(_Pid, Channel, Flag, Reason) ->
    case atomics:compare_exchange(Flag, 1, ?OPEN, Reason) of
        ok ->
            %% Closing is idempotent; guardian notification is serialized by
            %% the service with its crash-window reaper, never by publishers.
            py_channel:close(Channel),
            gen_server:cast(?MODULE, reap_terminal),
            ok;
        _ -> ok
    end.

init([]) ->
    Cache = ets:new(hornbeam_services_cache, [set, private]),
    Counters = ets:new(hornbeam_services_counters, [set, private]),
    %% Recover counter key accounting if this service alone was restarted.
    lists:foreach(fun
        ({?MODULE, counter, N, K}) -> ets:insert(Counters, {{N, K}});
        (_) -> ok
    end, hornbeam_state:keys()),
    ets:new(?MANAGED, [named_table, set, protected, {read_concurrency, true}]),
    erlang:send_after(30000, self(), sweep),
    erlang:send_after(100, self(), reap_terminal),
    {ok, #state{cache = Cache, counters = Counters}}.

handle_call(reset, _From, State) ->
    maps:foreach(fun(Ref, Entry) ->
        close_entry(Entry, ?CLOSED),
        demonitor(Ref, [flush])
    end, State#state.guardians),
    maps:foreach(fun(_, #{monitor := Ref}) -> demonitor(Ref, [flush]) end,
                 State#state.owners),
    ets:delete_all_objects(?MANAGED),
    ets:delete_all_objects(State#state.cache),
    lists:foreach(fun
        ({?MODULE, counter, _, _} = Key) -> hornbeam_state:delete(Key);
        (_) -> ok
    end, hornbeam_state:keys()),
    ets:delete_all_objects(State#state.counters),
    {reply, ok, #state{cache = State#state.cache, counters = State#state.counters}};
handle_call({Action, Args}, _From, State) ->
    {Reply, Next} = dispatch(Action, Args, State),
    {reply, Reply, Next};
handle_call(_, _From, State) ->
    {reply, failure(<<"invalid_arguments">>), State}.

handle_cast(reap_terminal, State) -> {noreply, reap_terminal(State)};
handle_cast(_, State) -> {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:take(Ref, State#state.guardians) of
        {Entry, Guardians} ->
            Next = finish_entry(Entry, ?CLOSED, State),
            ets:delete(?MANAGED, Pid),
            {noreply, Next#state{guardians = Guardians}};
        error ->
            case maps:find(Pid, State#state.owners) of
                {ok, #{monitor := Ref, tokens := Tokens}} ->
                    Next = maps:fold(fun(_Token, Entry, Acc) ->
                        case Entry of
                            #{flag := _} -> finish_entry(Entry, ?OWNER_DOWN, Acc);
                            _ -> Acc
                        end
                    end, State, Tokens),
                    {noreply, Next#state{owners = maps:remove(Pid, Next#state.owners)}};
                _ -> {noreply, State}
            end
    end;
handle_info(reap_terminal, State) ->
    %% A publisher may die after winning the terminal CAS but before closing
    %% the channel or notifying its guardian. Reap from the shared flag,
    %% independently of publisher progress. Tombstones prevent repeat signals.
    Next = reap_terminal(State),
    erlang:send_after(100, self(), reap_terminal),
    {noreply, Next};
handle_info(sweep, State) ->
    Next = sweep(State),
    erlang:send_after(30000, self(), sweep),
    {noreply, Next};
handle_info(_, State) -> {noreply, State}.

terminate(_, State) ->
    %% Untrappable kill is covered by each guardian's service monitor.
    maps:foreach(fun(_, Entry) ->
        close_entry(Entry, ?CLOSED)
    end, State#state.guardians),
    ok.
code_change(_, State, _) -> {ok, State}.

dispatch(<<"owner">>, [], State) ->
    Reply = case whereis(hornbeam_sup) of
        Pid when is_pid(Pid) -> success(Pid);
        _ -> failure(<<"unavailable">>)
    end,
    {Reply, State};
dispatch(<<"cache_set">>, [N, K, B, TTL], State)
        when is_binary(N), byte_size(N) =< 256,
             is_binary(K), byte_size(K) =< 1024,
             is_binary(B), byte_size(B) =< ?MAX_VALUE,
             is_integer(TTL), TTL >= -1 ->
    cache_set(N, K, B, TTL, State);
dispatch(<<"cache_get">>, [N, K, Renew], State)
        when is_binary(N), byte_size(N) =< 256,
             is_binary(K), byte_size(K) =< 1024,
             is_integer(Renew), Renew >= -1 ->
    {Entry, Next} = cache_entry({N, K}, State),
    Reply = case Entry of
        missing -> missing();
        {B, Expiry} ->
            case Renew >= 0 andalso Expiry =/= persistent of
                true -> ets:insert(State#state.cache,
                    {{binary:copy(N), binary:copy(K)}, B, now_ms() + Renew});
                false -> ok
            end,
            success({bytes, B})
    end,
    {Reply, Next};
dispatch(<<"cache_clear">>, [N], State)
        when is_binary(N), byte_size(N) =< 256 ->
    Bytes = ets:foldl(fun
        ({{NS, _}, B, _}, Acc) when NS =:= N -> Acc + byte_size(B);
        (_, Acc) -> Acc
    end, 0, State#state.cache),
    %% Use a bound match-spec constant, not the user namespace as a pattern.
    ets:select_delete(State#state.cache,
        [{{{'$1', '_'}, '_', '_'}, [{'==', '$1', {const, N}}], [true]}]),
    {success(true), State#state{cache_bytes = State#state.cache_bytes - Bytes}};
dispatch(Action, [N, K], State)
        when (Action =:= <<"cache_delete">> orelse Action =:= <<"cache_exists">>
              orelse Action =:= <<"cache_ttl">>),
             is_binary(N), byte_size(N) =< 256,
             is_binary(K), byte_size(K) =< 1024 ->
    {Entry, Next} = cache_entry({N, K}, State),
    case Action of
        <<"cache_delete">> ->
            ets:delete(State#state.cache, {N, K}),
            Removed = entry_size(Entry),
            {success(true), Next#state{cache_bytes = Next#state.cache_bytes - Removed}};
        <<"cache_exists">> -> {success(Entry =/= missing), Next};
        <<"cache_ttl">> ->
            Reply = case Entry of
                missing -> missing();
                {_, persistent} -> success(-1);
                {_, Expiry} -> success(max(0, Expiry - now_ms()))
            end,
            {Reply, Next}
    end;
dispatch(<<"counter_incr">>, [N, K, Delta], State)
        when is_binary(N), byte_size(N) =< 256,
             is_binary(K), byte_size(K) =< 1024, is_integer(Delta) ->
    Table = State#state.counters,
    case ets:member(Table, {N, K}) orelse ets:info(Table, size) < ?MAX_KEYS of
        true ->
            NS = binary:copy(N), Key = binary:copy(K),
            Value = hornbeam_state:incr({?MODULE, counter, NS, Key}, Delta),
            ets:insert(Table, {{NS, Key}}),
            {success(Value), State};
        false -> {failure(<<"counter_quota">>), State}
    end;
dispatch(<<"subscribe">>, [Owner, Token, Topic, MaxBytes], State)
        when is_pid(Owner), node(Owner) =:= node(),
             is_binary(Token), byte_size(Token) =:= 16,
             is_integer(MaxBytes), MaxBytes >= 1024, MaxBytes =< 1048576 ->
    case valid_topic(Topic) of
        true -> subscribe(Owner, Token, Topic, MaxBytes, bytes, State);
        false -> {failure(<<"invalid_arguments">>), State}
    end;
dispatch(<<"presence">>, [Owner, Token, Topic, Key, Payload, MaxBytes], State)
        when is_pid(Owner), node(Owner) =:= node(),
             is_binary(Token), byte_size(Token) =:= 16,
             is_binary(Key), byte_size(Key) > 0, byte_size(Key) =< 1024,
             is_integer(MaxBytes), MaxBytes >= 1024, MaxBytes =< 1048576 ->
    case valid_topic(Topic) andalso valid_payload(Payload) of
        true -> subscribe(Owner, Token, Topic, MaxBytes,
                          {presence, binary:copy(Key), binary:copy(Payload)}, State);
        false -> {failure(<<"invalid_arguments">>), State}
    end;
dispatch(<<"unsubscribe">>, [Owner, Token], State)
        when is_pid(Owner), node(Owner) =:= node(),
             is_binary(Token), byte_size(Token) =:= 16 ->
    unsubscribe(Owner, Token, State);
dispatch(<<"subscription_finalize">>, [Owner, Token], State)
        when is_pid(Owner), node(Owner) =:= node(),
             is_binary(Token), byte_size(Token) =:= 16 ->
    finalize_subscription(Owner, Token, State);
dispatch(<<"subscription_status">>, [Owner, Token], State)
        when is_pid(Owner), node(Owner) =:= node(),
             is_binary(Token), byte_size(Token) =:= 16 ->
    Status = case token_entry(Owner, Token, State) of
        #{flag := Flag} -> status(atomics:get(Flag, 1));
        #{status := S} -> S;
        missing -> <<"closed">>
    end,
    {success(Status), State};
dispatch(<<"subscription_count">>, [], State) ->
    Count = maps:fold(fun(_, #{flag := F}, Acc) ->
        case atomics:get(F, 1) of ?OPEN -> Acc + 1; _ -> Acc end
    end, 0, State#state.guardians),
    {success(Count), State};
dispatch(_, _, State) -> {failure(<<"invalid_arguments">>), State}.

cache_set(N, K, B, TTL, State0) ->
    {Old, State} = cache_entry({N, K}, State0),
    Bytes = State#state.cache_bytes - entry_size(Old) + byte_size(B),
    case (Old =/= missing orelse ets:info(State#state.cache, size) < ?MAX_KEYS)
            andalso Bytes =< ?MAX_CACHE_BYTES of
        true ->
            Expiry = case TTL of -1 -> persistent; _ -> now_ms() + TTL end,
            ets:insert(State#state.cache,
                {{binary:copy(N), binary:copy(K)}, binary:copy(B), Expiry}),
            {success(true), State#state{cache_bytes = Bytes}};
        false -> {failure(<<"cache_quota">>), State}
    end.

cache_entry(Key, State) ->
    case ets:lookup(State#state.cache, Key) of
        [] -> {missing, State};
        [{_, B, persistent}] -> {{B, persistent}, State};
        [{_, B, Expiry}] ->
            case Expiry > now_ms() of
                true -> {{B, Expiry}, State};
                false ->
                    ets:delete(State#state.cache, Key),
                    {missing, State#state{cache_bytes = State#state.cache_bytes - byte_size(B)}}
            end
    end.

reap_terminal(State) ->
    maps:fold(fun(_, #{owner := Owner, token := Token, flag := Flag,
                      channel := Channel, pid := Pid}, Acc) ->
        case {atomics:get(Flag, 1), token_entry(Owner, Token, Acc)} of
            {?OPEN, _} -> Acc;
            {Reason, #{flag := _}} ->
                py_channel:close(Channel),
                Pid ! subscription_terminal,
                put_token(Owner, Token, #{status => status(Reason), opened => true}, Acc);
            _ -> Acc
        end
    end, State, State#state.guardians).

sweep(State) ->
    Now = now_ms(),
    Freed = ets:foldl(fun
        ({Key, B, Expiry}, Acc) when is_integer(Expiry), Expiry =< Now ->
            ets:delete(State#state.cache, Key), Acc + byte_size(B);
        (_, Acc) -> Acc
    end, 0, State#state.cache),
    State#state{cache_bytes = State#state.cache_bytes - Freed}.

entry_size(missing) -> 0;
entry_size({B, _}) -> byte_size(B).
now_ms() -> erlang:monotonic_time(millisecond).

subscribe(Owner, Token, Topic, MaxBytes, Kind, State) ->
    case token_entry(Owner, Token, State) of
        #{flag := F, channel := Ch} ->
            case atomics:get(F, 1) of
                ?OPEN -> {success(Ch), State};
                Reason -> {failure(status(Reason)), State}
            end;
        #{status := Reason} -> {failure(Reason), State};
        missing ->
            case reserve_token(Owner, Token, State) of
                {error, Reason} -> {failure(Reason), State};
                {ok, Reserved} ->
                    Tokens = owner_tokens(Owner, Reserved),
                    Active = maps:fold(fun(_, E, Acc) ->
                        case E of
                            #{flag := F} ->
                                case atomics:get(F, 1) of ?OPEN -> Acc + 1; _ -> Acc end;
                            _ -> Acc
                        end
                    end, 0, Tokens),
                    case Active < ?MAX_OWNER_ACTIVE andalso
                            map_size(Reserved#state.guardians) < ?MAX_ACTIVE of
                        true -> open_channel(Owner, Token, Topic, MaxBytes, Kind, Reserved);
                        false -> {failure(<<"subscription_quota">>), Reserved}
                    end
            end
    end.

open_channel(Owner, Token, Topic0, MaxBytes, Kind, State) ->
    Topic = binary:copy(Topic0),
    case py_channel:new(#{max_size => MaxBytes}) of
        {ok, Channel} ->
            Flag = atomics:new(3, [{signed, false}]),
            atomics:put(Flag, 1, ?OPEN),
            case Kind of {presence, _, _} -> atomics:put(Flag, 3, 1); bytes -> ok end,
            Service = self(),
            {Pid, Ref} = spawn_monitor(fun() ->
                guardian(Service, Owner, Topic, Channel, Flag, MaxBytes)
            end),
            Entry = #{owner => Owner, token => binary:copy(Token), topic => Topic,
                      max_bytes => MaxBytes, channel => Channel, flag => Flag, pid => Pid},
            %% Install lookup before exposing pg membership to publishers.
            ets:insert(?MANAGED, {Pid, Topic, Channel, Flag}),
            ok = hornbeam_pubsub:subscribe_managed(Topic, Pid),
            Next = put_token(Owner, Token, Entry, State),
            Opened = Next#state{guardians = (Next#state.guardians)#{Ref => Entry}},
            case register_presence(Kind, Topic, Pid) of
                ok -> {success(Channel), Opened};
                {error, _} -> {failure(<<"unavailable">>), finish_entry(Entry, ?CLOSED, Opened)}
            end;
        {error, _} -> {failure(<<"channel_unavailable">>), State}
    end.

register_presence(bytes, _, _) -> ok;
register_presence({presence, Key, Payload}, Topic, Pid) ->
    try hornbeam_presence:track(Topic, Pid, Key, #{<<"hornbeam_services_payload">> => Payload}) of
        ok -> ok;
        Error -> Error
    catch exit:Reason -> {error, Reason} end.

%% Coalesce diffs before the guardian mailbox, not after an unbounded enqueue.
presence_changed(Pid, Flag) ->
    case atomics:compare_exchange(Flag, 2, 0, 1) of
        ok -> Pid ! presence_changed, ok;
        _ -> ok
    end.

presence_snapshot(Topic, Channel, Flag, MaxBytes) ->
    atomics:put(Flag, 2, 0),
    Entries = lists:sort([
        {Key, Ref, {bytes, Payload}}
        || {Key, #{<<"metas">> := Metas}} <- maps:to_list(hornbeam_presence:list(Topic)),
           #{<<"hb_ref">> := Ref, <<"hornbeam_services_payload">> := Payload} <- Metas
    ]),
    Frame = {<<"presence">>, Entries},
    case erlang:external_size(Frame) =< MaxBytes of
        false -> terminal(self(), Channel, Flag, ?OVERFLOW);
        true ->
            case py_channel:send(Channel, Frame) of
                ok -> ok;
                busy -> terminal(self(), Channel, Flag, ?OVERFLOW);
                {error, _} -> terminal(self(), Channel, Flag, ?CLOSED)
            end
    end.

unsubscribe(Owner, Token, State) ->
    case token_entry(Owner, Token, State) of
        #{flag := _} = Entry -> {success(true), finish_entry(Entry, ?CLOSED, State)};
        #{status := _} -> {success(true), State};
        missing ->
            %% No tombstone is needed once an owner is dead: no late open
            %% can succeed. Cleanup remains acknowledged after owner death.
            case is_process_alive(Owner) of
                false -> {success(true), State};
                true ->
                    case reserve_token(Owner, Token, State) of
                        {ok, Next} -> {success(true), Next};
                        {error, <<"owner_down">>} -> {success(true), State};
                        {error, Reason} -> {failure(Reason), State}
                    end
            end
    end.

%% Only the consumer of a successful, unique open may acknowledge retirement,
%% after its single unsubscribe has completed. Canceled opens are not retried:
%% their tombstones must survive a late subscribe from a different process.
finalize_subscription(Owner, Token, State) ->
    case token_entry(Owner, Token, State) of
        #{flag := _} -> {failure(<<"subscription_not_closed">>), State};
        #{status := Status, opened := true} ->
            Guardians = maps:filter(fun(Ref, Entry) ->
                case Entry of
                    #{owner := Owner, token := Token, pid := Pid} ->
                        close_entry(Entry, ?CLOSED),
                        demonitor(Ref, [flush]),
                        ets:delete(?MANAGED, Pid),
                        false;
                    _ -> true
                end
            end, State#state.guardians),
            #{monitor := OwnerRef, tokens := Tokens} = Info =
                maps:get(Owner, State#state.owners),
            Remaining = maps:remove(Token, Tokens),
            Owners = case map_size(Remaining) of
                0 ->
                    demonitor(OwnerRef, [flush]),
                    maps:remove(Owner, State#state.owners);
                _ -> (State#state.owners)#{Owner => Info#{tokens => Remaining}}
            end,
            {success(Status), State#state{owners = Owners, guardians = Guardians}};
        #{status := Status} -> {success(Status), State};
        missing -> {success(<<"closed">>), State}
    end.

reserve_token(Owner, Token, State) ->
    case is_process_alive(Owner) of
        false -> {error, <<"owner_down">>};
        true ->
            case maps:find(Owner, State#state.owners) of
                {ok, #{tokens := Tokens}} when map_size(Tokens) >= ?MAX_TOKENS ->
                    {error, <<"token_quota">>};
                {ok, _} ->
                    {ok, put_token(Owner, Token, #{status => <<"closed">>}, State)};
                error when map_size(State#state.owners) >= ?MAX_OWNERS ->
                    {error, <<"owner_quota">>};
                error ->
                    Ref = monitor(process, Owner),
                    Owners = (State#state.owners)#{Owner => #{monitor => Ref, tokens => #{}}},
                    {ok, put_token(Owner, Token, #{status => <<"closed">>},
                                   State#state{owners = Owners})}
            end
    end.

owner_tokens(Owner, State) ->
    #{tokens := Tokens} = maps:get(Owner, State#state.owners),
    Tokens.
token_entry(Owner, Token, State) ->
    case maps:find(Owner, State#state.owners) of
        {ok, #{tokens := Tokens}} -> maps:get(Token, Tokens, missing);
        error -> missing
    end.
put_token(Owner, Token, Entry, State) ->
    OwnerInfo = maps:get(Owner, State#state.owners),
    Tokens = maps:get(tokens, OwnerInfo),
    State#state{owners = (State#state.owners)#{Owner =>
        OwnerInfo#{tokens => Tokens#{binary:copy(Token) => Entry}}}}.

finish_entry(Entry, Reason, State) ->
    close_entry(Entry, Reason),
    #{owner := Owner, token := Token, flag := Flag} = Entry,
    %% A guardian DOWN may arrive after its owner's metadata was removed.
    case maps:is_key(Owner, State#state.owners) of
        true -> put_token(Owner, Token,
            #{status => status(atomics:get(Flag, 1)), opened => true}, State);
        false -> State
    end.

close_entry(#{pid := Pid, topic := Topic, channel := Channel, flag := Flag}, Reason) ->
    _ = atomics:compare_exchange(Flag, 1, ?OPEN, Reason),
    py_channel:close(Channel),
    %% Leave is synchronous: explicit unsubscribe is acknowledged only after it.
    leave(Topic, Pid),
    untrack_presence(Topic, Pid, Flag),
    Pid ! subscription_terminal,
    ok.

guardian(Service, Owner, Topic, Channel, Flag, MaxBytes) ->
    ServiceRef = monitor(process, Service),
    OwnerRef = monitor(process, Owner),
    PresenceRef = case atomics:get(Flag, 3) of
        1 -> monitor(process, {hornbeam_presence, node()});
        0 -> undefined
    end,
    guardian_loop(ServiceRef, OwnerRef, PresenceRef, Topic, Channel, Flag, MaxBytes).
guardian_loop(ServiceRef, OwnerRef, PresenceRef, Topic, Channel, Flag, MaxBytes) ->
    receive
        {'DOWN', OwnerRef, process, _, _} ->
            guardian_close(Topic, Channel, Flag, ?OWNER_DOWN);
        {'DOWN', ServiceRef, process, _, _} ->
            guardian_close(Topic, Channel, Flag, ?CLOSED);
        {'DOWN', PresenceRef, process, _, _} ->
            guardian_close(Topic, Channel, Flag, ?CLOSED);
        subscription_terminal -> guardian_close(Topic, Channel, Flag, ?CLOSED);
        presence_changed ->
            case atomics:get(Flag, 1) of
                ?OPEN -> presence_snapshot(Topic, Channel, Flag, MaxBytes);
                _ -> ok
            end,
            guardian_loop(ServiceRef, OwnerRef, PresenceRef, Topic, Channel, Flag, MaxBytes);
        _ -> guardian_loop(ServiceRef, OwnerRef, PresenceRef, Topic, Channel, Flag, MaxBytes)
    end.
guardian_close(Topic, Channel, Flag, Reason) ->
    _ = atomics:compare_exchange(Flag, 1, ?OPEN, Reason),
    py_channel:close(Channel),
    leave(Topic, self()),
    untrack_presence(Topic, self(), Flag),
    ok.

untrack_presence(Topic, Pid, Flag) ->
    case atomics:get(Flag, 3) of
        0 -> ok;
        1 ->
            try
                ok = hornbeam_presence:untrack_all(Topic, Pid),
                _ = hornbeam_presence:list(Topic),
                ok
            catch exit:_ -> ok end
    end.

leave(Topic, Pid) ->
    try hornbeam_pubsub:unsubscribe(Topic, Pid)
    catch exit:_ -> ok; error:badarg -> ok end.

valid_topic(T) when is_binary(T), byte_size(T) > 0, byte_size(T) =< 256 ->
    case unicode:characters_to_binary(T, utf8, utf8) of T -> true; _ -> false end;
valid_topic(_) -> false.
valid_payload(B) -> is_binary(B) andalso byte_size(B) =< 65536.
status(?OPEN) -> <<"open">>;
status(?CLOSED) -> <<"closed">>;
status(?OVERFLOW) -> <<"overflow">>;
status(?OWNER_DOWN) -> <<"owner_down">>;
status(?INVALID_PAYLOAD) -> <<"invalid_payload">>.
success(Value) -> {<<"ok">>, Value}.
missing() -> {<<"missing">>, <<>>}.
failure(Reason) -> {<<"error">>, Reason}.
