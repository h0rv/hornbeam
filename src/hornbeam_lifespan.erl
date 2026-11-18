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

%%% @doc ASGI Lifespan protocol manager.
%%%
%%% This module implements the ASGI lifespan protocol for application
%%% startup and shutdown events. It allows ASGI applications to perform
%%% initialization (loading ML models, connecting to databases) at startup
%%% and cleanup at shutdown.
%%%
%%% == Lifespan Protocol ==
%%%
%%% The lifespan protocol consists of these events:
%%%
%%% Startup:
%%% &lt;ol&gt;
%%% &lt;li&gt;Server sends: type = "lifespan.startup"&lt;/li&gt;
%%% &lt;li&gt;App responds: type = "lifespan.startup.complete" or
%%%     type = "lifespan.startup.failed"&lt;/li&gt;
%%% &lt;/ol&gt;
%%%
%%% Shutdown:
%%% &lt;ol&gt;
%%% &lt;li&gt;Server sends: type = "lifespan.shutdown"&lt;/li&gt;
%%% &lt;li&gt;App responds: type = "lifespan.shutdown.complete"&lt;/li&gt;
%%% &lt;/ol&gt;
%%%
%%% == Configuration ==
%%%
%%% The lifespan setting can be:
%%% &lt;ul&gt;
%%% &lt;li&gt;auto: Detect if app supports lifespan (default)&lt;/li&gt;
%%% &lt;li&gt;on: Require lifespan support, fail if not supported&lt;/li&gt;
%%% &lt;li&gt;off: Disable lifespan protocol&lt;/li&gt;
%%% &lt;/ul&gt;
-module(hornbeam_lifespan).

-behaviour(gen_server).

-export([
    start_link/0,
    start_link/1,
    startup/0,
    startup/1,
    startup/2,
    shutdown/0,
    shutdown/1,
    get_state/0,
    get_state/1,
    set_state/2,
    update_state/2,
    update_state/3,
    get_context/0,
    get_loop/1,
    is_running/0,
    is_running/1
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
-define(DEFAULT_TIMEOUT, 30000).
-define(CACHE_TABLE, hornbeam_lifespan_cache).

%% Per-mount lifespan tracking
-record(mount_lifespan, {
    mount_id :: binary(),
    app_module :: binary(),
    app_callable :: binary(),
    started = false :: boolean(),
    supported = unknown :: boolean() | unknown,
    session = undefined :: term()
}).

-record(state, {
    app_module :: binary() | undefined,
    app_callable :: binary() | undefined,
    lifespan_mode :: auto | on | off,
    lifespan_state :: map(),
    py_context :: term() | undefined,  %% Python context for affinity
    started = false :: boolean(),
    supported = unknown :: boolean() | unknown,
    session = undefined :: term(),
    %% Per-mount tracking for multi-app mode
    mounts = #{} :: #{binary() => #mount_lifespan{}}
}).

%%% ============================================================================
%%% API
%%% ============================================================================

%% @doc Start the lifespan manager.
start_link() ->
    start_link(#{}).

%% @doc Start the lifespan manager with options.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Run lifespan startup protocol.
%%
%% This should be called after the application is loaded but before
%% accepting requests.
-spec startup() -> ok | {error, term()}.
startup() ->
    startup(#{}).

%% @doc Run lifespan startup with options.
-spec startup(map()) -> ok | {error, term()}.
startup(Opts) ->
    gen_server:call(?SERVER, {startup, Opts}, infinity).

%% @doc Run lifespan startup for a specific mount.
%% Used in multi-app mode to run lifespan per mounted app.
-spec startup(binary(), map()) -> ok | {error, term()}.
startup(MountId, Opts) when is_binary(MountId) ->
    gen_server:call(?SERVER, {startup_mount, MountId, Opts}, infinity).

%% @doc Run lifespan shutdown protocol.
-spec shutdown() -> ok | {error, term()}.
shutdown() ->
    gen_server:call(?SERVER, shutdown, infinity).

%% @doc Run lifespan shutdown for a specific mount.
-spec shutdown(binary()) -> ok | {error, term()}.
shutdown(MountId) when is_binary(MountId) ->
    gen_server:call(?SERVER, {shutdown_mount, MountId}, infinity).

%% @doc Get the lifespan state for single-app mode (backward compat).
%% Uses ETS cache for fast concurrent reads.
-spec get_state() -> map().
get_state() ->
    case ets:lookup(?CACHE_TABLE, lifespan_state) of
        [{lifespan_state, State}] -> State;
        [] -> #{}
    end.

%% @doc Get the lifespan state for a specific mount.
%% Used in multi-app mode to get per-mount state.
-spec get_state(binary()) -> map().
get_state(MountId) when is_binary(MountId) ->
    case ets:lookup(?CACHE_TABLE, {lifespan_state, MountId}) of
        [{{lifespan_state, MountId}, State}] -> State;
        [] -> #{}
    end.

%% @doc Set the lifespan state for a specific mount.
%% Used after startup to store per-mount state in ETS.
-spec set_state(binary(), map()) -> ok.
set_state(MountId, State) when is_binary(MountId), is_map(State) ->
    ets:insert(?CACHE_TABLE, {{lifespan_state, MountId}, State}),
    ok.

%% @doc Update a single key in the lifespan state (single-app mode).
%% This is called from Python to persist state changes.
-spec update_state(binary(), term()) -> ok.
update_state(Key, Value) ->
    CurrentState = get_state(),
    NewState = CurrentState#{Key => Value},
    ets:insert(?CACHE_TABLE, {lifespan_state, NewState}),
    ok.

%% @doc Update a single key in the lifespan state for a specific mount.
-spec update_state(binary(), binary(), term()) -> ok.
update_state(MountId, Key, Value) when is_binary(MountId) ->
    CurrentState = get_state(MountId),
    NewState = CurrentState#{Key => Value},
    ets:insert(?CACHE_TABLE, {{lifespan_state, MountId}, NewState}),
    ok.

%% @doc Check if lifespan is running (single-app mode).
-spec is_running() -> boolean().
is_running() ->
    case ets:lookup(?CACHE_TABLE, started) of
        [{started, Started}] -> Started;
        [] -> false
    end.

%% @doc Check if lifespan is running for a specific mount.
-spec is_running(binary()) -> boolean().
is_running(MountId) when is_binary(MountId) ->
    case ets:lookup(?CACHE_TABLE, {started, MountId}) of
        [{{started, MountId}, Started}] -> Started;
        [] -> false
    end.

%% @doc Get the Python context for ASGI calls.
%%
%% Retained for callers using the context API. Lifespan itself runs on the
%% event-loop pool, not this context; this is not a loop-affinity guarantee.
%% Uses ETS cache for fast concurrent reads (avoids gen_server bottleneck).
-spec get_context() -> term() | undefined.
get_context() ->
    case ets:lookup(?CACHE_TABLE, py_context) of
        [{py_context, Ctx}] -> Ctx;
        [] -> undefined
    end.

-spec get_loop(binary() | undefined) -> {ok, reference()} | {error, term()}.
get_loop(MountId) ->
    Cached = case ets:whereis(?CACHE_TABLE) of
        undefined -> [];
        Table -> ets:lookup(Table, {lifespan_loop, MountId})
    end,
    case Cached of
        [{{lifespan_loop, MountId}, LoopRef}] -> {ok, LoopRef};
        [] -> py_event_loop_pool:get_loop()
    end.

%%% ============================================================================
%%% gen_server callbacks
%%% ============================================================================

init(Opts) ->
    LifespanMode = maps:get(lifespan, Opts, auto),

    %% Create ETS table for fast concurrent reads (avoids gen_server bottleneck)
    ?CACHE_TABLE = ets:new(?CACHE_TABLE, [
        named_table,
        public,
        {read_concurrency, true}
    ]),

    %% Get a Python context from hornbeam_context_pool for ASGI affinity
    %% This ensures module-level state persists across requests
    %% Using hornbeam_context_pool ensures priv/ is in sys.path
    PyContext = try
        hornbeam_context_pool:get_context_ref()
    catch
        _:_ -> undefined
    end,

    %% Cache initial values
    ets:insert(?CACHE_TABLE, [
        {py_context, PyContext},
        {lifespan_state, #{}},
        {started, false}
    ]),

    %% Register state callbacks for direct Python access (no whereis needed)
    register_state_callbacks(),

    {ok, #state{
        lifespan_mode = LifespanMode,
        lifespan_state = #{},
        py_context = PyContext
    }}.

handle_call({startup, _Opts}, _From, #state{started = true} = State) ->
    {reply, ok, State};
handle_call({startup, Opts}, _From, State) ->
    AppModule = hornbeam_config:get_config(app_module),
    AppCallable = hornbeam_config:get_config(app_callable),
    LifespanMode = maps:get(lifespan, Opts, State#state.lifespan_mode),

    case LifespanMode of
        off ->
            %% Update ETS cache
            ets:insert(?CACHE_TABLE, {started, true}),
            {reply, ok, State#state{started = true, supported = false}};
        _ ->
            %% Submit persistent lifespan execution to the event-loop pool
            case run_startup(AppModule, AppCallable, undefined) of
                {ok, LifespanState, Session} ->
                    %% Update ETS cache
                    ets:insert(?CACHE_TABLE, [
                        {lifespan_state, LifespanState},
                        {started, true}
                    ]),
                    {reply, ok, State#state{
                        app_module = AppModule,
                        app_callable = AppCallable,
                        lifespan_mode = LifespanMode,
                        lifespan_state = LifespanState,
                        session = Session,
                        started = true,
                        supported = true
                    }};
                {error, not_supported} when LifespanMode =:= auto ->
                    %% Lifespan not supported, but that's OK in auto mode
                    %% Update ETS cache
                    ets:insert(?CACHE_TABLE, {started, true}),
                    {reply, ok, State#state{
                        app_module = AppModule,
                        app_callable = AppCallable,
                        lifespan_mode = LifespanMode,
                        started = true,
                        supported = false
                    }};
                {error, not_supported} when LifespanMode =:= on ->
                    %% Lifespan required but not supported
                    {reply, {error, lifespan_not_supported}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end;

%% Per-mount startup handler
handle_call({startup_mount, MountId, Opts}, From, #state{mounts = Mounts} = State) ->
    case maps:get(MountId, Mounts, undefined) of
        #mount_lifespan{started = true} -> {reply, ok, State};
        _ -> startup_mount_call(MountId, Opts, From, State)
    end;

handle_call({shutdown_mount, MountId}, _From, #state{mounts = Mounts} = State) ->
    case maps:get(MountId, Mounts, undefined) of
        undefined -> {reply, ok, State};
        #mount_lifespan{started = false} -> {reply, ok, State};
        #mount_lifespan{session = Session} = ML ->
            Result = run_shutdown(Session),
            ets:delete(?CACHE_TABLE, {lifespan_loop, MountId}),
            ets:insert(?CACHE_TABLE, [
                {{started, MountId}, false},
                {{lifespan_state, MountId}, #{}}
            ]),
            {reply, Result, State#state{mounts = Mounts#{MountId =>
                ML#mount_lifespan{started = false, session = undefined}}}}
    end;

handle_call(shutdown, From, #state{mounts = Mounts} = State) when map_size(Mounts) > 0 ->
    {Results, NewState} = lists:mapfoldl(fun(MountId, AccState) ->
        {reply, Result, NextState} = handle_call({shutdown_mount, MountId}, From, AccState),
        {Result, NextState}
    end, State, maps:keys(Mounts)),
    {reply, Result, FinalState} = handle_call(shutdown, From, NewState#state{mounts = #{}}),
    Errors = [Error || Error <- Results ++ [Result], Error =/= ok],
    Reply = case Errors of [] -> ok; [First | _] -> First end,
    {reply, Reply, FinalState};

handle_call(shutdown, _From, #state{started = false} = State) ->
    {reply, ok, State};
handle_call(shutdown, _From, #state{session = Session} = State) ->
    Result = run_shutdown(Session),
    ets:delete(?CACHE_TABLE, {lifespan_loop, undefined}),
    ets:insert(?CACHE_TABLE, [{started, false}, {lifespan_state, #{}}]),
    {reply, Result, State#state{started = false, lifespan_state = #{}, session = undefined}};
handle_call(get_state, _From, #state{lifespan_state = LifespanState} = State) ->
    {reply, LifespanState, State};
handle_call(is_running, _From, #state{started = Started} = State) ->
    {reply, Started, State};
handle_call(get_context, _From, #state{py_context = PyContext} = State) ->
    {reply, PyContext, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

startup_mount_call(MountId, Opts, _From, #state{mounts = Mounts} = State) ->
    AppModule = maps:get(app_module, Opts),
    AppCallable = maps:get(app_callable, Opts),
    LifespanMode = maps:get(lifespan, Opts, auto),

    case LifespanMode of
        off ->
            %% Update ETS cache for this mount
            ets:insert(?CACHE_TABLE, {{started, MountId}, true}),
            MountLifespan = #mount_lifespan{
                mount_id = MountId,
                app_module = AppModule,
                app_callable = AppCallable,
                started = true,
                supported = false
            },
            {reply, ok, State#state{mounts = Mounts#{MountId => MountLifespan}}};
        _ ->
            %% Try to run lifespan startup with mount_id
            case run_startup(AppModule, AppCallable, MountId) of
                {ok, LifespanState, Session} ->
                    %% Update ETS cache for this mount
                    ets:insert(?CACHE_TABLE, [
                        {{lifespan_state, MountId}, LifespanState},
                        {{started, MountId}, true}
                    ]),
                    MountLifespan = #mount_lifespan{
                        mount_id = MountId,
                        app_module = AppModule,
                        app_callable = AppCallable,
                        started = true,
                        supported = true,
                        session = Session
                    },
                    {reply, ok, State#state{mounts = Mounts#{MountId => MountLifespan}}};
                {error, not_supported} when LifespanMode =:= auto ->
                    %% Lifespan not supported, but that's OK in auto mode
                    ets:insert(?CACHE_TABLE, {{started, MountId}, true}),
                    MountLifespan = #mount_lifespan{
                        mount_id = MountId,
                        app_module = AppModule,
                        app_callable = AppCallable,
                        started = true,
                        supported = false
                    },
                    {reply, ok, State#state{mounts = Mounts#{MountId => MountLifespan}}};
                {error, not_supported} when LifespanMode =:= on ->
                    {reply, {error, lifespan_not_supported}, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end.

handle_cast(_Request, State) ->
    {noreply, State}.

%% Handle state update messages from Python (via erlang.send)
%% Python sends binaries, so we match on <<"update_state">>
handle_info({<<"update_state">>, Key, Value}, State) ->
    %% Single-app mode state update
    update_state(Key, Value),
    {noreply, State};

handle_info({<<"update_state">>, MountId, Key, Value}, State) ->
    %% Multi-app mode state update
    update_state(MountId, Key, Value),
    {noreply, State};

handle_info({async_result, Ref, Result}, #state{session = {Ref, _, Channel}} = State) ->
    py_channel:close(Channel),
    ets:delete(?CACHE_TABLE, {lifespan_loop, undefined}),
    ets:insert(?CACHE_TABLE, {started, false}),
    error_logger:error_msg("Lifespan task exited: ~p~n", [Result]),
    {noreply, State#state{started = false, session = undefined}};
handle_info({async_result, Ref, Result}, #state{mounts = Mounts} = State) ->
    NewMounts = maps:map(fun(MountId, ML) ->
        case ML#mount_lifespan.session of
            {Ref, _, Channel} ->
                py_channel:close(Channel),
                ets:delete(?CACHE_TABLE, {lifespan_loop, MountId}),
                ets:insert(?CACHE_TABLE, {{started, MountId}, false}),
                error_logger:error_msg("Mount lifespan task exited (~p): ~p~n", [MountId, Result]),
                ML#mount_lifespan{started = false, session = undefined};
            _ -> ML
        end
    end, Mounts),
    {noreply, State#state{mounts = NewMounts}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{session = Session, mounts = Mounts}) ->
    %% Signal all mounts before waiting, so total termination has one deadline.
    Sessions = [S || S <- [Session | [ML#mount_lifespan.session ||
        ML <- maps:values(Mounts)]], S =/= undefined],
    lists:foreach(fun({_, _, Ch}) -> py_channel:send(Ch, <<"lifespan.shutdown">>) end, Sessions),
    Deadline = erlang:monotonic_time(millisecond) + lifespan_timeout(),
    lists:foreach(fun(S) -> finish_shutdown(S, Deadline) end, Sessions),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% ============================================================================
%%% Internal Functions
%%% ============================================================================

%% Keep handlers on the lifespan loop so application resources retain affinity.
run_startup(AppModule, AppCallable, MountId) ->
    try
        {ok, Channel} = py_channel:new(#{max_size => 1024}),
        Session = {make_ref(), erlang:unique_integer([positive]), Channel},
        try
            {Ref, Token, Channel} = Session,
            {ok, LoopRef} = py_event_loop_pool:get_loop(),
            ok = py_nif:submit_task(LoopRef, self(), Ref,
                <<"hornbeam_lifespan_runner">>, <<"run_lifespan">>,
                [self(), Token, Channel, AppModule, AppCallable, MountId,
                 lifespan_timeout()], #{}),
            ok = py_channel:send(Channel, <<"lifespan.startup">>),
            case await_response(Session, lifespan_timeout() + 500) of
                {ok, Response} ->
                    case handle_startup_response(Response) of
                        {ok, AppState} ->
                            ets:insert(?CACHE_TABLE, {{lifespan_loop, MountId}, LoopRef}),
                            {ok, AppState, Session};
                        Error -> py_channel:close(Channel), Error
                    end;
                Error -> py_channel:close(Channel), Error
            end
        catch
            Class:Reason ->
                py_channel:close(Channel),
                {error, {Class, Reason}}
        end
    catch
        Class0:Reason0 -> {error, {Class0, Reason0}}
    end.

lifespan_timeout() ->
    case hornbeam_config:get_config(lifespan_timeout) of
        undefined ->
            case hornbeam_config:get_config(timeout) of
                undefined -> ?DEFAULT_TIMEOUT;
                T -> T
            end;
        T -> T
    end.

await_response({Ref, Token, _}, Timeout) ->
    receive
        {<<"lifespan">>, Token, Response} -> {ok, Response};
        {async_result, Ref, {error, Reason}} -> {error, Reason};
        {async_result, Ref, {ok, _}} -> {error, lifespan_exited}
    after Timeout ->
        {error, timeout}
    end.

run_shutdown(undefined) -> ok;
run_shutdown({_, _, Channel} = Session) ->
    case py_channel:send(Channel, <<"lifespan.shutdown">>) of
        ok -> finish_shutdown(Session, erlang:monotonic_time(millisecond) + lifespan_timeout() + 500);
        Error -> py_channel:close(Channel), {error, {shutdown_channel, Error}}
    end.

finish_shutdown({_, _, Channel} = Session, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    Result = case await_response(Session, Remaining) of
        {ok, Response} -> handle_shutdown_response(Response);
        Error -> Error
    end,
    py_channel:close(Channel),
    Result.

handle_startup_response(Response) ->
    case maps:get(<<"type">>, Response, undefined) of
        <<"lifespan.startup.complete">> ->
            State = maps:get(<<"state">>, Response, #{}),
            {ok, State};
        <<"lifespan.startup.failed">> ->
            Message = maps:get(<<"message">>, Response, <<"Unknown error">>),
            {error, {startup_failed, Message}};
        <<"lifespan.not_supported">> ->
            {error, not_supported};
        _ ->
            {error, {invalid_response, Response}}
    end.

handle_shutdown_response(Response) ->
    case maps:get(<<"type">>, Response, undefined) of
        <<"lifespan.shutdown.complete">> ->
            ok;
        <<"lifespan.shutdown.failed">> ->
            {error, {shutdown_failed, maps:get(<<"message">>, Response, <<"Unknown error">>)}};
        _ ->
            {error, {invalid_response, Response}}
    end.

%% @private
%% Register callbacks for direct Python state access via erlang.call()
%% This avoids erlang.whereis() on every request
register_state_callbacks() ->
    py:register_function(lifespan_state_get, fun state_get_callback/1),
    py:register_function(lifespan_state_set, fun state_set_callback/1),
    ok.

%% @private
%% Callback: get state value
%% Args: [] -> full state, [Key] -> single value, [MountId, Key] -> per-mount value
state_get_callback([]) ->
    get_state();
state_get_callback([Key]) ->
    State = get_state(),
    maps:get(Key, State, undefined);
state_get_callback([MountId, Key]) when is_binary(MountId) ->
    State = get_state(MountId),
    maps:get(Key, State, undefined);
state_get_callback([MountId, _Key]) when MountId =:= undefined; MountId =:= none ->
    %% No mount_id, fall back to single-app mode
    get_state().

%% @private
%% Callback: set state value
%% Args: [Key, Value] -> single-app mode, [MountId, Key, Value] -> per-mount
state_set_callback([Key, Value]) ->
    update_state(Key, Value),
    ok;
state_set_callback([MountId, Key, Value]) when is_binary(MountId) ->
    update_state(MountId, Key, Value),
    ok;
state_set_callback([MountId, Key, Value]) when MountId =:= undefined; MountId =:= none ->
    %% No mount_id, fall back to single-app mode
    update_state(Key, Value),
    ok.
