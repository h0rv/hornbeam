-module(hornbeam_lifespan_affinity_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2, http/1, websocket/1,
         mounted_http/1, mounted_websocket/1, mounted_websocket_route/1, fallback/1]).
-export([init/2, handle_in/2, handle_info/2, terminate/2]).

all() -> [http, websocket, mounted_http, mounted_websocket, mounted_websocket_route, fallback].

init_per_suite(Config) ->
    application:load(erlang_python),
    application:set_env(erlang_python, event_loop_pool_size, 4),
    application:set_env(erlang_python, num_contexts, 2),
    application:set_env(hornbeam, context_pool_size, 2),
    {ok, _} = application:ensure_all_started(hornbeam),
    ?assertEqual(4, length(element(2, py_event_loop_pool:get_all_loops()))),
    Config.

end_per_suite(_Config) ->
    application:stop(hornbeam),
    application:stop(erlang_python),
    application:unset_env(erlang_python, event_loop_pool_size).

init_per_testcase(Test, Config) ->
    {ok, Socket} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Options = #{
        bind => iolist_to_binary(io_lib:format("127.0.0.1:~p", [Port])),
        worker_class => asgi, lifespan => on, websocket_protocol => asgi,
        pythonpath => [test_apps_dir()]},
    case Test of
        mounted_websocket_route ->
            ok = hornbeam:start(Options#{mounts => [
                {<<"/api">>, "lifespan_affinity_app:routed_app", #{worker_class => asgi}}]});
        T when T =:= mounted_http; T =:= mounted_websocket ->
            ok = hornbeam:start(Options#{mounts => [
                {<<"/first">>, "lifespan_affinity_app:app", #{worker_class => asgi}},
                {<<"/second">>, "lifespan_affinity_app:app", #{worker_class => asgi}}]});
        _ -> ok = hornbeam:start("lifespan_affinity_app:app", Options)
    end,
    [{port, Port} | Config].

end_per_testcase(_, _) ->
    hornbeam:stop(),
    ?assertEqual([], ets:match_object(hornbeam_lifespan_cache, {{lifespan_loop, '_'}, '_'})).

test_apps_dir() ->
    Lib = filename:dirname(filename:dirname(code:which(hornbeam))),
    {ok, Link} = file:read_link(filename:join(Lib, "src")),
    Root = filename:dirname(filename:absname(filename:join(Lib, Link))),
    filename:join(Root, "test/test_apps").

http(Config) -> http(Config, "/").

mounted_http(Config) ->
    http(Config, "/first/"),
    http(Config, "/second/").

http(Config, Path) ->
    Port = proplists:get_value(port, Config),
    URL = iolist_to_binary(io_lib:format("http://127.0.0.1:~p~s", [Port, Path])),
    lists:foreach(fun(_) ->
        {ok, #{status := Status, body := {full, Body}}} =
            hornbeam_test_http:request(get, URL, #{}),
        ?assertEqual({200, <<"same-loop">>}, {Status, Body})
    end, lists:seq(1, 16)).

websocket(Config) -> websocket(Config, "/").

mounted_websocket(Config) ->
    websocket(Config, "/first/"),
    websocket(Config, "/second/").

mounted_websocket_route(Config) ->
    websocket(Config, "/api/ws").

websocket(Config, Path) ->
    Port = proplists:get_value(port, Config),
    URL = iolist_to_binary(io_lib:format("ws://127.0.0.1:~p~s", [Port, Path])),
    lists:foreach(fun(_) ->
        {ok, Client} = ws:connect(URL, #{handler => ?MODULE,
            handler_opts => self(), timeout => 5000}),
        try
            receive
                {affinity_frame, Client, Frame} ->
                    ?assertEqual({text, <<"same-loop">>}, Frame)
            after 5000 -> error(websocket_frame_timeout)
            end
        after ws:close(Client, 1000)
        end
    end, lists:seq(1, 16)).

fallback(_) ->
    {ok, Expected} = py_event_loop_pool:get_loop(),
    ?assertEqual({ok, Expected}, hornbeam_lifespan:get_loop(<<"missing">>)),
    ok = hornbeam_lifespan:shutdown(),
    ?assertEqual({ok, Expected}, hornbeam_lifespan:get_loop(undefined)).

init(_, Observer) -> {ok, Observer}.
handle_in(Frame, Observer) ->
    Observer ! {affinity_frame, self(), Frame},
    {ok, Observer}.
handle_info(_, Observer) -> {ok, Observer}.
terminate(_, _) -> ok.
