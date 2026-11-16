%% Requires the native-only-capable erlang_python dependency, not bridge doubles.
-module(hornbeam_services_native_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1, public_api_over_http/1]).

all() -> [public_api_over_http].

init_per_suite(Config) ->
    application:load(hornbeam),
    application:set_env(hornbeam, context_pool_size, 2),
    application:set_env(erlang_python, num_contexts, 2),
    {ok, _} = application:ensure_all_started(hornbeam),
    {ok, Socket} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    gen_tcp:close(Socket),
    Lib = code:lib_dir(hornbeam),
    {ok, Src} = file:read_link(filename:join(Lib, "src")),
    Root = filename:dirname(filename:absname(filename:join(Lib, Src))),
    ok = hornbeam:start("services_native_app:application", #{
        bind => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
        worker_class => asgi, context_mode => worker, lifespan => off,
        timeout => 5000, pythonpath => [filename:join(Root, "test/test_apps")]
    }),
    [{port, Port} | Config].

end_per_suite(_Config) ->
    hornbeam:stop(), application:stop(hornbeam).

public_api_over_http(Config) ->
    Url = iolist_to_binary(["http://127.0.0.1:",
        integer_to_list(proplists:get_value(port, Config)), "/"]),
    {ok, #{status := 200, body := {full, Body}}} =
        hornbeam_test_http:request(get, Url),
    ?assertEqual(<<"ok">>, Body).
