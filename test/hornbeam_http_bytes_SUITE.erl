%% Real HTTP regressions crossing the Erlang/Python native boundary.
-module(hornbeam_http_bytes_SUITE).

-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         scope_types/1, cookie/1, inline_bodies/1, large_body/1]).

all() -> [scope_types, cookie, inline_bodies, large_body].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(hornbeam),
    {ok, Socket} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    %% Resolve the source checkout through rebar's src link, like the
    %% existing ASGI suite (the test app is not a production module).
    LibDir = filename:dirname(filename:dirname(code:which(hornbeam))),
    SourceDir = case file:read_link(filename:join(LibDir, "src")) of
        {ok, RelPath} ->
            filename:dirname(filename:absname(filename:join(LibDir, RelPath)));
        {error, _} -> LibDir
    end,
    AppDir = filename:join(SourceDir, "test/test_apps"),
    ok = hornbeam:start("http_bytes_app:application", #{
        bind => iolist_to_binary(["127.0.0.1:", integer_to_list(Port)]),
        worker_class => asgi,
        context_mode => worker,
        lifespan => off,
        timeout => 3000,
        pythonpath => [list_to_binary(AppDir)]
    }),
    [{port, Port} | Config].

end_per_suite(_Config) ->
    hornbeam:stop(),
    application:stop(hornbeam),
    ok.

scope_types(Config) ->
    %% The app asserts field types before responding. In particular, an
    %% ASCII-only header or query must not become str at the native boundary.
    Url = url(Config, <<"/caf%C3%A9?name=caf%C3%A9">>),
    {ok, #{status := 200, body := {full, Body}}} =
        hornbeam_test_http:request(get, Url),
    ?assertNotEqual(nomatch, binary:match(Body, <<"\"path\": \"/caf\\u00e9\"">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"\"raw_path\": \"/caf%C3%A9\"">>)),
    ?assertNotEqual(nomatch, binary:match(Body, <<"\"query\": \"name=caf%C3%A9\"">>)).

cookie(Config) ->
    {ok, #{status := 200, body := {full, Body}}} =
        hornbeam_test_http:request(get, url(Config, <<"/cookie">>),
            #{headers => [{<<"Cookie">>, <<"probe=yes">>}]}),
    ?assertEqual(<<"yes">>, Body).

inline_bodies(Config) ->
    lists:foreach(fun(Body) -> assert_echo(Config, Body) end,
        [<<"hello">>, <<"café"/utf8>>, <<"雪"/utf8>>, <<255, 0, 128>>, <<>>]).

large_body(Config) ->
    %% Keep the already-correct raw ByteChannel path covered separately.
    assert_echo(Config, binary:copy(<<"café雪"/utf8>>, 12000)).

assert_echo(Config, Body) ->
    {ok, #{status := 200, body := {full, Echo}}} =
        hornbeam_test_http:request(post, url(Config, <<"/echo">>),
            #{headers => [{<<"Content-Type">>, <<"application/octet-stream">>}],
              body => Body}),
    ?assertEqual(Body, Echo).

url(Config, Path) ->
    iolist_to_binary(["http://127.0.0.1:",
                      integer_to_list(proplists:get_value(port, Config)), Path]).
