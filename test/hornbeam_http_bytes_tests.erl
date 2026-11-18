%% Regression tests for ASGI's byte fields before native conversion.
-module(hornbeam_http_bytes_tests).

-include_lib("eunit/include/eunit.hrl").

scope_bytes_test() ->
    Headers = [{<<"cookie">>, <<"probe=yes">>},
               {<<"x-repeat">>, <<"café"/utf8>>},
               {<<"x-repeat">>, <<255, 0, 128>>}],
    Req = livery_req:new(#{path => <<"/caf%C3%A9">>,
                           raw_query => <<"name=caf%C3%A9">>,
                           headers => Headers}),
    Scope = hornbeam_request:build_asgi_scope(Req, #{}),
    ?assertEqual({bytes, <<"/caf%C3%A9">>}, maps:get(raw_path, Scope)),
    ?assertEqual({bytes, <<"name=caf%C3%A9">>}, maps:get(query_string, Scope)),
    ?assertEqual([[{bytes, K}, {bytes, V}] || {K, V} <- Headers],
                 maps:get(headers, Scope)),
    ?assertEqual(<<"http">>, maps:get(type, Scope)),
    ?assertEqual(<<"/caf%C3%A9">>, maps:get(path, Scope)),
    ?assertEqual(<<>>, maps:get(root_path, Scope)).

mounted_raw_path_test() ->
    Req = livery_req:new(#{path => <<"/api/caf%C3%A9">>}),
    Scope = hornbeam_request:build_asgi_scope(Req,
        #{script_name => <<"/api">>, path_info => <<"/caf%C3%A9">>}),
    ?assertEqual({bytes, <<"/api/caf%C3%A9">>}, maps:get(raw_path, Scope)),
    ?assertEqual(<<"/caf%C3%A9">>, maps:get(path, Scope)),
    ?assertEqual(<<"/api">>, maps:get(root_path, Scope)).

websocket_paths_test_() ->
    [?_test(begin
        Req = livery_req:new(#{path => RawPath}),
        HttpScope = hornbeam_request:build_asgi_scope(Req, State),
        WsScope = hornbeam_websocket:build_websocket_scope(Req, State),
        ?assertEqual(Path, maps:get(path, HttpScope)),
        ?assertEqual(Path, maps:get(<<"path">>, WsScope)),
        ?assertEqual(RootPath, maps:get(root_path, HttpScope)),
        ?assertEqual(RootPath, maps:get(<<"root_path">>, WsScope)),
        ?assertEqual({bytes, RawPath}, maps:get(<<"raw_path">>, WsScope))
    end) || {RawPath, State, Path, RootPath} <- [
        {<<"/ws">>, #{}, <<"/ws">>, <<>>},
        {<<"/ws">>, #{script_name => <<"/">>, path_info => <<"/ws">>}, <<"/ws">>, <<"/">>},
        {<<"/api/ws">>, #{script_name => <<"/api">>, path_info => <<"/ws">>},
         <<"/ws">>, <<"/api">>},
        {<<"/api">>, #{script_name => <<"/api">>, path_info => <<"/">>}, <<"/">>, <<"/api">>},
        {<<"/api/nested/caf%C3%A9">>,
         #{script_name => <<"/api/nested">>, path_info => <<"/caf%C3%A9">>},
         <<"/caf%C3%A9">>, <<"/api/nested">>}
    ]].
