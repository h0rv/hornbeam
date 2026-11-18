%% Network regressions. Requires the patched native Channel.async_receive API.
%% Run after building the complete runtime:
%% rebar3 ct --suite test/hornbeam_ws_continuous_patch_SUITE.erl
-module(hornbeam_ws_continuous_patch_SUITE).
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([idle_output_and_owner/1, five_item_arrays_are_asgi/1,
         native_channels_are_explicit/1, mounted_legacy_topics_are_isolated/1,
         shutdown_awaits_asgi_cleanup/1]).
%% Also act as the ws client's observer handler.
-export([init/2, handle_in/2, handle_info/2, terminate/2]).

all() -> [idle_output_and_owner, five_item_arrays_are_asgi,
          native_channels_are_explicit, mounted_legacy_topics_are_isolated,
          shutdown_awaits_asgi_cleanup].

init_per_suite(Config) ->
    application:load(hornbeam),
    application:set_env(hornbeam, context_pool_size, 2),
    application:set_env(erlang_python, num_contexts, 2),
    {ok, _} = application:ensure_all_started(hornbeam),
    Config.

end_per_suite(_Config) ->
    application:stop(hornbeam).

init_per_testcase(Test, Config) ->
    {ok, Socket} = gen_tcp:listen(0, [{ip, {127, 0, 0, 1}}, {active, false}]),
    {ok, {_, Port}} = inet:sockname(Socket),
    ok = gen_tcp:close(Socket),
    Options = #{bind => iolist_to_binary(io_lib:format("127.0.0.1:~p", [Port])),
                worker_class => asgi, context_mode => worker, num_contexts => 2,
                num_acceptors => 2, lifespan => off,
                pythonpath => [test_apps_dir()]},
    Selected = case Test of
        native_channels_are_explicit -> Options#{websocket_protocol => channels};
        _ -> Options
    end,
    case Test of
        shutdown_awaits_asgi_cleanup ->
            Marker = filename:join(proplists:get_value(priv_dir, Config), "ws-shutdown.log"),
            hornbeam:register_function(ws_shutdown_record, fun([Event]) ->
                file:write_file(Marker, [Event, <<"\n">>], [append])
            end),
            ok = hornbeam:start("ws_shutdown_app:app", Options#{lifespan => on});
        mounted_legacy_topics_are_isolated ->
            ok = hornbeam:start(Selected#{mounts => [
                {<<"/">>, "ws_continuous_patch_app:app", #{worker_class => asgi}},
                {<<"/public">>, "ws_continuous_patch_app:app", #{worker_class => asgi}},
                {<<"/other">>, "ws_continuous_patch_app:app", #{worker_class => asgi}}]});
        _ -> ok = hornbeam:start("ws_continuous_patch_app:app", Selected)
    end,
    [{port, Port} | Config].

end_per_testcase(shutdown_awaits_asgi_cleanup, _Config) ->
    hornbeam:stop(),
    hornbeam:unregister_function(ws_shutdown_record);
end_per_testcase(_Test, _Config) ->
    hornbeam:stop().

test_apps_dir() ->
    Lib = filename:dirname(filename:dirname(code:which(hornbeam))),
    Src = filename:join(Lib, "src"),
    case file:read_link(Src) of
        {ok, Link} ->
            Root = filename:dirname(filename:absname(filename:join(Lib, Link))),
            filename:join(Root, "test/test_apps");
        {error, _} -> filename:absname("test/test_apps")
    end.

shutdown_awaits_asgi_cleanup(Config) ->
    Client = connect(Config, "/shutdown"),
    try
        ?assertEqual({text, <<"ready">>}, next_frame(Client)),
        ok = hornbeam:stop(),
        Marker = filename:join(proplists:get_value(priv_dir, Config), "ws-shutdown.log"),
        ?assertEqual({ok, <<"closed\nshutdown\n">>}, file:read_file(Marker)),
        ?assertEqual([], ets:tab2list(hornbeam_ws_tasks))
    after ws:close(Client, 1000) end.

connect(Config) ->
    connect(Config, "/ws-patch").

connect(Config, Path) ->
    Port = proplists:get_value(port, Config),
    URL = iolist_to_binary(io_lib:format("ws://127.0.0.1:~p~s", [Port, Path])),
    {ok, Client} = ws:connect(URL, #{handler => ?MODULE, handler_opts => self(),
                                   timeout => 5000}),
    Client.

next_frame(Client) ->
    receive
        {ws_patch_frame, Client, Frame} -> Frame
    after 3000 -> error(websocket_frame_timeout)
    end.

initial_frames(Client) ->
    Frames = [next_frame(Client) || _ <- lists:seq(1, 3)],
    ?assertEqual(lists:sort([{text, <<"welcome">>}, {text, <<"owner-direct">>},
                             {text, <<"idle-push">>}]), lists:sort(Frames)).

mounted_legacy_topics_are_isolated(Config) ->
    Root = connect(Config, "/chat/private"),
    try
        Public = connect(Config, "/public/chat/private"),
        try
            Other = connect(Config, "/other/chat/private"),
            try
                lists:foreach(fun initial_frames/1, [Root, Public, Other]),
                [RootPid] = hornbeam_pubsub:get_members(<<"chat:private">>),
                [PublicPid] = hornbeam_pubsub:get_members(<<"public:chat:private">>),
                [OtherPid] = hornbeam_pubsub:get_members(<<"other:chat:private">>),
                ?assertEqual(3, length(lists:usort([RootPid, PublicPid, OtherPid]))),
                lists:foreach(fun({Client, Topic, Message}) ->
                    ?assertEqual(1, hornbeam_pubsub:publish(Topic, Message)),
                    ?assertEqual({text, Message}, next_frame(Client))
                end, [{Root, <<"chat:private">>, <<"private-root">>},
                      {Public, <<"public:chat:private">>, <<"public-only">>},
                      {Other, <<"other:chat:private">>, <<"other-only">>}])
            after ws:close(Other, 1000)
            end
        after ws:close(Public, 1000)
        end
    after ws:close(Root, 1000)
    end.

idle_output_and_owner(Config) ->
    Client = connect(Config),
    try
        %% No inbound application frame is sent before all three outputs.
        initial_frames(Client),
        Bytes = <<"caf", 195, 169, 233, 155, 170>>,
        ok = ws:send(Client, {binary, Bytes}),
        ?assertEqual({binary, Bytes}, next_frame(Client))
    after ws:close(Client, 1000)
    end.

five_item_arrays_are_asgi(Config) ->
    Client = connect(Config),
    try
        initial_frames(Client),
        Join = <<"[\"1\",\"1\",\"lv:probe\",\"phx_join\",{}]">>,
        ok = ws:send(Client, {text, Join}),
        ?assertEqual({text, <<"echo:", Join/binary>>}, next_frame(Client)),
        Heartbeat = <<"[null,\"2\",\"hornbeam\",\"heartbeat\",{}]">>,
        ok = ws:send(Client, {text, Heartbeat}),
        ?assertEqual({text, <<"echo:", Heartbeat/binary>>}, next_frame(Client))
    after ws:close(Client, 1000)
    end.

native_channels_are_explicit(Config) ->
    ?assertEqual(channels, hornbeam_config:get_config(websocket_protocol)),
    Client = connect(Config),
    try
        ok = ws:send(Client,
            {text, <<"[null,\"2\",\"hornbeam\",\"heartbeat\",{}]">>}),
        %% A default ASGI accept/welcome here would fail the match.
        {text, Reply} = next_frame(Client),
        ?assertMatch([null, <<"2">>, <<"hornbeam">>, <<"hb_reply">>, _],
                     json:decode(Reply))
    after ws:close(Client, 1000)
    end.

init(_Req, Observer) -> {ok, Observer}.
handle_in(Frame, Observer) ->
    Observer ! {ws_patch_frame, self(), Frame},
    {ok, Observer}.
handle_info(_Info, Observer) -> {ok, Observer}.
terminate(_Reason, _Observer) -> ok.
