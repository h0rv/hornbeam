%% Run in its own VM: initializes the existing Python NIF, not a mock.
-module(hornbeam_services_native_bytes_tests).
-include_lib("eunit/include/eunit.hrl").

native_byte_conversion_test() ->
    ok = py_nif:init(),
    {ok, Context, _} = py_nif:context_create(worker),
    {ok, State} = hornbeam_state:start_link(),
    {ok, Pubsub} = hornbeam_pubsub:start_link(),
    {ok, Services} = hornbeam_services:start_link(),
    try
        ok = py_nif:context_exec(Context, <<"import erlang">>),
        {<<"ok">>, Channel} = hornbeam_services:call(<<"subscribe">>,
            [self(), <<1:128>>, <<"native-bytes">>, 1024]),
        lists:foreach(fun(Bytes) ->
            1 = hornbeam_pubsub:publish(<<"native-bytes">>, Bytes),
            %% This calls the same C receive and converter as Channel.try_receive.
            ?assertEqual({ok, true}, py_nif:context_eval(Context,
                <<"(lambda event, expected: isinstance(event, tuple) and "
                  "event[0] in ('event', b'event') and type(event[1]) is bytes "
                  "and event[1] == expected)(erlang._channel_try_receive(channel), expected)">>,
                #{<<"channel">> => Channel, <<"expected">> => {bytes, Bytes}})),
            {<<"ok">>, true} = hornbeam_services:call(<<"cache_set">>,
                [<<"n">>, <<"k">>, Bytes, -1]),
            Response = hornbeam_services:call(<<"cache_get">>, [<<"n">>, <<"k">>, -1]),
            ?assertEqual({ok, true}, py_nif:context_eval(Context,
                <<"type(response[1]) is bytes and response[1] == expected">>,
                #{<<"response">> => Response, <<"expected">> => {bytes, Bytes}}))
        end, [<<>>, <<"valid UTF8">>, <<195, 169>>, <<0, 255>>]),
        lists:foreach(fun({Action, Args}) ->
            Ack = hornbeam_services:call(Action, Args),
            ?assertEqual({ok, true}, py_nif:context_eval(Context,
                <<"response[1] is True">>, #{<<"response">> => Ack}))
        end, [{<<"cache_set">>, [<<"n">>, <<"k">>, <<>>, -1]},
              {<<"cache_delete">>, [<<"n">>, <<"k">>]},
              {<<"cache_clear">>, [<<"n">>]},
              {<<"unsubscribe">>, [self(), <<1:128>>]}])
    after
        gen_server:stop(Services), gen_server:stop(Pubsub), gen_server:stop(State),
        gen_server:stop(hornbeam_pubsub_scope), py_nif:context_destroy(Context)
    end.
