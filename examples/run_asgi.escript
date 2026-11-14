#!/usr/bin/env escript
%%! +S 4:4
%% Launch one ASGI application using an already built runtime.
main([Runtime, App, Bind, LifespanText | PythonPaths]) ->
    Paths = filelib:wildcard(filename:join([Runtime, "_build/default/lib/*/ebin"])) ++
            filelib:wildcard(filename:join([Runtime, "_build/default/checkouts/*/ebin"])),
    ok = code:add_paths(Paths),
    Lifespan = case LifespanText of
        "on" -> on;
        "off" -> off;
        "auto" -> auto
    end,
    application:load(hornbeam),
    application:set_env(erlang_python, num_contexts, 4),
    application:set_env(erlang_python, event_loop_pool_size, 1),
    application:set_env(hornbeam, context_pool_size, 4),
    case application:ensure_all_started(hornbeam) of
        {ok, _} -> ok;
        StartError -> io:format("APPLICATION_START_ERROR ~p~n", [StartError]), halt(1)
    end,
    Options = #{worker_class => asgi, context_mode => worker, num_contexts => 4,
                num_acceptors => 4, bind => Bind, lifespan => Lifespan,
                timeout => 30000, websocket_timeout => 60000,
                websocket_protocol => asgi, pythonpath => PythonPaths},
    case hornbeam:start(App, Options) of
        ok -> io:format("HORNBEAM_READY~n"), timer:sleep(infinity);
        ServerError -> io:format("HORNBEAM_START_ERROR ~p~n", [ServerError]), halt(1)
    end;
main(_) ->
    io:format(standard_error, "Usage: runtime app bind lifespan pythonpath...~n", []),
    halt(2).
