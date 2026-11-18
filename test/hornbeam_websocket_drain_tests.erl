-module(hornbeam_websocket_drain_tests).
-include_lib("eunit/include/eunit.hrl").

with_table(Test) ->
    Table = ets:new(hornbeam_ws_tasks, [named_table, public, set]),
    try Test() after ets:delete(Table) end.

empty_drain_test() ->
    with_table(fun() -> ?assertEqual(ok, hornbeam_websocket:drain()) end).

awaits_task_completion_test() ->
    with_table(fun() ->
        Parent = self(),
        Task = spawn(fun() ->
            receive shutdown -> Parent ! closing end,
            receive finish -> hornbeam_websocket:unregister_task(self()) end
        end),
        hornbeam_websocket:register_task(Task),
        Waiter = spawn(fun() -> hornbeam_websocket:drain(), Parent ! drained end),
        try
            receive closing -> ok after 1000 -> error(no_disconnect) end,
            receive drained -> error(early_drain) after 20 -> ok end,
            Task ! finish,
            receive drained -> ok after 1000 -> error(drain_timeout) end,
            ?assertEqual([], ets:tab2list(hornbeam_ws_tasks))
        after exit(Task, kill), exit(Waiter, kill) end
    end).

dead_task_does_not_block_test() ->
    with_table(fun() ->
        {Task, Ref} = spawn_monitor(fun() -> ok end),
        receive {'DOWN', Ref, process, Task, normal} -> ok end,
        hornbeam_websocket:register_task(Task),
        ?assertEqual(ok, hornbeam_websocket:drain())
    end).

shared_shutdown_deadline_test_() ->
    {timeout, 10, fun() ->
        with_table(fun() ->
            Tasks = [spawn(fun() -> receive finish -> ok end end) || _ <- [1, 2]],
            lists:foreach(fun hornbeam_websocket:register_task/1, Tasks),
            Start = erlang:monotonic_time(millisecond),
            try
                ?assertEqual(ok, hornbeam_websocket:drain()),
                Elapsed = erlang:monotonic_time(millisecond) - Start,
                ?assert(Elapsed >= 5900),
                ?assert(Elapsed < 8000)
            after lists:foreach(fun(Pid) -> exit(Pid, kill) end, Tasks) end
        end)
    end}.
