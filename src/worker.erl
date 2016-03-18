-module(worker).
-export([init/1]).

init(Manager) ->
    io:format("Spawning new process: ~p~n", [self()]),
    Manager ! {self(), idle},
    loop(Manager).

loop(Manager) ->
    receive
        {From, map, Map, File} ->
            map(Map, File),
            timer:sleep(1000),
            Manager ! {self(), idle}
    end,
    loop(Manager).

map(Map, File) ->
    {ok, Data} = file:read_file("../generated/split" ++ integer_to_list(File) ++ ".txt"),
    Result = Map(File, Data),
    io:format("Read data during map: ~p~n", [Result]).
