-module(worker).
-export([init/1]).
-define(DELIMITER, <<" \r\n">>).

init(Manager) ->
    Id = erlang:unique_integer(),
    logger:log(io_lib:format("Spawning new process: ~p~n", [self()])),
    Manager ! {self(), idle},
    loop(Manager, Id).

loop(Manager, Id) ->
    receive
        {Manager, map, Map, File} ->
            map(Map, File, Id),
            Manager ! {self(), idle},
            loop(Manager, Id);
        {Manager, partition, Partitioning, WorkerCount} ->
            send_partitioned_data(Partitioning, WorkerCount, get_mapped_items(Id)),
            loop(Manager, Id, [])
    end.

loop(Manager, Id, Data) ->
    receive
        {_, partitiondata, ReceivedData} ->
            NewData = Data ++ ReceivedData;
        {Manager, reduce, Reduce} ->
            Combined = combine(Data),
            Result = lists:map(fun({Key, Values}) -> Reduce(Key, Values) end, Combined),
            save_reduced_result(Id, Result),
            Manager ! {self(), reducedone, Id},
            NewData = Data
    end,
    loop(Manager, Id, NewData).


map(Map, File, Id) ->
    {ok, Data} = file:read_file("../generated/split" ++ integer_to_list(File) ++ ".txt"),
    Result = Map(File, Data),
    file:write_file("../generated/mapped" ++ integer_to_list(Id) ++ ".txt", key_value_list_to_string(Result, ?DELIMITER), [append]).

key_value_list_to_string([], _) -> [];
key_value_list_to_string([Item | Rest], Delimiter) ->
    {Key, Value} = Item,
    [Key] ++ binary_to_list(Delimiter) ++ [Value] ++ binary_to_list(?DELIMITER) ++ key_value_list_to_string(Rest, Delimiter).


get_mapped_items(Id) ->
    {ok, Data} = file:read_file("../generated/mapped" ++ integer_to_list(Id) ++ ".txt"),
    to_key_value_list(
        binary:split(Data, ?DELIMITER, [global])
    ).

to_key_value_list([]) -> [];
to_key_value_list([<<>>]) -> [];
to_key_value_list([Key, Value | Rest]) ->
    [{Key, Value}] ++ to_key_value_list(Rest).

send_partitioned_data([], _, _) -> [];
send_partitioned_data([Partition | Rest], WorkerCount, Items) ->
    {Target, HashValue} = Partition,
    Data = [{Key, Value} || {Key, Value} <- Items, erlang:phash2(Key, WorkerCount) == HashValue],
    Target ! {self(), partitiondata, Data},
    send_partitioned_data(Rest, WorkerCount, Items).

combine(Data) ->
    logger:log(io_lib:format("Combining data ~p", [length(Data)])),
    Keys = lists:usort([Key || {Key, _} <- Data]),
    combine_by_key(Keys, Data).

combine_by_key([], _) -> [];
combine_by_key([Key | Rest], Data) ->
    [{Key, [Value || {ItemKey, Value} <- Data, ItemKey == Key]}]
        ++ combine_by_key(Rest, Data).

save_reduced_result(Id, Result) ->
    file:write_file("../generated/reduced" ++ integer_to_list(Id) ++ ".txt", key_value_list_to_string(Result, <<" - ">>)).
