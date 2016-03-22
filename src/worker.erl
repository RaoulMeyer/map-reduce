-module(worker).
-export([init/1]).
-define(DELIMITER, <<"\r\n">>).

init(Manager) ->
    {A, B, C} = now(),
    random:seed(A, B, C),
    Id = random:uniform(100000),
    io:format("Spawning new process: ~p~n", [self()]),
    timer:sleep(2000),
    Manager ! {self(), idle},
    loop(Manager, Id).

loop(Manager, Id) ->
    receive
        {From, map, Map, File} ->
            map(Map, File, Id),
            timer:sleep(1000),
            Manager ! {self(), idle},
            loop(Manager, Id);
        {From, sort} ->
            Sorted = sort(Id),
            {Min, _} = lists:nth(1, Sorted),
            {Max, _} = lists:last(Sorted),
            Manager ! {self(), sorted, Min, Max},
            loop(Manager, Id);
        {From, partition, Partitioning} ->
            send_partitioned_data(Partitioning, get_mapped_items(Id)),
            loop(Manager, Id, [])
    end.

loop(Manager, Id, Data) ->
    receive
        {From, partitiondata, ReceivedData} ->
            NewData = Data ++ ReceivedData;
        {From, reduce, Reduce} ->
            Combined = combine(sort_mapped_items(Data)),
            Result = lists:map(fun({Key, Values}) -> Reduce(Key, Values) end, Combined),
            save_reduced_result(Id, Result),
            NewData = Data
    end,
    loop(Manager, Id, NewData).


map(Map, File, Id) ->
    {ok, Data} = file:read_file("../generated/split" ++ integer_to_list(File) ++ ".txt"),
    Result = Map(File, Data),
    file:write_file("../generated/mapped" ++ integer_to_list(Id) ++ ".txt", key_value_list_to_string(Result), [append]),
    io:format("Read data with id ~p during map: ~p~n", [Id, Result]).

key_value_list_to_string([]) -> [];
key_value_list_to_string([Item | Rest]) ->
    {Key, Value} = Item,
    [Key] ++ binary_to_list(?DELIMITER) ++ [Value] ++ binary_to_list(?DELIMITER) ++ key_value_list_to_string(Rest).


sort(Id) ->
    io:format("Id: ~p~n", [Id]),
    timer:sleep(1000),
    Items = get_mapped_items(Id),
    SortedItems = sort_mapped_items(Items),
    io:format("Sorted items: ~p~n", [SortedItems]),
    SortedItems.

get_mapped_items(Id) ->
    {ok, Data} = file:read_file("../generated/mapped" ++ integer_to_list(Id) ++ ".txt"),
    to_key_value_list(
        binary:split(Data, ?DELIMITER, [global])
    ).

to_key_value_list([]) -> [];
to_key_value_list([<<>>]) -> [];
to_key_value_list([Key, Value | Rest]) ->
    [{Key, Value}] ++ to_key_value_list(Rest).

sort_mapped_items([]) -> [];
sort_mapped_items([Pivot | Rest]) ->
    {PivotKey, _} = Pivot,
    sort_mapped_items([{ItemKey, ItemValue} || {ItemKey, ItemValue} <- Rest, ItemKey =< PivotKey])
	++ [Pivot] ++
    sort_mapped_items([{ItemKey, ItemValue} || {ItemKey, ItemValue} <- Rest, ItemKey > PivotKey]).

send_partitioned_data([], _) -> [];
send_partitioned_data([Partition | Rest], Items) ->
    {Target, From, To} = Partition,
    Data = [{Key, Value} || {Key, Value} <- Items, From < binary_to_list(Key), binary_to_list(Key) < To],
    Target ! {self(), partitiondata, Data},
    send_partitioned_data(Rest, Items).

combine(Data) ->
    Keys = lists:usort([Key || {Key, _} <- Data]),
    combine_by_key(Keys, Data).

combine_by_key([], _) -> [];
combine_by_key([Key | Rest], Data) ->
    [{Key, [Value || {ItemKey, Value} <- Data, ItemKey == Key]}]
        ++ combine_by_key(Rest, Data).

save_reduced_result(Id, Result) ->
    io:format("Reduced: ~p~n", [key_value_list_to_string(Result)]),
    file:write_file("../generated/reduced" ++ integer_to_list(Id) ++ ".txt", key_value_list_to_string(Result)).
