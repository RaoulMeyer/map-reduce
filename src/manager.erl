-module(manager).
-export([init/0]).
-define(DELIMITER, <<"\r\n">>).
-define(WORKER_COUNT, 10).


%%% init %%%
init() ->
    init(
        fun(_, Value) -> [{Word, <<"1">>} || Word <- binary:split(Value, [<<" ">>], [global]), Word =/= <<>>] end,
        fun(Key, Values) -> {Key, list_to_binary(integer_to_list(lists:foldl(fun(X, Sum) -> list_to_integer(binary_to_list(X)) + Sum end, 0, Values)))} end
    ).

init(M, R) ->
    logger:logger(),
    logger:log(io_lib:format("Process start at ~p", [calendar:local_time()])),
    split(M, R).


%%% split %%%
split(M, R) ->
    logger:log("Splitting input into blocks"),
    InputFilesCount = save_array_to_files(
        filter_empty_lines(
            read_lines("../input/27603.male.24.Advertising.Sagittarius.xml")
        )
    ),
    logger:log(io_lib:format("Input was split into ~p separate blocks", [InputFilesCount])),
    map(M, R, InputFilesCount).

read_lines(FileName) ->
    {ok, Data} = file:read_file(FileName),
    binary:split(Data, [?DELIMITER], [global]).

filter_empty_lines(Lines) ->
    lists:filter(fun(X) -> string:strip(binary_to_list(X)) =/= "" end, Lines).

save_array_to_files(Data) -> save_array_to_files(Data, 0).
save_array_to_files([], Counter) -> Counter - 1;
save_array_to_files([First | Rest], Counter) when First =/= <<"">> ->
    file:write_file("../generated/split" ++ integer_to_list(Counter) ++ ".txt", First),
    save_array_to_files(Rest, Counter + 1);
save_array_to_files([_ | Rest], Counter) ->
    save_array_to_files(Rest, Counter).


%%% map %%%
map(M, R, InputFilesCount) ->
    Workers = spawn_workers(min(?WORKER_COUNT, InputFilesCount)),
    logger:log(io_lib:format("Spawned ~p workers, starting map phase", [length(Workers)])),
    map(M, R, 0, InputFilesCount, Workers).

map(_, R, ProcessedFiles, TotalFiles, Workers) when ProcessedFiles == TotalFiles + 1 ->
    logger:log("Map phase done, starting partitioning"),
    sort(R, Workers);
map(M, R, ProcessedFiles, TotalFiles, Workers) ->
    receive
        {From, idle} ->
            logger:log(io_lib:format("Idle worker detected, sending file number ~p to ~p~n", [ProcessedFiles, From])),
            From ! {self(), map, M, ProcessedFiles},
            UpdatedProcessedFiles = ProcessedFiles + 1;
        _ ->
            UpdatedProcessedFiles = ProcessedFiles
    end,
    map(M, R, UpdatedProcessedFiles, TotalFiles, Workers).

spawn_workers(0) -> [];
spawn_workers(Count) ->
    WorkerPid = spawn(worker, init, [self()]),
    AllWorkerPids = [WorkerPid] ++ spawn_workers(Count - 1),
    AllWorkerPids.

%%% sort %%%
sort(R, Workers) ->
    message_workers({self(), sort}, Workers),
    sort(R, Workers, 0, [], []).

sort(R, Workers, TotalSorted, MinValues, MaxValues) when length(Workers) == TotalSorted ->
    partition(R, Workers, MinValues, MaxValues);
sort(R, Workers, TotalSorted, MinValues, MaxValues) ->
    receive
        {_, sorted, Min, Max} ->
            NewMinValues = MinValues ++ [Min],
            NewMaxValues = MaxValues ++ [Max]
    end,
    sort(R, Workers, TotalSorted + 1, NewMinValues, NewMaxValues).

message_workers(_, []) -> ok;
message_workers(Message, [FirstWorker | Rest]) ->
    FirstWorker ! Message,
    message_workers(Message, Rest).


%%% partition %%%
partition(R, Workers, MinValues, MaxValues) ->
    Partitioning = build_partitioning(Workers, lists:min(MinValues), lists:max(MaxValues)),
    message_workers({self(), partition, Partitioning}, Workers),
    logger:log("Partitioning sent, workers are sending data to target workers"),
    timer:sleep(1000),
    reduce(R, Workers).

build_partitioning(Workers, Min, Max) ->
    build_partition_for_workers(Workers, length(Workers), Min, Max, 0).

build_partition_for_workers([], _, _, _, _) -> [];
build_partition_for_workers([FirstWorker | Rest], WorkerCount, Min, Max, Position) ->
    SmallestLetter = lists:nth(1, binary_to_list(Min)) * 256 - 1,
    LargestLetter = lists:nth(1, binary_to_list(Max)) * 256 + 1,
    StartingLetters = SmallestLetter + (LargestLetter - SmallestLetter) / WorkerCount * Position,
    EndingLetters = SmallestLetter + (LargestLetter - SmallestLetter) / WorkerCount * (Position + 1),
    [{FirstWorker, integer_to_two_letters(StartingLetters), integer_to_two_letters(EndingLetters)}]
        ++ build_partition_for_workers(Rest, WorkerCount, Min, Max, Position + 1).

integer_to_two_letters(Integer) ->
    [trunc(Integer / 256), trunc(Integer) rem 256].

%%% reduce %%%
reduce(R, Workers) ->
    logger:log("Starting reduce phase, waiting for all reducers to be done"),
    message_workers({self(), reduce, R}, Workers),
    merge(Workers, 0).

%%% merge %%%
merge(Workers, TotalReady) when length(Workers) == TotalReady -> cleanup(Workers);
merge(Workers, TotalReady) ->
    receive
        {_, reducedone, FileNumber} ->
            {ok, Data} = file:read_file("../generated/reduced" ++ integer_to_list(FileNumber) ++ ".txt"),
            file:write_file("../output/output.txt", Data, [append])
    end,
    merge(Workers, TotalReady + 1).

%%% cleanup %%%
cleanup(Workers) ->
    logger:log("Output generated, cleaning up threads"),
    cleanup_workers(Workers),
    flush(),
    logger:log(io_lib:format("Process end at ~p", [calendar:local_time()])),
    ok.

cleanup_workers([]) -> ok;
cleanup_workers([FirstWorker | Rest]) ->
    exit(FirstWorker, kill),
    cleanup_workers(Rest).
flush() ->
    receive
        _ -> flush()
    after
        0 -> ok
    end.
