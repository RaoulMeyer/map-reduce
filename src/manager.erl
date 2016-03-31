-module(manager).
-export([init/0]).
-define(DELIMITER, <<"\r\n">>).
-define(WORKER_COUNT, 200).
-define(CHUNK_SIZE, 128000).


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
    Filename = "../input/input.txt",
    logger:log("Splitting input into blocks"),
    {ok, InputDevice} = file:open(Filename, [read]),
    InputFilesCount = write_chunk_to_file(InputDevice, -1, ?CHUNK_SIZE + 1),
    logger:log(io_lib:format("Input was split into ~p separate blocks", [InputFilesCount])),
    map(M, R, InputFilesCount).

write_chunk_to_file(InputDevice, FileNumber, ContentLength) when ContentLength > ?CHUNK_SIZE ->
    file:write_file("../generated/split" ++ integer_to_list(FileNumber + 1) ++ ".txt", ""),
    logger:log(io_lib:format("Splitting chunk ~p", [FileNumber + 1])),
    write_chunk_to_file(InputDevice, FileNumber + 1, 0);
write_chunk_to_file(InputDevice, FileNumber, ContentLength) ->
    case file:read(InputDevice, 10000) of
            eof  ->
                file:close(InputDevice),
                FileNumber + 1;
            {ok, Line} ->
                file:write_file("../generated/split" ++ integer_to_list(FileNumber) ++ ".txt", Line, [append]),
                write_chunk_to_file(InputDevice, FileNumber, ContentLength + length(Line))
        end.


%%% map %%%
map(M, R, InputFilesCount) ->
    Workers = spawn_workers(min(?WORKER_COUNT, InputFilesCount)),
    logger:log(io_lib:format("Spawned ~p workers, starting map phase", [length(Workers)])),
    map(M, R, 0, InputFilesCount, Workers).

map(_, R, ProcessedFiles, TotalFiles, Workers) when ProcessedFiles == TotalFiles ->
    logger:log("Map phase done, starting partitioning"),
    partition(R, Workers);
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

message_workers(_, []) -> ok;
message_workers(Message, [FirstWorker | Rest]) ->
    FirstWorker ! Message,
    message_workers(Message, Rest).


%%% partition %%%
partition(R, Workers) ->
    Partitioning = build_partitioning(Workers),
    message_workers({self(), partition, Partitioning, length(Workers)}, Workers),
    logger:log("Partitioning sent, workers are sending data to target workers"),
    timer:sleep(2000),
    reduce(R, Workers).

build_partitioning(Workers) ->
    build_partition_for_workers(Workers, 0).

build_partition_for_workers([], _) -> [];
build_partition_for_workers([FirstWorker | Rest], Position) ->
    [{FirstWorker, Position}]
        ++ build_partition_for_workers(Rest, Position + 1).

%%% reduce %%%
reduce(R, Workers) ->
    logger:log("Starting reduce phase, waiting for all reducers to be done"),
    message_workers({self(), reduce, R}, Workers),
    file:write_file("../output/output.txt", ""),
    merge(Workers, 0).

%%% merge %%%
merge(Workers, TotalReady) when length(Workers) == TotalReady -> cleanup(Workers);
merge(Workers, TotalReady) ->
    receive
        {_, reducedone, FileNumber} ->
            logger:log(io_lib:format("Filenumber ~p ready with reduce, merging", [FileNumber])),
            append_file_to_output("../generated/reduced" ++ integer_to_list(FileNumber) ++ ".txt", "../output/output.txt")
    end,
    merge(Workers, TotalReady + 1).

append_file_to_output(InputPath, OutputPath) ->
    {ok, InputDevice} = file:open(InputPath, [read]),
    read_file_and_append(InputDevice, OutputPath),
    file:close(InputDevice).

read_file_and_append(Input, OutputPath) ->
    case file:read(Input, ?CHUNK_SIZE) of
        {ok, Data} ->
            file:write_file(OutputPath, Data, [append]),
            read_file_and_append(Input, OutputPath);
        _ ->
            ok
    end.


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
