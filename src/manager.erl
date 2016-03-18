-module(manager).
-export([init/0]).
-define(DELIMITER, <<"\r\n">>).

%%% init %%%
init() ->
    init(
        fun(Key, Value) -> [{Word, 1} || Word <- binary:split(Value, [<<" ">>], [global])] end,
        fun() -> ok end
    ).

init(M, R) ->
    split(M, R).


%%% split %%%
split(M, R) ->
    InputFilesCount = save_array_to_files(
        read_lines("../input/test.txt")
    ),
    map(M, R, InputFilesCount).

read_lines(FileName) ->
    {ok, Data} = file:read_file(FileName),
    binary:split(Data, [?DELIMITER], [global]).

save_array_to_files(Data) -> save_array_to_files(Data, 0).
save_array_to_files([], Counter) -> Counter - 1;
save_array_to_files([First | Rest], Counter) ->
    file:write_file("../generated/split" ++ integer_to_list(Counter) ++ ".txt", First),
    save_array_to_files(Rest, Counter + 1).


%%% map %%%
map(M, R, InputFilesCount) ->
    Workers = spawn_workers(1),
    map(M, R, 0, InputFilesCount, Workers).

map(M, R, ProcessedFiles, TotalFiles, Workers) when ProcessedFiles == TotalFiles + 1 ->
    timer:sleep(1000),
    cleanup_workers(Workers),
    sort(R, TotalFiles, Workers);
map(M, R, ProcessedFiles, TotalFiles, Workers) ->
    receive
        {From, idle} ->
            io:format("Idle worker detected, sending file number ~p~n", [ProcessedFiles]),
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

cleanup_workers([]) -> ok;
cleanup_workers([FirstWorker | Rest]) ->
    exit(FirstWorker, kill),
    cleanup_workers(Rest).

%%% sort %%%
sort(R, TotalFiles, Workers) ->
    ok.
