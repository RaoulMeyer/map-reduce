-module(logger).
-export([log/1, logger/0, logger_loop/1]).

logger() ->
    Logger = whereis(logger),
    if
        Logger == undefined ->
            Timestamp = erlang:system_time(milli_seconds),
            register(logger, spawn(logger, logger_loop, [Timestamp]));
        true ->
            ok
    end.

log(Message) ->
    whereis(logger) ! {self(), log, Message}.

logger_loop(Timestamp) ->
    receive
        {From, log, Message} ->
            file:write_file("../logs/log" ++ integer_to_list(Timestamp) ++ ".txt", io_lib:format("~p", [From]) ++ ": " ++ Message ++ <<"\r\n">>, [append]),
            io:format(io_lib:format("~p", [From]) ++ ": " ++ Message ++ "\n")
    end,
    logger_loop(Timestamp).
