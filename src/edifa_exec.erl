-module(edifa_exec).

%--- Exports -------------------------------------------------------------------

% API Functions
-export([start/2]).
-export([call/3]).
-export([terminate/1]).
-export([find_executables/2]).
-export([command/2, command/3]).


%--- Macros --------------------------------------------------------------------

-define(MAX_LINE_SIZE, 1024).


%--- Types ---------------------------------------------------------------------

-record(state, {
    terminating = false :: boolean(),
    parent :: pid(),
    exec_pid :: pid(),
    mod :: module(),
    sub :: term(),
    cmd_paths = #{} :: #{atom() => binary()},
    stdout_buffer = <<>> :: binary(),
    stderr_buffer = <<>> :: binary(),
    log_handler :: undefined | log_handler()
}).
-type state() :: #state{}.

-type command_input_data() :: eof | [iodata() | [eof]] | iodata().
-type command_input() :: {stdin, InputLine :: binary() | eof}.
-type command_output() :: {stdout, OuputLine :: binary()}
                        | {stderr, ErrorLine :: binary()}.
-type command_exit() :: {exit, {status, Status :: integer()}
                               | {signal, Signal :: integer()}
                               | {down, Reason :: term()}}.

-type log_event() :: {exec, Args :: [binary()]}
                   | command_input()
                   | command_output()
                   | command_exit()
                   | {result, term()}
                   | {error, term()}.
-type log_handler() :: fun((log_event()) -> ok).

-type continue_return() ::
    {continue, NewState :: term()}
  | {continue, Inputs :: command_input_data(), NewState :: term()}.

-type ok_return() ::
    {ok, NewState :: term()}
  | {ok, Result :: term(), NewState :: term()}.

-type deep_call_list() :: [call_spec() | command_handler() | deep_call_list()].
-type call_return() ::
    {call, [call_spec() | command_handler()],
     LastResult :: term(), NewState :: term()}.

-type error_return() ::
    {error, Reason :: term()} | {error, Reason :: term(), NewState :: term()}.

-type start_handler() :: fun((State :: term(), LastResult :: term()) ->
    continue_return() | ok_return() | call_return() | error_return()).

-type data_handler() :: fun((State :: term(), command_output()) ->
    continue_return() | ok_return() | call_return() | error_return()).

-type command_message() :: {start, LastResult :: term()}
                         | command_output()
                         | command_exit()
                         | {message, Msg :: term()}.
-type full_command_handler() :: fun((State :: term(), call_spec(),
                                    command_message()) ->
    continue_return() | ok_return() | call_return() | error_return()).
-type simple_command_handler() :: fun((State :: term(), LastResult :: term()) ->
    ok_return() | call_return() | error_return()).
-type command_handler() :: full_command_handler() | simple_command_handler().

-type exit_handler() :: fun((State :: term(), command_exit()) ->
    ok_return() | call_return() | error_return()).

-type call_spec() :: #{
    % The command tag to call, must have been returned from the init callback.
    cmd := atom(),
    % The command arguments.
    args => [string() | binary()],
    % The system environment variables.
    env => [string() | binary() |  clear
            | {Name :: string() | binary(),
               Val :: string() | binary() | false}],
    % Handler called by the default command handler when starting,
    % will not be called if `handler` is specified.
    on_start => start_handler(),
    % Handler called by the default command handler when receiving data from
    % the command, will not be called if `handler` is specified.
    on_data => data_handler(),
    % Handler called by the default command handler when the command exited,
    % will not be called if `handler` is specified.
    on_exit => exit_handler(),
    % Top level command handler, by specifying it, on_start, on_data and on_exit
    % will not be called.
    handler => command_handler(),

    % These are options for the default implementation of the command handler.
    % By specifying on_start, on_data, on_exit or handler, they may not be used.

    % The input data to send to the command just after starting by the default
    % start handler. Not used when specifying an explicit on_start handler.
    input => undefined | command_input_data(),
    % Define how the result should be extracted from the command output
    % by the default data handler. Not used when specifying an explicit on_data
    % handler, and an explicit on_exit handler would have to get the result
    % from the state key `result` and return it explicitly.
    result => undefined
            | {default, term()}
            | {match, iodata() | unicode:charlist()}
            | {match, iodata() | unicode:charlist(), re:options()}
            | {match, iodata() | unicode:charlist(), re:options(), fun((list()) -> term())}
            | {collect, stdout | stderr | both},
    % If not undefined, there is a result key in the state, and the command
    % exit with status 0, the result will be kept under the specified key in
    % the state. Not used when specifying an explicit exit handler.
    keep_result => undefined | atom(),
    % If true, the command will always succeed, and the result will be
    % `{error, Reason}` if an error happen. Default: false.
    error_as_result => undefined | boolean(),
    % Define how the error should be extracted from the command output
    % by the default data handler. Not used when specifying an explicit on_data
    % handler, and an explicit on_exit handler would have to get the result
    % from the state key `error` and return it explicitly.
    error => undefined
            | {default, term()}
            | {match, iodata() | unicode:charlist()}
            | {match, iodata() | unicode:charlist(), re:options()}
            | {match, iodata() | unicode:charlist(), re:options(), fun((list()) -> term())}
}.

-export_type([log_handler/0, call_spec/0]).


%--- API Functions -------------------------------------------------------------

start(Mod, Opts) ->
    Self = self(),
    Ref = make_ref(),
    Pid = spawn_link(fun() -> proc_init(Self, Ref, Mod, Opts) end),
    receive
        {Ref, Pid, ok} -> {ok, Pid};
        {Ref, Pid, {error, Reason}} -> {error, Reason};
        {'EXIT', Pid, Reason} -> {error, Reason}
    after
        5000 ->
            erlang:unlink(Pid),
            Pid ! terminate,
            {error, timeout}
    end.

call(Pid, Name, Args) ->
    call_proc(Pid, call, [Name, Args]).

terminate(Pid) ->
    MonRef = erlang:monitor(process, Pid),
    Pid ! terminate,
    receive
        {'DOWN', MonRef, process, Pid, normal} -> ok;
        {'DOWN', MonRef, process, Pid, Reason} -> {error, Reason}
    after
        5000 ->
            demonitor(MonRef, [flush]),
            {error, timeout}
    end.

-spec command(term(), atom(), [string() | binary()]) ->
    ok_return() | call_return().
command(State, Cmd, Args) ->
    command(State, [#{cmd => Cmd, args => Args}]).

-spec command(term(), call_spec() | deep_call_list()) ->
    ok_return() | call_return().
command(State, []) ->
    {ok, State};
command(State, CmdSpec) when is_map(CmdSpec) ->
    {call, [CmdSpec], undefined, State};
command(State, CmdSpecs) when is_list(CmdSpecs) ->
    {call, lists:flatten(CmdSpecs), undefined, State}.


%--- Internal Functions --------------------------------------------------------

find_executables([], Map) ->
    {ok, Map};
find_executables([Tag | Rest], Map) when is_atom(Tag) ->
    find_executables([{Tag, Tag} | Rest], Map);
find_executables([{Tag, Name} | Rest], Map) when is_atom(Tag) ->
    case os:find_executable(Name) of
        false -> {error, {command_not_found, Name}};
        Path -> find_executables(Rest, Map#{Tag => iolist_to_binary(Path)})
    end.

-spec cmd_path(state(), any()) -> binary().
cmd_path(#state{cmd_paths = Paths}, Tag) ->
    #{Tag := Path} = Paths,
    Path.

call_proc(Pid, Action, Args) ->
    CallRef = make_ref(),
    MonRef = erlang:monitor(process, Pid),
    Pid ! {Action, {CallRef, self()}, Args},
    receive
        {'DOWN', MonRef, process, Pid, Reason} ->
            {error, Reason};
        {CallRef, Result} ->
            demonitor(MonRef, [flush]),
            Result
    after
        5000 ->
            demonitor(MonRef, [flush]),
            {error, timeout}
    end.

proc_init(Parent, Ref, Mod, Opts) ->
    process_flag(trap_exit, true),
    LogHandler = maps:get(log_handler, Opts, undefined),
    case exec:start_link([]) of
        {error, Reason} ->
            Parent ! {Ref, self(), {error, Reason}};
        {ok, ExecPid} ->
            case Mod:init(Opts) of
                {error, Reason} ->
                    Parent ! {Ref, self(), {error, Reason}};
                {ok, Sub, Cmds} ->
                    case find_executables(Cmds, #{}) of
                        {error, Reason} ->
                            Mod:terminate(Sub, Reason),
                            Parent ! {Ref, self(), {error, Reason}};
                        {ok, CmdPaths} ->
                            State = #state{parent = Parent, exec_pid = ExecPid,
                                           log_handler = LogHandler,
                                           mod = Mod, sub = Sub,
                                           cmd_paths = CmdPaths},
                            Parent ! {Ref, self(), ok},
                            proc_loop(State)
                    end
            end
    end.

proc_loop(#state{mod = Mod, sub = Sub, parent = Parent} = State) ->
    receive
        {'EXIT', Parent, Reason} ->
            proc_terminate(State, Reason);
        {'EXIT', _Other, _Reason} ->
            proc_loop(State);
        {call, Caller, [Name, Args]} ->
            try apply(Mod, Name, [Sub | Args]) of
                Result -> proc_reply(Caller, State, Result)
            catch
                throw:Reason -> proc_reply(Caller, State, {error, Reason})
            end;
        terminate ->
            proc_terminate(State, normal)
    end.

proc_reply({CallRef, Pid}, State, {error, Reason, NewSub}) ->
    Pid ! {CallRef, {error, Reason}},
    proc_loop(State#state{sub = NewSub});
proc_reply({CallRef, Pid}, State, {error, Reason}) ->
    Pid ! {CallRef, {error, Reason}},
    proc_loop(State);
proc_reply({CallRef, Pid}, State, {ok, NewSub}) ->
    Pid ! {CallRef, ok},
    proc_loop(State#state{sub = NewSub});
proc_reply({CallRef, Pid}, State, {ok, Reply, NewSub}) ->
    Pid ! {CallRef, {ok, Reply}},
    proc_loop(State#state{sub = NewSub});
proc_reply({CallRef, Pid}, State, {call, CallSpecs, LastResult, NewSub}) ->
    case proc_command(State#state{sub = NewSub}, CallSpecs, LastResult) of
        terminated ->
            Pid ! {CallRef, {error, terminated}},
            terminated;
        {ok, Reply, NewState} ->
            Pid ! {CallRef, {ok, Reply}},
            proc_loop(NewState);
        {ok, NewState} ->
            Pid ! {CallRef, ok},
            proc_loop(NewState);
        {error, Reason, NewState} ->
            Pid ! {CallRef, {error, Reason}},
            proc_loop(NewState)
    end.

proc_terminate(#state{mod = Mod, sub = Sub} = State, Reason) ->
    State2 = State#state{terminating = true},
    case Mod:terminate(Sub, Reason) of
        ok -> terminated;
        {ok, _Sub2} -> terminated;
        {call, CallSpecs, LastResult, NewSub} ->
            State3 = State2#state{sub = NewSub},
            case proc_command(State3, CallSpecs, LastResult) of
                {ok, _Reply, _NewState} -> terminated;
                {ok, _NewState} -> terminated;
                {error, _Reason, _NewState} -> terminated
            end
    end.

proc_command(State, [Handler | NextCalls], LastResult)
  when is_function(Handler) ->
    proc_command_start(State, undefined, undefined,
                       Handler, NextCalls, LastResult);
proc_command(State, [CallSpec | NextCalls], LastResult) ->
    case proc_cmd_run(State, CallSpec) of
        {error, _Reason, _State2} = Error -> Error;
        {ok, CallRef, State2} ->
            Handler = make_handler(State2, CallSpec),
            proc_command_start(State2, CallSpec, CallRef,
                               Handler, NextCalls, LastResult)
    end.

validate_cmd_args(L) when is_list(L) ->
    validate_cmd_args(L, []).

validate_cmd_args([], Acc) ->
    lists:reverse(Acc);
validate_cmd_args([L | R], Acc) when is_list(L) ->
    validate_cmd_args(R, [L | Acc]);
validate_cmd_args([B | R], Acc) when is_binary(B) ->
    validate_cmd_args(R, [unicode:characters_to_list(B) | Acc]).

validate_cmd_env(L) ->
    validate_cmd_env(L, []).

validate_cmd_env([], Acc) ->
    lists:reverse(Acc);
validate_cmd_env([clear | R], Acc) ->
    validate_cmd_env(R, [clear | Acc]);
validate_cmd_env([{K, V} = Pair | R], Acc)
  when is_list(K) orelse is_binary(K),
       is_list(V) orelse is_binary(V) orelse V =:= false ->
    validate_cmd_env(R, [Pair | Acc]);
validate_cmd_env([L | R], Acc) when is_list(L) ->
    validate_cmd_env(R, [L | Acc]);
validate_cmd_env([B | R], Acc) when is_binary(B) ->
    validate_cmd_env(R, [unicode:characters_to_list(B) | Acc]).

validate_cmd_input(undefined) -> {[], false};
validate_cmd_input(eof) -> {[], true};
validate_cmd_input(<<>>) -> {[], false};
validate_cmd_input([]) -> {[], false};
validate_cmd_input(B) when is_binary(B) ->
    {split_data(B), false};
validate_cmd_input(L) when is_list(L) ->
    case lists:reverse(L) of
        [eof | Rest] ->
            {split_data(iolist_to_binary(lists:reverse(Rest))), true};
        _ ->
            {split_data(iolist_to_binary(L)), false}
    end.

proc_cmd_status(normal) ->
    {exit, {status, 0}};
proc_cmd_status({exit_status, Status}) ->
    case exec:status(Status) of
        {status, ExitStatus} -> {exit, {status, ExitStatus}};
        {signal, Signal, _Core} -> {exit, {signal, Signal}}
    end;
proc_cmd_status(Other) ->
    {exit, {down, Other}}.

split_stream_data(State = #state{stdout_buffer = Buff}, stdout, Data) ->
    {Lines, NewBuff} = split_data(<<Buff/binary, Data/binary>>, ?MAX_LINE_SIZE),
    {Lines, State#state{stdout_buffer = NewBuff}};
split_stream_data(State = #state{stderr_buffer = Buff}, stderr, Data) ->
    {Lines, NewBuff} = split_data(<<Buff/binary, Data/binary>>, ?MAX_LINE_SIZE),
    {Lines, State#state{stderr_buffer = NewBuff}}.

split_data(B) when is_binary(B) ->
    case split_data(B, ?MAX_LINE_SIZE) of
        {Lines, <<>>} -> Lines;
        {Lines, ExtraData} -> Lines ++ [ExtraData]
    end.

split_data(B, MaxLineSize) ->
    split_data(B, MaxLineSize, []).

split_data(B, MaxLineSize, Acc) ->
    case binary:split(B, <<"\n">>) of
        [Line, <<>>] when byte_size(Line) =< MaxLineSize ->
            {lists:reverse([<<Line/binary, "\n">> | Acc]), <<>>};
        [Line, Rest] when byte_size(Line) =< MaxLineSize ->
            split_data(Rest, MaxLineSize, [<<Line/binary, "\n">> | Acc]);
        [_Line, _Rest] ->
            <<Chunk:MaxLineSize/binary, Rest/binary>> = B,
            split_data(Rest, MaxLineSize, [Chunk | Acc]);
        [Data] when byte_size(Data) =< MaxLineSize ->
            {lists:reverse([Data | Acc]), Data};
        [Data] ->
            <<Chunk:MaxLineSize/binary, Rest/binary>> = Data,
            split_data(Rest, MaxLineSize, [Chunk | Acc])
    end.

proc_cmd_run(State, #{cmd := Cmd} = CallSpec) ->
    CmdPath = cmd_path(State, Cmd),
    Args = validate_cmd_args([CmdPath | maps:get(args, CallSpec, [])]),
    Env = validate_cmd_env(maps:get(env, CallSpec, [])),
    Opts = [{env, Env}, stdin, stdout, stderr, monitor],
    case exec:run(Args, Opts) of
        {error, Reason} ->
            {error, Reason, State};
        {ok, Pid, OsPid} ->
            {ok, {Pid, OsPid}, proc_log(State, {exec, Args})}
    end.

proc_cmd_send(State, _CallSpec, undefined, _Data) ->
    State;
proc_cmd_send(State, _CallSpec, {_, OsPid}, Data) ->
    {Lines, SendEof} = validate_cmd_input(Data),
    State2 = lists:foldl(fun(L, S) ->
        exec:send(OsPid, L),
        proc_log(S, {stdin, L})
    end, State, Lines),
    case SendEof of
        false -> State2;
        true ->
            exec:send(OsPid, eof),
            proc_log(State2, {stdin, eof})
    end.

proc_cmd_close(State, undefined) ->
    State;
proc_cmd_close(State, {Pid, _} = CallRef) ->
    case exec:stop(Pid) of
        {error, no_process} -> State;
        ok ->
            proc_cmd_close_consume(proc_log(State, closed), CallRef)
    end.

proc_flush_stream_data(State) ->
    proc_flush_stream_data(State, [stdout, stderr], []).

proc_flush_stream_data(State, [], Acc) ->
    {lists:reverse(Acc), State};
proc_flush_stream_data(State = #state{stdout_buffer = <<>>}, [stdout | Rest], Acc) ->
    proc_flush_stream_data(State, Rest, Acc);
proc_flush_stream_data(State = #state{stdout_buffer = Data}, [stdout | Rest], Acc) ->
    State2 = State#state{stdout_buffer = <<>>},
    proc_flush_stream_data(State2, Rest, [{stdout, Data} | Acc]);
proc_flush_stream_data(State = #state{stderr_buffer = <<>>}, [stderr | Rest], Acc) ->
    proc_flush_stream_data(State, Rest, Acc);
proc_flush_stream_data(State = #state{stderr_buffer = Data}, [stderr | Rest], Acc) ->
    State2 = State#state{stderr_buffer = <<>>},
    proc_flush_stream_data(State2, Rest, [{stderr, Data} | Acc]).

proc_cmd_close_consume(State, {Pid, OsPid} = CallRef) ->
    receive
        {Stream, OsPid, Data} when Stream =:= stdout; Stream =:= stderr ->
            {Lines, State2} = split_stream_data(State, Stream, Data),
            State3 = lists:foldl(fun(L, S) ->
                proc_log(S, {Stream, L})
            end, State2, Lines),
            proc_cmd_close_consume(State3, CallRef);
        {'DOWN', OsPid, process, Pid, Reason} ->
            {Events, State2} = proc_flush_stream_data(State),
            State3 = lists:foldl(fun(Event, S) ->
                proc_log(S, Event)
            end, State2, Events),
            proc_log(State3, proc_cmd_status(Reason))
    end.

proc_log(#state{log_handler = undefined} = State, _Data) -> State;
proc_log(#state{log_handler = LogHandler} = State, Data) ->
    catch LogHandler(Data),
    State.

proc_command_start(#state{sub = Sub} = State, _CallSpec, undefined,
                   Handler, NextCalls, LastResult)
  when is_function(Handler, 2) ->
    % Special handling for simplified handler
    try Handler(Sub, LastResult) of
        Result ->
            process_handler_exit(State, Result, Handler, NextCalls)
    catch
        throw:Reason ->
            process_handler_exit(State, {error, Reason}, Handler, NextCalls)
    end;
proc_command_start(State, CallSpec, CallRef,
                   Handler, NextCalls, LastResult)
  when is_function(Handler, 3) ->
    proc_call_result_handler(State, CallSpec, CallRef, Handler,
                             NextCalls, [], {start, LastResult}).

process_handler_exit(State, {ok, NewSub}, _Handler, []) ->
    {ok, State#state{sub = NewSub}};
process_handler_exit(State, {ok, Result, NewSub}, _Handler, []) ->
    proc_log(State, {result, Result}),
    {ok, Result, State#state{sub = NewSub}};
process_handler_exit(State, {error, Reason}, _Handler, _NextCalls) ->
    {error, Reason, proc_log(State, {error, Reason})};
process_handler_exit(State, {error, Reason, NewSub}, _Handler, _NextCalls) ->
    proc_log(State, {error, Reason}),
    {error, Reason, State#state{sub = NewSub}};
process_handler_exit(State, {call, CallSpecs, LastResult, NewSub},
                     _Handler, NextCalls) ->
    proc_command(State#state{sub = NewSub}, CallSpecs ++ NextCalls, LastResult);
process_handler_exit(State, {ok, NewSub}, _Handler, NextCalls) ->
    proc_command(State#state{sub = NewSub}, NextCalls, undefined);
process_handler_exit(State, {ok, Result, NewSub}, _Handler, NextCalls) ->
    proc_log(State, {result, Result}),
    proc_command(State#state{sub = NewSub}, NextCalls, Result).

process_handler_result(State, {continue, NewSub}, CallSpec, CallRef,
                       Handler, NextCalls, NextData) ->
    proc_command_consume(State#state{sub = NewSub},
                         CallSpec, CallRef, Handler, NextCalls, NextData);
process_handler_result(State, {continue, Input, NewSub}, CallSpec, CallRef,
                       Handler, NextCalls, NextData) ->
    State2 = proc_cmd_send(State#state{sub = NewSub}, CallSpec, CallRef, Input),
    proc_command_consume(State2, CallSpec, CallRef, Handler, NextCalls, NextData);
process_handler_result(State, {ok, NewSub}, _CallSpec, CallRef,
                       _Handler, [], _NextData) ->
    {ok, proc_cmd_close(State#state{sub = NewSub}, CallRef)};
process_handler_result(State, {ok, Result, NewSub}, _CallSpec, CallRef,
                       _Handler, [], _NextData) ->
    proc_log(State, {result, Result}),
    {ok, Result, proc_cmd_close(State#state{sub = NewSub}, CallRef)};
process_handler_result(State, {error, Reason}, _CallSpec, CallRef,
                       _Handler, _NextCalls, _NextData) ->
    {error, Reason, proc_cmd_close(proc_log(State, {error, Reason}), CallRef)};
process_handler_result(State, {error, Reason, NewSub}, _CallSpec, CallRef,
                       _Handler, _NextCalls, _NextData) ->
    proc_log(State, {error, Reason}),
    {error, Reason, proc_cmd_close(State#state{sub = NewSub}, CallRef)};
process_handler_result(State, {call, CallSpecs, LastResult, NewSub},
                       _CallSpec, CallRef, _Handler, NextCalls, _NextData) ->
    proc_command(proc_cmd_close(State#state{sub = NewSub}, CallRef),
                 CallSpecs ++ NextCalls, LastResult);
process_handler_result(State, {ok, NewSub}, _CallSpec, CallRef,
                       _Handler, NextCalls, _NextData) ->
    proc_command(proc_cmd_close(State#state{sub = NewSub}, CallRef),
                 NextCalls, undefined);
process_handler_result(State, {ok, Result, NewSub}, _CallSpec, CallRef,
                       _Handler, NextCalls, _NextData) ->
    proc_log(State, {result, Result}),
    proc_command(proc_cmd_close(State#state{sub = NewSub}, CallRef),
                 NextCalls, Result).

proc_command_consume(#state{terminating = Terminating, parent = Parent} = State,
                     CallSpec, {Pid, OsPid} = CallRef, Handler, NextCalls, []) ->
    receive
        {'EXIT', Parent, Reason} when not Terminating ->
            proc_terminate(proc_cmd_close(State, CallRef), Reason);
        terminate when not Terminating ->
            proc_terminate(proc_cmd_close(State, CallRef), normal);
        {'DOWN', OsPid, process, Pid, Reason} ->
            {DataEvents, State2} = proc_flush_stream_data(State),
            StatusEvent = proc_cmd_status(Reason),
            AllEvents = DataEvents ++ [StatusEvent],
            proc_command_consume(State2, CallSpec, undefined, Handler,
                                 NextCalls, AllEvents);
        {Stream, OsPid, Data} when Stream =:= stdout; Stream =:= stderr ->
            {Lines, State2} = split_stream_data(State, Stream, Data),
            DataEvents = [{Stream, D} || D <- Lines],
            proc_command_consume(State2, CallSpec, CallRef, Handler,
                                 NextCalls, DataEvents);
        OtherMsg ->
            proc_call_result_handler(State, CallSpec, CallRef,
                                     Handler, NextCalls, [],
                                     {message, OtherMsg})
    end;
proc_command_consume(State, CallSpec, _CallRef, Handler,
                     NextCalls, [{exit, _} = ExitEvent | _Rest]) ->
    proc_call_exit_handler(State, CallSpec, Handler, NextCalls, ExitEvent);
proc_command_consume(State, CallSpec, CallRef, Handler,
                     NextCalls, [Event | Rest]) ->
    proc_call_result_handler(State, CallSpec, CallRef,
                             Handler, NextCalls, Rest, Event).

proc_call_result_handler(State = #state{sub = Sub}, CallSpec, CallRef,
                         Handler, NextCalls, NextEvents, Event) ->
    State2 = proc_log(State, Event),
    try Handler(Sub, CallSpec, Event) of
        Result ->
            process_handler_result(State2, Result, CallSpec, CallRef,
                                   Handler, NextCalls, NextEvents)
    catch
        throw:Reason ->
            process_handler_result(State2, {error, Reason}, CallSpec, CallRef,
                                   Handler, NextCalls, NextEvents)
    end.

proc_call_exit_handler(State = #state{sub = Sub}, CallSpec,
                       Handler, NextCalls, Event) ->
    State2 = proc_log(State, Event),
    try Handler(Sub, CallSpec, Event) of
        Result ->
            process_handler_exit(State2, Result, Handler, NextCalls)
    catch
        throw:Reason ->
            process_handler_exit(State2, {error, Reason}, Handler, NextCalls)
    end.

make_handler(State, CallSpec) ->
    DefStartHandler = make_default_start_handler(State, CallSpec),
    StartHandler = maps:get(on_start, CallSpec, DefStartHandler),
    DefDataHandler = make_default_data_handler(State, CallSpec),
    DataHandler = maps:get(on_data, CallSpec, DefDataHandler),
    DefExitHandler = make_default_exit_handler(State, CallSpec),
    ExitHandler = maps:get(on_exit, CallSpec, DefExitHandler),
    make_messager_handler(StartHandler, DataHandler, ExitHandler).

make_default_start_handler(_State, #{input := Input})
  when Input =:= undefined; Input =:= <<>> ->
    fun(Sub, _LastResult) -> {continue, maps:remove(result, Sub)} end;
make_default_start_handler(_State, #{input := Input}) ->
    fun(Sub, _LastResult) -> {continue, Input, maps:remove(result, Sub)} end;
make_default_start_handler(_State, _CallSpec) ->
    fun(Sub, _LastResult) -> {continue, maps:remove(result, Sub)} end.

make_data_extractor(Key, {collect, StreamSpec}, _Default) ->
    fun
        (Sub, {Stream, Data})
          when StreamSpec =:= both; Stream =:= StreamSpec  ->
            Results = maps:get(Key, Sub, []),
            Sub#{Key => [Data | Results]};
        (Sub, _) ->
            Sub
    end;
make_data_extractor(Key, {match, Regexp}, _Default) ->
    {ok, MP} = re:compile(Regexp),
    fun(Sub, {_, Data}) ->
        case re:run(Data, MP, [{capture, all_but_first, binary}]) of
            nomatch -> Sub;
            match -> Sub#{Key => true};
            {match, [Result]} -> Sub#{Key => Result};
            {match, [_|_] = Result} -> Sub#{Key => Result}
        end
    end;
make_data_extractor(Key, {match, Regexp, Opts}, _Default) ->
    CompOpts = filter_re_compile_options(Opts),
    RunOpts = filter_re_run_options(Opts),
    {ok, MP} = re:compile(Regexp, CompOpts),
    fun(Sub, {_, Data}) ->
        case re:run(Data, MP, RunOpts) of
            nomatch -> Sub;
            match -> Sub#{Key => true};
            {match, [Result]} ->
                Sub#{Key => Result};
            {match, [_|_] = Result} ->
                Sub#{Key => Result}
        end
    end;
make_data_extractor(Key, {match, Regexp, Opts, MatchHandler}, _Default) ->
    CompOpts = filter_re_compile_options(Opts),
    RunOpts = filter_re_run_options(Opts),
    {ok, MP} = re:compile(Regexp, CompOpts),
    fun(Sub, {_, Data}) ->
        case re:run(Data, MP, RunOpts) of
            nomatch -> Sub;
            match -> Sub#{Key => MatchHandler(true)};
            {match, Captured} -> Sub#{Key => MatchHandler(Captured)}
        end
    end;
make_data_extractor(_Key, _Other, undefined) ->
    fun(Sub, _Data) -> Sub end;
make_data_extractor(_Key, _Other, Default) ->
    make_data_extractor(_Key, Default, undefined).

make_default_data_handler(State, #{cmd := Cmd} = CallSpec) ->
    CmdPath = cmd_path(State, Cmd),
    CmdBin = atom_to_binary(Cmd),
    DefaultErrorRegex = <<"^(?:", CmdBin/binary, ": |", (escape_regexp(CmdPath))/binary, ": )([^\n]*)">>,
    DefaultErrorExtractor = {match, DefaultErrorRegex, [multiline, {capture, all_but_first, binary}]},
    Extractors = [
        make_data_extractor(result, maps:get(result, CallSpec, undefined), undefined),
        make_data_extractor(error, maps:get(error, CallSpec, undefined), DefaultErrorExtractor)
    ],
    fun (Sub, Event) ->
            {continue, lists:foldl(fun(F, S) -> F(S, Event) end,
                                   Sub, Extractors)}
    end.

make_default_exit_handler(_State, CallSpec) ->
    Cleanup = fun(Sub) ->
        maps:remove(error, maps:remove(result, Sub))
    end,
    ErrorAsResult = case maps:find(error_as_result, CallSpec) of
        {ok, true} -> true;
        _ -> false
    end,
    DefaultError = case maps:find(error, CallSpec) of
        {ok, {default, Default1}} -> Default1;
        _ -> undefined
    end,
    {DefaultResult, ReverseResult} = case maps:find(result, CallSpec) of
        {ok, {default, Default2}} -> {Default2, false};
        {ok, {collect, _}} -> {undefined, true};
        _ -> {undefined, false}
    end,
    ProcessResult = case maps:find(keep_result, CallSpec) of
        error -> fun(Sub, Result) -> {ok, Result, Sub} end;
        {ok, Key} -> fun(Sub, Result) -> {ok, Result, Sub#{Key => Result}} end
    end,
    fun
        (Sub = #{result := Result}, {exit, {status, 0}})
          when ReverseResult =:= false ->
            ProcessResult(Cleanup(Sub), Result);
        (Sub = #{result := Result}, {exit, {status, 0}})
          when ReverseResult =:= true ->
            ProcessResult(Cleanup(Sub), lists:reverse(Result));
        (Sub, {exit, {status, 0}}) ->
            case DefaultResult of
                undefined -> {ok, Cleanup(Sub)};
                _ -> ProcessResult(Cleanup(Sub), DefaultResult)
            end;
        (Sub = #{error := Reason}, _Status)
          when ErrorAsResult =:= true ->
            ProcessResult(Cleanup(Sub), {error, Reason});
        (Sub = #{error := Reason}, _Status)
          when ErrorAsResult =:= false ->
            {error, Reason, Cleanup(Sub)};
        (Sub, Status) ->
            case DefaultError of
                undefined -> {error, Status, Cleanup(Sub)};
                _ -> {error, DefaultError, Cleanup(Sub)}
            end
    end.

make_messager_handler(StartHandler, DataHandler, ExitHandler) ->
    fun
        (Sub, _CallSpec, {start, LastResult}) -> StartHandler(Sub, LastResult);
        (Sub, _CallSpec, {stdout, _Data} = Msg) -> DataHandler(Sub, Msg);
        (Sub, _CallSpec, {stderr, _Data} = Msg) -> DataHandler(Sub, Msg);
        (Sub, _CallSpec, {exit, _Reason} = Msg) -> ExitHandler(Sub, Msg);
        (Sub, _CallSpec, {message, _Msg}) -> {continue, Sub}
    end.

filter_re_compile_options(Opts) ->
    filter_re_compile_options(Opts, []).

filter_re_compile_options([], Acc) ->
    lists:reverse(Acc);
filter_re_compile_options([F | Rest], Acc)
  when F =:= unicode; F =:= anchored; F =:= caseless; F =:= dollar_endonly;
       F =:= dotall; F =:= extended; F =:= firstline; F =:= multiline;
       F =:= no_auto_capture; F =:= dupnames; F =:= ungreedy;
       F =:= bsr_anycrlf; F =:= bsr_unicode; F =:= no_start_optimize;
       F =:= ucp; F =:= never_utf ->
    filter_re_compile_options(Rest, [F | Acc]);
filter_re_compile_options([{T, _} = V | Rest], Acc)
  when T =:= newline ->
    filter_re_compile_options(Rest, [V | Acc]);
filter_re_compile_options([_ | Rest], Acc) ->
    filter_re_compile_options(Rest, Acc).

filter_re_run_options(Opts) ->
    filter_re_run_options(Opts, []).

filter_re_run_options([], Acc) ->
    lists:reverse(Acc);
filter_re_run_options([F | Rest], Acc)
  when F =:= anchored; F =:= global; F =:= notbol; F =:= notempty;
       F =:= notempty_atstart; F =:= noteol; F =:= report_errors ->
    filter_re_run_options(Rest, [F | Acc]);
filter_re_run_options([{T, _} = V | Rest], Acc)
  when T =:= capture; T =:= match_limit; T =:= match_limit_recursion;
       T =:= newline; T =:= offset ->
    filter_re_run_options(Rest, [V | Acc]);
filter_re_run_options([{T, _, _} = V | Rest], Acc)
  when T =:= capture ->
    filter_re_run_options(Rest, [V | Acc]);
filter_re_run_options([_ | Rest], Acc) ->
    filter_re_run_options(Rest, Acc).

escape_regexp(String) when is_binary(String) ->
    EscapedString = unicode:characters_to_list(String),
    escape_regexp(EscapedString);
escape_regexp(String) when is_list(String) ->
    list_to_binary(lists:flatmap(fun escape_regexp_char/1, String)).

escape_regexp_char($.) -> "\\.";
escape_regexp_char($^) -> "\\^";
escape_regexp_char($&) -> "\\&";
escape_regexp_char($*) -> "\\*";
escape_regexp_char($+) -> "\\+";
escape_regexp_char($?) -> "\\?";
escape_regexp_char(${) -> "\\{";
escape_regexp_char($}) -> "\\}";
escape_regexp_char($[) -> "\\[";
escape_regexp_char($]) -> "\\]";
escape_regexp_char($() -> "\\(";
escape_regexp_char($)) -> "\\)";
escape_regexp_char($|) -> "\\|";
escape_regexp_char($\\) -> "\\\\";
escape_regexp_char(Char) -> [Char].
