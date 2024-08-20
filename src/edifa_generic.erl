-module(edifa_generic).


%--- Exports -------------------------------------------------------------------

% API functions
-export([create/4]).
-export([write/3]).
-export([extract/4]).

% Helper functions
-export([copy_command/3]).


%--- Macros --------------------------------------------------------------------

-define(MAX_BLOCK_SIZE, 65536).
-define(FMT(F, A), iolist_to_binary(io_lib:format(F, A))).


%--- API Functions -------------------------------------------------------------

create(#{image_filename := CurrFilename}, _Filename, _Size, _Opts) ->
    {error, ?FMT("Image file already created: ~s", [CurrFilename])};
create(State, Filename, Size, Opts) ->
    FilenameBin = iolist_to_binary(Filename),
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    BlockSize = block_size([Size], MaxBlockSize),
    edifa_exec:command(State#{image_filename => FilenameBin}, [
        #{cmd => mkdir, args => ["-p", filename:dirname(FilenameBin)]},
        #{cmd => dd, args => [
            "conv=sparse,notrunc",
            "if=/dev/zero",
            "of=" ++ unicode:characters_to_list(FilenameBin),
            "bs=" ++ integer_to_list(BlockSize),
            "count=" ++ integer_to_list(Size div BlockSize)
        ]}
    ]).

write(#{image_filename := OutFilename} = State, InFilename, Opts) ->
    edifa_exec:command(State, copy_command(InFilename, OutFilename, Opts));
write(_State, _InFilename, _Opts) ->
    {error, <<"No image file created">>}.

extract(#{image_filename := InFilename} = State, From, To, OutFilename) ->
    case extract_block(State, From, To) of
        {error, _Reason} = Error -> Error;
        {ok, Start, Size} ->
            Opts = #{skip => Start, count => Size},
            edifa_exec:command(State, [
                #{cmd => mkdir, args => ["-p", filename:dirname(OutFilename)]},
                copy_command(InFilename, OutFilename, Opts)
            ])
    end;
extract(_State, _From, _To, _OutputFile) ->
    {error, <<"No image file created">>}.

copy_command(From, To, Opts) ->
    FromFilename = unicode:characters_to_list(iolist_to_binary(From)),
    ToFilename = unicode:characters_to_list(iolist_to_binary(To)),
    #{
        cmd => dd,
        args => [
            "conv=sparse,notrunc",
            "if=" ++ FromFilename,
            "of=" ++ ToFilename
            ] ++ dd_block_opts(Opts)
    }.

%--- Internal Functions --------------------------------------------------------

gcd(A, B) when B > A -> gcd(B, A);
gcd(A, B) when A rem B > 0 -> gcd(B, A rem B);
gcd(A, B) when A rem B =:= 0 -> B.

gcd([H | T]) -> lists:foldl(fun gcd/2, H, T).

block_size(Indexes, MaxSize) ->
    Gcd = gcd(Indexes),
    max_divisor(Gcd, min(Gcd, MaxSize)).

max_divisor(Value, Div) when (Value rem Div) =:= 0 -> Div;
max_divisor(Value, Div) -> max_divisor(Value, Div - 1).

dd_block_opts(Opts = #{count := Count, seek := Seek, skip := Skip}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = block_size([Seek], MaxBlockSize),
    InputBlockSize = block_size([Count, Skip], MaxBlockSize),
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "skip=" ++ integer_to_list(Skip div InputBlockSize),
     "seek=" ++ integer_to_list(Seek div OutputBlockSize),
     "count=" ++ integer_to_list(Count div InputBlockSize)];
dd_block_opts(Opts = #{count := Count, seek := Seek}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = block_size([Seek], MaxBlockSize),
    InputBlockSize = block_size([Count], MaxBlockSize),
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "seek=" ++ integer_to_list(Seek div OutputBlockSize),
     "count=" ++ integer_to_list(Count div InputBlockSize)];
dd_block_opts(Opts = #{count := Count, skip := Skip}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = MaxBlockSize,
    InputBlockSize = block_size([Count, Skip], MaxBlockSize),
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "skip=" ++ integer_to_list(Skip div InputBlockSize),
     "count=" ++ integer_to_list(Count div InputBlockSize)];
dd_block_opts(Opts = #{seek := Seek, skip := Skip}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = block_size([Seek], MaxBlockSize),
    InputBlockSize = block_size([Skip], MaxBlockSize),
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "skip=" ++ integer_to_list(Skip div InputBlockSize),
     "seek=" ++ integer_to_list(Seek div OutputBlockSize)];
dd_block_opts(Opts = #{count := Count}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    BlockSize = block_size([Count], MaxBlockSize),
    ["bs=" ++ integer_to_list(BlockSize),
     "count=" ++ integer_to_list(Count div BlockSize)];
dd_block_opts(Opts = #{seek := Seek}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = block_size([Seek], MaxBlockSize),
    InputBlockSize = MaxBlockSize,
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "seek=" ++ integer_to_list(Seek div OutputBlockSize)];
dd_block_opts(Opts = #{skip := Skip}) ->
    MaxBlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    OutputBlockSize = MaxBlockSize,
    InputBlockSize = block_size([Skip], MaxBlockSize),
    ["obs=" ++ integer_to_list(OutputBlockSize),
     "ibs=" ++ integer_to_list(InputBlockSize),
     "skip=" ++ integer_to_list(Skip div InputBlockSize)];
dd_block_opts(Opts) ->
    BlockSize = maps:get(max_block_size, Opts, ?MAX_BLOCK_SIZE),
    ["bs=" ++ integer_to_list(BlockSize)].

extract_block(#{partition_ids := [First|_]} = State, reserved, reserved) ->
    #{partition_ids := [First|_], partitions := PartMap} = State,
    #{First := #{index := 1, start := Start}} = PartMap,
    {ok, 0, Start};
extract_block(_State, reserved, reserved) ->
    {error, <<"No partition defined">>};
extract_block(#{partitions := PartMap}, reserved, ToId) ->
    case maps:find(ToId, PartMap) of
        error -> {error, ?FMT("Partition ~s not found", [ToId])};
        {ok, #{start := Start, size := Size}} -> {ok, 0, Start + Size}
    end;
extract_block(#{partitions := _}, FromId, reserved) ->
    {error, ?FMT("Invalid extraction range ~s-reserved", [FromId])};
extract_block(#{partitions := PartMap}, FromId, ToId) ->
    case maps:find(FromId, PartMap) of
        error -> {error, ?FMT("Partition ~s not found", [FromId])};
        {ok, #{index := Idx1, start := Start1}} ->
            case maps:find(ToId, PartMap) of
                error -> {error, ?FMT("Partition ~s not found", [FromId])};
                {ok, #{index := Idx2, start := Start2, size := Size2}}
                  when Idx1 =< Idx2 ->
                    {ok, Start1, Start2 + Size2 - Start1};
                {ok, #{index := Idx2}} ->
                    {error, ?FMT("Invalid extraction range ~s(#~w)-~s(~w)",
                                 [FromId, Idx1, ToId, Idx2])}
            end
    end;
extract_block(_State, _From, _To) ->
    {error, <<"No partition table defined">>}.

