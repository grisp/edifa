-module(edifa).


%--- Exports -------------------------------------------------------------------

% API Functions
-export([create/2, create/3]).
-export([write/3]).
-export([partition/3, partition/4]).
-export([format/4]).
-export([mount/2, mount/3]).
-export([unmount/2, unmount/3]).
-export([extract/3, extract/4, extract/5]).
-export([close/1, close/2]).


%--- Types -------------------------------------------------------------------

-record(image, {pid}).

-type image() :: #image{}.
-type create_options() :: #{
    % The maximum size of a block written to the output.
    max_block_size => undefined | pos_integer(),
    temp_dir => undefined | file:filename(),
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type write_options() :: #{
    % The maximum size of a block read from the input or written to the output.
    % The real block size may depend on the other options.
    max_block_size => pos_integer(),
    % The number of bytes to write.
    count => pos_integer(),
    % The number of bytes to skip in the input file.
    skip => pos_integer(),
    % The offset in the output file where to start writing.
    seek => pos_integer(),
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type partition_table() :: mbr.
-type partition_type() :: fat32.
-type mbr_partition_specs() :: [#{
    type := partition_type() | pos_integer(),
    start => non_neg_integer(),
    size := non_neg_integer(),
    active => boolean() % Default: false
}].
-type partition_specs() :: mbr_partition_specs().
-type partition_id() :: atom().
-type partition_filesystem() :: fat.
-type partition_options() :: #{
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type format_options() :: #{
    identifier => undefined | non_neg_integer(),
    label => undefined | iodata(),
    type => undefined | 12 | 16 | 32,
    cluster_size => undefined | 1 | 2 | 4 | 8 | 16 | 32 | 64 | 128, % Default: 8
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type mount_options() :: #{
    mount_point => undefined | file:filename(),
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type unmount_options() :: #{
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type extract_options() :: #{
    compressed => boolean(),
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.
-type close_options() :: #{
    log_handler => undefined | edifa_exec:log_handler(),
    log_state => term()
}.

%--- Macros --------------------------------------------------------------------

-define(DEFAULT_FORMAT_CLUSTER_SIZE, 8).
-define(FMT(F, A), iolist_to_binary(io_lib:format(F, A))).


%--- API Functions -------------------------------------------------------------

%% @equiv edifa:create(Filename, Size, #{})
-spec create(Filename :: file:filename(), Size :: pos_integer()) ->
    {ok, image()} | {error, Reason :: term()}.
create(Filename, Size) ->
    create(Filename, Size, #{}).

%% @doc Creates an image of give size in bytes with given options.
%% The file must not already exists. If the image file is `undefined',
%% a temporary file will be created and deleted at the end.
%% <p>Options:
%% <ul>
%%   <li><b>max_block_size</b>:
%%      Maximum block size used when doing file operations.
%%   </li>
%%   <li><b>temp_dir</b>:
%%      The directory to use when creating temporary files and directories.
%%   </li>
%%   <li><b>log_handler</b>:
%%      A function to call with log events.
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec create(Filename :: file:filename() | undefined, Size :: pos_integer(),
             Options :: create_options()) ->
    {ok, image()} | {ok, image(), LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
create(Filename, Size, Opts) ->
    case Filename =/= undefined andalso filelib:is_file(Filename) of
        true -> {error, ?FMT("Image file ~s already exists", [Filename])};
        false ->
            case os:type() of
                {unix, darwin} ->
                    create(Filename, Size, Opts, edifa_macos, Opts);
                {unix, linux} ->
                    create(Filename, Size, Opts, edifa_linux, Opts);
                Other ->
                    {error, ?FMT("operating system not supported: ~p", [Other])}
            end
    end.

%% @doc Writes data into a previously created image from gien input file.
%% <p>Options:
%% <ul>
%%   <li><b>max_block_size</b>:
%%      Maximum block size used when doing file operations.
%%   </li>
%%   <li><b>count</b>:
%%      The number of bytes to write.
%%   </li>
%%   <li><b>skip</b>:
%%      The number of bytes to skip in the input file.
%%   </li>
%%   <li><b>seek</b>:
%%      The offset in the output file where to start writing.
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec write(image(), file:filename(), write_options()) ->
    ok | {ok, LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
write(#image{pid = Pid}, Filename, Opts) ->
    case filelib:is_file(Filename) of
        false -> {error, {not_found, Filename}};
        true ->
            {CallOpts, CmdOpts} = split_opts(Opts),
            edifa_exec:call(Pid, write, [Filename, CmdOpts], CallOpts)
    end.

%% @equiv edifa:partition(Image, PartitionTableType, PartitionSpec, #{})
-spec partition(image(), partition_table(), partition_specs()) ->
    {ok, [partition_id()]} | {error, Reason :: term()}.
partition(Img, PartTable, PartitionSpecs) ->
    partition(Img, PartTable, PartitionSpecs, #{}).

%% @doc Creates the partition table on a previously created image.
%% Only supports MBR partitions for now.
%% Support for active/bootable partition flag is not implemented yet.
%% <p>Specification of MBR partitions:
%% <ul>
%%   <li><b>type</b>:
%%     Required partition type as the 'fat32' atom or a positive integer.
%%   </li>
%%   <li><b>start</b>:
%%     Optional partition start offset in bytes. It <b>MUST</b> be a multiple of
%%     the sector size (512). If not specified, it will be 0 for the first
%%     partition, and the ending of the last partition for the following ones.
%%   </li>
%%   <li><b>size</b>:
%%     Required size of the partition. It <b>MUST</b> be a multiple of the
%%     sector size (512).
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%%  </ul></p>
-spec partition(image(), partition_table(), partition_specs(),
                partition_options()) ->
    {ok, [partition_id()]} | {ok, [partition_id()], LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
partition(#image{pid = Pid}, PartTable, PartitionSpecs, Opts) ->
    case PartTable of
        mbr ->
            case validate_mbr_specs(PartitionSpecs) of
                {error, _Reason} = Error -> Error;
                {ok, NewSpecs} ->
                    {CallOpts, CmdOpts} = split_opts(Opts),
                    edifa_exec:call(Pid, partition,
                                    [PartTable, NewSpecs, CmdOpts], CallOpts)
            end;
        _ ->
            {error, ?FMT("Unsupported partition table type ~p", [PartTable])}
    end.

%% @doc Formats a partition in a previously created image.
%% Only supports FAT for now.
%% <p>Options for FAT file system:
%% <ul>
%%   <li><b>identifier</b>:
%%     The parition identifier as a positive integer.
%%   </li>
%%   <li><b>label</b>:
%%     The parition label as a binary or a string.
%%   </li>
%%   <li><b>type</b>:
%%     The type of FAT, could be either 12, 16 or 32.
%%   </li>
%%   <li><b>cluster_size</b>:
%%     The number of sector per cluster, must be a power of 2 from 1 to 128
%%     included. Default is 8.
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec format(image(), partition_id(), partition_filesystem(), format_options()) ->
    ok | {ok, LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
format(#image{pid = Pid}, PartId, fat, Opts) when is_atom(PartId) ->
    {CallOpts, CmdOpts} = split_opts(Opts),
    case validate_format_options(CmdOpts) of
        {error, _Reason} = Error -> Error;
        {ok, CmdOpts2} ->
            edifa_exec:call(Pid, format, [PartId, fat, CmdOpts2], CallOpts)
    end;
format(#image{}, _PartId, FileSystem, _Opts) ->
    {error, ?FMT("File system ~p not supported", [FileSystem])}.

%% @equiv edifa:mount(Image, PartId, #{})
-spec mount(image(), partition_id()) ->
    {ok, MountPoint :: binary()} | {error, Reason :: term()}.
mount(Img, PartId) ->
    mount(Img, PartId, #{}).

%% @doc Mounts a partition from given image.
%% Returns the path to the directory where the parition is mounted.
%% <p>Options:
%% <ul>
%%   <li><b>mount_point</b>:
%%     An explicit directory path to mount the file system into.
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec mount(image(), partition_id(), mount_options()) ->
    {ok, MountPoint :: binary()}
  | {ok, MountPoint :: binary(), LogState :: term()}
  | {error, Reason :: term()}
  | {error, Reason :: term(), LogState :: term()}.
mount(#image{pid = Pid}, PartId, Opts) ->
    {CallOpts, CmdOpts} = split_opts(Opts),
    edifa_exec:call(Pid, mount, [PartId, CmdOpts], CallOpts).

%% @equiv edifa:unmount(Image, PartId, #{})
-spec unmount(image(), partition_id()) -> ok | {error, Reason :: term()}.
unmount(Img, PartId) ->
    unmount(Img, PartId, #{}).

%% @doc Unmounts a previously mounted partition.
%% <p>Options:
%% <ul>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec unmount(image(), partition_id(), unmount_options()) ->
    ok | {ok, LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
unmount(#image{pid = Pid}, PartId, Opts) ->
    {CallOpts, CmdOpts} = split_opts(Opts),
    edifa_exec:call(Pid, unmount, [PartId, CmdOpts], CallOpts).

%% @doc Extracts a partition or the reserved space before the first partition
%% into the given file. The output file must not already exists.\
%% @equiv edifa:extract(Image, ParetId, PartId, OutputFile, #{})
-spec extract(image(), reserved | partition_id(),
              Filename :: file:filename()) ->
    ok | {error, Reason :: term()}.
extract(Img, PartId, OutputFile) ->
    extract(Img, PartId, PartId, OutputFile, #{}).

-spec extract(image(), From :: reserved | partition_id(),
              To :: reserved | partition_id(), file:filename()) -> ok.
%% @equiv edifa:extract(Image, From, To, OutputFile, #{})
extract(Img, From, To, OutputFile) ->
    extract(Img, From, To, OutputFile, #{}).

%% @doc Extracts a range of partition that may include the reserved space before
%% the first partition into the given file. Both paritions are included in the
%% extracted data, so to extract a single one, the same must be specified
%% from the `From' and `To' arguments.
%% The output file must not already exists.
%% All the paritions involved <b>MUST NOT</b> be mounted, otherwise the
%% extracted data mya not be up-to-date.
%% <p>Options:
%% <ul>
%%   <li><b>compressed</b>:
%%     If the output file should be compressed with gzip. If `true', the `.gz`
%%     extension will be append at the end of the file name. Default: `false'.
%%   </li>
%%   <li><b>log_handler</b>:
%%      Either a stateless or stateful anonymous function that will be called
%%      with all the events happening during the operation. If a stateful
%%      handler is specified, the function will return the state as an extra
%%      last tuple element.
%%   </li>
%%   <li><b>log_state</b>:
%%      The state data to use if the log handler is stateful.
%%   </li>
%% </ul></p>
-spec extract(image(), From :: reserved | partition_id(),
              To :: reserved | partition_id(), file:filename(),
              extract_options()) ->
    ok | {ok, LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
extract(#image{pid = Pid}, From, To, OutputFile, Opts) ->
    case filelib:is_file(OutputFile) of
        true -> {error, ?FMT("Output file ~s already exists", [OutputFile])};
        false ->
            {CallOpts, CmdOpts} = split_opts(Opts),
            edifa_exec:call(Pid, extract,
                            [From, To, OutputFile, CmdOpts], CallOpts)
    end.

%% @equiv edifa:close(Image, #{})
-spec close(image()) -> ok | {error, Reason :: term()}.
close(Img) ->
    close(Img, #{}).

%% @doc Closes the previously created image.
-spec close(image(), close_options()) ->
    ok | {ok, LogState :: term()}
  | {error, Reason :: term()} | {error, Reason :: term(), LogState :: term()}.
close(#image{pid = Pid}, Opts) ->
    edifa_exec:terminate(Pid, Opts).


%--- Internal Functions --------------------------------------------------------

split_opts(Opts) ->
    {maps:with([log_handler, log_state], Opts),
     maps:without([log_handler, log_state], Opts)}.

create(Filename, Size, CreateOpts, Mod, ModOpts) ->
    case edifa_exec:start(Mod, ModOpts) of
        {error, _Reason} = Error -> Error;
        {ok, Pid} ->
            {CallOpts, CmdOpts} = split_opts(CreateOpts),
            Args = [Filename, Size, CmdOpts],
            case edifa_exec:call(Pid, create, Args, CallOpts) of
                {error, _Reason} = Error -> Error;
                {error, _Reason, _LogState} = Error -> Error;
                ok -> {ok, #image{pid = Pid}};
                {ok, LogState} -> {ok, #image{pid = Pid}, LogState}
            end
    end.

as_binary(undefined) -> undefined;
as_binary(Other) -> iolist_to_binary(Other).

validate_mbr_specs([]) -> {error, no_partition_specified};
validate_mbr_specs(Specs) ->
    validate_mbr_specs(Specs, false, 0, []).

validate_mbr_specs([], _HasActive, _FreeOffset, Acc) ->
    {ok, lists:reverse(Acc)};
validate_mbr_specs([#{active := true} | _], true, _, _) ->
    {error, <<"Multiple active partitions">>};
validate_mbr_specs([#{start := Start} | _], _, _, _)
  when (Start rem 512) =/= 0 ->
    {error, <<"Partition start position is not a multiple of sector size (512)">>};
validate_mbr_specs([#{size := Size} | _], _, _, _)
  when (Size rem 512) =/= 0 ->
    {error, <<"Partition size is not a multiple of sector size (512)">>};
validate_mbr_specs([#{type := Type} | _], _, _, _)
  when not (is_atom(Type) orelse is_integer(Type));
       is_atom(Type), Type =/= fat32;
       is_integer(Type), (Type < 1) orelse (Type > 255) ->
    {error, ?FMT("Invalid partition type ~p", [Type])};
validate_mbr_specs([#{start := Start, size := Size, type := _} = Spec | Rest],
                   HasActive, FreeOffset, Acc)
  when Start >= FreeOffset ->
    Active = maps:get(active, Spec, false),
    Spec2 = Spec#{active => Active},
    validate_mbr_specs(Rest, HasActive or Active, Start + Size,
                       [Spec2 | Acc]);
validate_mbr_specs([#{size := Size, type := _} = Spec | Rest],
                   HasActive, FreeOffset, Acc) ->
    Active = maps:get(active, Spec, false),
    Spec2 = Spec#{start => FreeOffset, active => Active},
    validate_mbr_specs(Rest, HasActive or Active, FreeOffset + Size,
                       [Spec2 | Acc]);
validate_mbr_specs(_Specs, _HasActive, _FreeOffset, _Acc) ->
    {error, <<"Overlapping partitions">>}.

validate_format_options(Opts) ->
    Result = #{label => as_binary(maps:get(label, Opts, undefined))},
    case maps:get(cluster_size, Opts, ?DEFAULT_FORMAT_CLUSTER_SIZE) of
        ClustSize when ClustSize =:= 1; ClustSize =:= 2; ClustSize =:= 4;
                       ClustSize =:= 8; ClustSize =:= 16; ClustSize =:= 32;
                       ClustSize =:= 64; ClustSize =:= 128 ->
            case maps:get(identifier, Opts, undefined) of
                VolId when VolId =:= undefined;
                           is_integer(VolId), VolId > 0, VolId < 4294967296 ->
                    case maps:get(type, Opts, undefined) of
                        FatType when FatType =:= undefined;
                                     FatType =:= 12;
                                     FatType =:= 16;
                                     FatType =:= 32 ->
                            {ok, Result#{
                                identifier => VolId,
                                type => FatType,
                                cluster_size => ClustSize
                            }};
                        BadFatType ->
                            {error, ?FMT("Bad fat type ~p", [BadFatType])}
                    end;
                BadVolId ->
                    {error, ?FMT("Bad volume identifier ~p", [BadVolId])}
            end;
        BadClustSize ->
            {error, ?FMT("Bad format cluster size ~p", [BadClustSize])}
    end.
