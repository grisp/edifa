-module(edifa_macos).


%--- Includes ------------------------------------------------------------------

-include_lib("kernel/include/file.hrl").
-include_lib("xmerl/include/xmerl.hrl").


%--- Exports -------------------------------------------------------------------

% API Functions
-export([init/1]).
-export([terminate/2]).

-export([create/4]).
-export([write/3]).
-export([partition/4]).
-export([format/4]).
-export([mount/3]).
-export([unmount/3]).
-export([extract/5]).


%--- Macros --------------------------------------------------------------------

-define(FMT(F, A), iolist_to_binary(io_lib:format(F, A))).


%--- API Functions -------------------------------------------------------------

init(Opts) ->
    {ok, State, Commands} = edifa_generic:init(Opts),
    {ok, State, [hdiutil, fdisk, diskutil, newfs_msdos | Commands]}.

terminate(State, _Reason) ->
    edifa_exec:command(State, cmd_cleanup(State)).

create(State, Filename, Size, Opts) ->
    edifa_generic:create(State, Filename, Size, Opts).

write(State, Filename, Opts) ->
    edifa_generic:write(State, Filename, Opts).

% Suppress the "no local return" warning for the image_partition/3 function
-dialyzer({no_return, partition/4}).

partition(State, mbr, Specs, _Opts) ->
    with_image_device(State, fun(State2, ImageDevice) ->
        PartitionSpecs = format_partition_specs(512, Specs),
        edifa_exec:command(State2, [
            #{
                cmd => fdisk,
                args => ["-ry", ImageDevice],
                input => [PartitionSpecs, eof]
            },
            cmd_diskutil_list(ImageDevice),
            fun(State3, DiskInfo) ->
                try
                    #{partitions := DiskUtilPartitions} = DiskInfo,
                    {_, RevPartSpecs} = lists:foldl(fun
                        ({undefined, #{idx := Idx, device_name := DevName}}, {Idx, _Acc}) ->
                            throw(?FMT("Unexpected disk ~s partition #~w: ~s",
                                       [ImageDevice, Idx, DevName]));
                        ({_Spec, undefined}, {Idx, _Acc}) ->
                            throw(?FMT("Partition #~w of disk ~s without corresponding device name",
                                       [Idx, ImageDevice]));
                        ({#{type := T, size := S} = Spec,
                         #{index := Idx, type := T, size := S} = Info},
                         {Idx, Acc}) ->
                            {Idx + 1, [maps:merge(Spec, Info) | Acc]};
                        ({#{type := T, size := S1},
                         #{index := Idx, type := T, size := S2, device_name := Name}},
                         {Idx, _Acc}) ->
                            throw(?FMT("Partition #~w ~s size mismatch, expected ~w and got ~w",
                                       [Idx, Name, S1, S2]));
                        ({#{type := T1},
                         #{index := Idx, type := T2, device_name := Name}},
                         {Idx, _Acc}) ->
                            throw(?FMT("Partition #~w ~s type mismatch, expected ~p and got ~p",
                                               [Idx, Name, T1, T2]))
                    end, {1, []}, lists_zip(Specs, DiskUtilPartitions,
                                            {pad, undefined, undefined})),
                    PartitionList = lists:reverse(RevPartSpecs),
                    PartitionIds = [I || #{id := I} <- PartitionList],
                    PartitionKV = [{I, V} || V = #{id := I} <- PartitionList],
                    PartitionMap = maps:from_list(PartitionKV),
                    {ok, PartitionIds, State3#{partition_ids => PartitionIds,
                                               partitions => PartitionMap}}
                catch
                    throw:Reason -> {error, Reason, State3}
                end
            end
        ])
    end).

format(State = #{partitions := PartMap}, PartId, fat, Opts) ->
    case maps:find(PartId, PartMap) of
        error ->
            {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{sector_size := SecSize, device_file := Device}} ->
            Args = case maps:get(type, Opts, undefined) of
                undefined -> [];
                FatType when FatType =:= 12; FatType =:= 16; FatType =:= 32 ->
                    ["-F", integer_to_list(FatType)]
            end ++ case maps:get(identifier, Opts, undefined) of
                undefined -> [];
                VolId when is_integer(VolId) ->
                    ["-I", format_volume_id(VolId)]
            end ++ case maps:get(label, Opts, undefined) of
                undefined -> [];
                VolName when is_binary(VolName) ->
                    ["-v", VolName]
            end ++ case maps:get(cluster_size, Opts, undefined) of
                undefined -> [];
                ClustSize when is_integer(ClustSize) ->
                    ["-c", integer_to_list(ClustSize)]
            end ++ ["-S", integer_to_list(SecSize), Device],
            edifa_exec:command(State, [
                #{cmd => newfs_msdos, args => Args},
                fun(State2, undefined) ->
                    #{partitions := #{PartId := OldInfo} = Parts} = State2,
                    NewInfo = OldInfo#{format => fat},
                    State3 = State2#{partitions => Parts#{PartId => NewInfo}},
                    {ok, State3}
                end
            ])
    end;
format(_State, _PartId, _FileSystem, _Opts) ->
    {error, <<"No partition table defined">>}.

mount(State = #{image_device := ImageDevice, partitions := PartMap},
      PartId, Opts) ->
    case maps:find(PartId, PartMap) of
        error ->
            {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{mount_point := MountPoint}}
          when MountPoint =/= undefined ->
            {ok, MountPoint, State};
        {ok, #{device_file := DeviceFile}} ->
            edifa_exec:command(State, [
                case maps:find(mount_point, Opts) of
                    {ok, MountPoint} when MountPoint =/= undefined ->
                        [
                            edifa_generic:cmd_mkdir(MountPoint),
                            #{
                                cmd => diskutil,
                                args => [
                                    "mount",
                                    "-mountPoint", MountPoint,
                                    DeviceFile
                                ]
                            }
                        ];
                    _ ->
                        [
                            #{
                                cmd => diskutil,
                                args => ["mount", DeviceFile]
                            }
                        ]
                end,
                cmd_diskutil_list(ImageDevice),
                fun(State2, #{partitions := InfoList}) ->
                    InfoMap = maps:from_list([{I, M} || M = #{id := I} <- InfoList]),
                    case InfoMap of
                        #{PartId := #{mount_point := MountPoint}} ->
                            #{partitions := #{PartId := OldInfo} = Parts} = State2,
                            NewInfo = OldInfo#{mount_point => MountPoint},
                            State3 = State2#{partitions => Parts#{PartId => NewInfo}},
                            {ok, MountPoint, State3};
                        _ ->
                            {error, ?FMT("Failed to mount disk ~s partition ~s",
                                         [ImageDevice, PartId])}
                    end
                end
            ])
    end;
mount(_State, _PartId, _Opts) ->
    {error, <<"No partition table defined">>}.

unmount(State = #{partitions := PartMap}, PartId, _Opts) ->
    case maps:find(PartId, PartMap) of
        error -> {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{mount_point := undefined}} -> {ok, State};
        {ok, #{device_file := DeviceFile}} ->
            edifa_exec:command(State, [
                #{
                    cmd => diskutil,
                    args => ["umount", DeviceFile]
                },
                fun(State2, _) ->
                    #{partitions := #{PartId := OldInfo} = Parts} = State2,
                    NewInfo = OldInfo#{mount_point => undefined},
                    State3 = State2#{partitions => Parts#{PartId => NewInfo}},
                    {ok, State3}
                end
            ])
    end;
unmount(_State, _PartId, _Opts) ->
    {error, <<"No partition table defined">>}.

extract(State, From, To, Filename, Opts) ->
    edifa_generic:extract(State, From, To, Filename, Opts).


%--- Internal Functions --------------------------------------------------------

cmd_cleanup(State) ->
    cmd_cleanup_image_device(State)
        ++ edifa_generic:cmd_cleanup(State).

cmd_cleanup_image_device(#{image_device := ImageDevice}) ->
    [#{cmd => hdiutil, args => ["detach", ImageDevice]}];
cmd_cleanup_image_device(_State) ->
    [].

with_image_device(State = #{image_device := ImageDevice}, Fun) ->
    Fun(State, ImageDevice);
with_image_device(State = #{image_filename := ImageFilename}, Fun) ->
    edifa_exec:command(State, [#{
        cmd => hdiutil,
        args => [
            "attach", "-imagekey", "diskimage-class=CRawDiskImage",
            "-nomount", ImageFilename
        ],
        result => {match, "(/dev/disk[0-9]+)[^s]"},
        keep_result => image_device,
        error => {default, <<"Failed to attach image to device">>}
    }, Fun]);
with_image_device(_State, _Fun) ->
    {error, <<"No image created">>}.

cmd_diskutil_list(DiskDevice) ->
    [
        #{
            cmd => diskutil,
            args => ["list", "-plist", DiskDevice],
            result => {collect, stdout}
        },
        fun(State, DiskutilOutput) ->
            {ok, parse_diskutil_plist(DiskutilOutput), State}
        end
    ].

format_partition_type(fat32) -> <<"0x0B">>;
format_partition_type(TypeId) when is_integer(TypeId), TypeId > 0 ->
    io_lib:format("0x~2.16.0B", [TypeId]).

format_partition_spec(_SectorSize, undefined) ->
    <<"0,0,0x00\n">>;
format_partition_spec(SectorSize, #{type := Type, start := Start, size := Size})
  when is_integer(Start), Start >= 0, (Start rem SectorSize) =:= 0,
       is_integer(Size), Size >= 0, (Size rem SectorSize) =:= 0 ->
    [io_lib:format("~w,~w,", [Start div SectorSize, Size div SectorSize]),
     format_partition_type(Type), "\n"].

format_partition_specs(SectorSize, Specs) when is_list(Specs) ->
    PaddedSpecs = Specs ++ lists:duplicate(max(0, 4 - length(Specs)), undefined),
    [format_partition_spec(SectorSize, Spec) || Spec <- PaddedSpecs].

format_volume_id(VolId) ->
    iolist_to_binary(io_lib:format("0x~8.16.0B", [VolId band 16#FFFFFFFF])).

parse_plist(#xmlElement{name = plist, content = Content}) ->
    [#{} = PList] = parse_plist_array(Content),
    PList;
parse_plist(#xmlElement{name = dict, content = Content}) ->
    parse_plist_dict(Content);
parse_plist(#xmlElement{name = array, content = Content}) ->
    parse_plist_array(Content);
parse_plist(#xmlElement{name = key, content = Content}) ->
    {key, iolist_to_binary([V || #xmlText{value = V} <- Content])};
parse_plist(#xmlElement{name = string, content = Content}) ->
    iolist_to_binary([V || #xmlText{value = V} <- Content]);
parse_plist(#xmlElement{name = integer, content = Content}) ->
    Text = iolist_to_binary([V || #xmlText{value = V} <- Content]),
    binary_to_integer(Text);
parse_plist(#xmlElement{name = real, content = Content}) ->
    Text = iolist_to_binary([V || #xmlText{value = V} <- Content]),
    binary_to_float(Text);
parse_plist(#xmlElement{name = true}) ->
    true;
parse_plist(#xmlElement{name = false}) ->
    false;
parse_plist(Other) ->
    Other.

parse_plist_array(Content) ->
    [parse_plist(E) || E = #xmlElement{} <- Content].

parse_plist_dict(Content) ->
    parse_plist_dict(parse_plist_array(Content), #{}).

parse_plist_dict([], Result) -> Result;
parse_plist_dict([{key, Key}, Value | Rest], Result) ->
    parse_plist_dict(Rest, Result#{Key => Value}).

parse_diskutil_plist(Data) ->
    DataAsBin = iolist_to_binary(Data),
    DataAsStr = unicode:characters_to_list(DataAsBin),
    XmlDoc = case xmerl_scan:string(DataAsStr) of
        {ParsedXml, []} -> ParsedXml;
        _ -> throw(<<"Invalid diskutil plist XML">>)
    end,
    RawPartInfoList = case parse_plist(XmlDoc) of
        #{<<"AllDisksAndPartitions">> := [#{
          <<"Content">> := <<"FDisk_partition_scheme">>,
          <<"Partitions">> := Partitions}]} ->
            Partitions;
        #{<<"AllDisksAndPartitions">> := [#{<<"Content">> := <<"">>}]} ->
            [];
        _ ->
            throw(<<"Invalid diskutil plist format">>)
    end,
    {_, RevPartSpecs} = lists:foldl(fun(RawInfo, {Idx, Acc}) ->
            #{<<"Content">> := RawType,
              <<"DeviceIdentifier">> := DevId,
              <<"Size">> := Size} = RawInfo,
            DeviceFile = <<"/dev/", DevId/binary>>,
            MountPoint = maps:get(<<"MountPoint">>, RawInfo, undefined),
            VolumeName = maps:get(<<"VolumeName">>, RawInfo, undefined),
            case is_device(DeviceFile) of
                true -> ok;
                enoent ->
                    throw(?FMT("Partition #~w device ~s not found",
                               [Idx, DeviceFile]));
                false ->
                    throw(?FMT("Partition #~w device ~s is not a proper device",
                               [Idx, DeviceFile]))
            end,
            case (MountPoint =:= undefined) orelse is_directory(MountPoint) of
                true -> ok;
                enoent ->
                    throw(?FMT("Partition #~w device ~s mount point ~s not found",
                               [Idx, DeviceFile, MountPoint]));
                false ->
                    throw(?FMT("Partition #~w device ~s mount point ~s is not a directory",
                               [Idx, DeviceFile, MountPoint]))
            end,
            Info = #{
                index => Idx,
                id => binary_to_atom(DevId),
                type => parse_diskutil_content(RawType),
                device_name => DevId,
                device_file => DeviceFile,
                size => Size,
                sector_size => 512,
                format => undefined,
                volume_name => VolumeName,
                mount_point => MountPoint
            },
            {Idx + 1, [Info | Acc]}
    end, {1, []}, RawPartInfoList),
    #{partitions => lists:reverse(RevPartSpecs)}.

parse_diskutil_content(<<"DOS_FAT_32">>) -> fat32;
parse_diskutil_content(<<"0x", Hex/binary>>) -> binary_to_integer(Hex, 16).

% Could use lists:zip/3 when OTP < 26 support is not required anymore
lists_zip(A, B, How) ->
    lists_zip(A, B, How, []).

lists_zip([], [], _How, Acc) ->
    lists:reverse(Acc);
lists_zip([], [B | RestB], {pad, PadA, _PadB} = How, Acc) ->
    lists_zip([], RestB, How, [{PadA, B} | Acc]);
lists_zip([A | RestA], [], {pad, _PadA, PadB} = How, Acc) ->
    lists_zip(RestA, [], How, [{A, PadB} | Acc]);
lists_zip([A | RestA], [B | RestB], How, Acc) ->
    lists_zip(RestA, RestB, How, [{A, B} | Acc]).

is_device(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = device}} -> true;
        {ok, #file_info{}} -> false;
        {error, Reason} -> Reason
    end.

is_directory(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = directory}} -> true;
        {ok, #file_info{}} -> false;
        {error, Reason} -> Reason
    end.
