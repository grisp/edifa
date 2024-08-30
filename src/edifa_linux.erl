-module(edifa_linux).


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
    {ok, State,
        [id, sfdisk, fusefat, fusermount, {mkvfat, "mkfs.vfat"}
         | Commands]}.

terminate(State, _Reason) ->
    edifa_exec:command(State, cmd_cleanup(State)).

create(State, Filename, Size, Opts) ->
    edifa_generic:create(State, Filename, Size, Opts).

write(State, Filename, Opts) ->
    edifa_generic:write(State, Filename, Opts).

partition(State = #{image_filename := ImageFilename}, mbr, Specs, _Opts) ->
    PartitionSpecs = format_partition_specs(512, Specs),
    edifa_exec:command(State, [
        #{
            cmd => sfdisk,
            args => ["-f", ImageFilename],
            input => [PartitionSpecs, eof]
        },
        fun(State2, _) ->
            edifa_generic:with_temp_dir(State2, fun(State3, TempDir) ->
                BaseName = filename:basename(ImageFilename),
                {_, RevIds, PartMap} = lists:foldl(fun(Spec, {Idx, Ids, Map}) ->
                    Name = <<"p", (integer_to_binary(Idx))/binary>>,
                    Id = binary_to_atom(Name),
                    Filename = <<BaseName/binary, ".", Name/binary, ".fs">>,
                    Filepath = filename:join(TempDir, Filename),
                    Spec2 = Spec#{
                        index => Idx,
                        id => Id,
                        name => Name,
                        sector_size => 512,
                        format => undefined,
                        filepath => Filepath,
                        mount_point => undefined
                    },
                    {Idx + 1, [Id | Ids], Map#{Id => Spec2}}
                end, {1, [], #{}}, Specs),
                PartIds = lists:reverse(RevIds),
                State4 = State3#{partition_ids => PartIds, partitions => PartMap},
                {ok, PartIds, State4}
            end)
        end
    ]);
partition(_State, mbr, _Specs, _Opts) ->
    {error, <<"No image created">>}.

format(State = #{image_filename := ImageFile, partitions := PartMap},
       PartId, fat, Opts) ->
    case maps:find(PartId, PartMap) of
        error -> {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{sector_size := SecSize, filepath := PartFile,
               start := Start, size := Size}} ->
            BlockSize = Size div 1024,
            Args = case maps:get(type, Opts, undefined) of
                undefined -> [];
                FatType when FatType =:= 12; FatType =:= 16; FatType =:= 32 ->
                    ["-F", integer_to_list(FatType)]
            end ++ case maps:get(identifier, Opts, undefined) of
                undefined -> [];
                VolId when is_integer(VolId) ->
                    ["-i", format_volume_id(VolId)]
            end ++ case maps:get(label, Opts, undefined) of
                undefined -> [];
                VolName when is_binary(VolName) ->
                    ["-n", VolName]
            end ++ case maps:get(cluster_size, Opts, undefined) of
                undefined -> [];
                ClustSize when is_integer(ClustSize) ->
                    ["-s", integer_to_list(ClustSize)]
            end ++ ["-S", integer_to_list(SecSize),
                    "-C", PartFile, integer_to_list(BlockSize)],
            edifa_exec:command(State, [
                edifa_generic:cmd_rm(true, PartFile),
                #{cmd => mkvfat, args => Args},
                edifa_generic:cmd_dd(PartFile, ImageFile,
                                     #{count => Size, seek => Start}),
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

mount(State = #{image_filename := ImageFile, partitions := PartMap,
                temp_dir := TempDir},
      PartId, Opts) ->
    case maps:find(PartId, PartMap) of
        error ->
            {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{mount_point := MountPoint}}
          when MountPoint =/= undefined ->
            {ok, MountPoint, State};
        {ok, #{start := Start, size := Size, name := Name, filepath := PartFile}} ->
            BaseName = filename:basename(ImageFile),
            DirName = <<BaseName/binary, ".", Name/binary>>,
            DefaultMountPoint = filename:join(TempDir, DirName),
            MountPoint = maps:get(mount_point, Opts, DefaultMountPoint),
            with_user_ids(State, fun(State2, {UId, GId}) ->
                UIdStr = integer_to_list(UId),
                GIdStr = integer_to_list(GId),
                IdOpts = "uid=" ++ UIdStr ++ ",gid=" ++ GIdStr,
                edifa_exec:command(State2, [
                    edifa_generic:cmd_mkdir(MountPoint),
                    edifa_generic:cmd_rm(true, PartFile),
                    edifa_generic:cmd_dd(ImageFile, PartFile,
                                         #{count => Size, skip => Start}),
                    #{
                        cmd => fusefat,
                        args => [
                            "-o", "rw+,umask=0133," ++ IdOpts,
                            PartFile,
                            MountPoint
                        ]
                    },
                    fun(State3, undefined) ->
                        #{partitions := #{PartId := OldInfo} = Parts} = State3,
                        NewInfo = OldInfo#{mount_point => MountPoint},
                        State4 = State3#{partitions => Parts#{PartId => NewInfo}},
                        {ok, MountPoint, State4}
                    end
                ])
            end)
    end;
mount(_State, _PartId, _Opts) ->
    {error, <<"No partition table defined">>}.

unmount(State = #{image_filename := ImageFile, partitions := PartMap},
        PartId, _Opts) ->
    case maps:find(PartId, PartMap) of
        error -> {error, ?FMT("Unknown partition ~s", [PartId])};
        {ok, #{mount_point := undefined}} -> {ok, State};
        {ok, #{start := Start, size := Size,
               filepath := PartFile, mount_point := MountPoint}} ->
            retry_unmount(State, MountPoint, 5, 1000, fun(State2, _) ->
                edifa_exec:command(State2, [
                    edifa_generic:cmd_dd(PartFile, ImageFile,
                                         #{count => Size, seek => Start}),
                    edifa_generic:cmd_rm(true, PartFile),
                    fun(State3, _) ->
                        #{partitions := #{PartId := OldInfo} = Parts} = State3,
                        NewInfo = OldInfo#{mount_point => undefined},
                        State4 = State3#{partitions => Parts#{PartId => NewInfo}},
                        {ok, State4}
                    end
                ])
            end)
    end;
unmount(_State, _PartId, _Opts) ->
    {error, <<"No partition table defined">>}.

extract(State, From, To, Filename, Opts) ->
    edifa_generic:extract(State, From, To, Filename, Opts).


%--- Internal Functions --------------------------------------------------------

cmd_cleanup(State) ->
    cmd_cleanup_partitions(State)
        ++ edifa_generic:cmd_cleanup(State).

cmd_cleanup_partitions(#{partitions := PartMap}) ->
    cmd_cleanup_partitions(maps:values(PartMap), []);
cmd_cleanup_partitions(#{}) ->
    [].

cmd_cleanup_partitions([], Acc) ->
    lists:reverse(Acc);
cmd_cleanup_partitions([#{filepath := F, mount_point := M} | Rest], Acc)
  when F =/= undefined, M =/= undefined ->
    cmd_cleanup_partitions(Rest, [
        #{cmd => fusermount, args => ["-u", M]},
        edifa_generic:cmd_rm(true, F) | Acc]);
cmd_cleanup_partitions([#{filepath := F} | Rest], Acc)
  when F =/= undefined ->
    cmd_cleanup_partitions(Rest, [edifa_generic:cmd_rm(true, F) | Acc]);
cmd_cleanup_partitions([#{} | Rest], Acc) ->
    cmd_cleanup_partitions(Rest, Acc).

with_user_ids(State = #{user_id := UId, groups_id := GId}, Fun)
  when is_integer(UId), is_integer(GId) ->
    Fun(State, {UId, GId});
with_user_ids(State, Fun) ->
    edifa_exec:command(State, [
        #{cmd => id, args => ["-u"],
          result => {collect, stdout}, keep_result => user_id},
        #{cmd => id, args => ["-g"],
          result => {collect, stdout}, keep_result => group_id},
        fun(State2 = #{user_id := UIdBin, group_id := GIdBin}, _) ->
            UIdStr = unicode:characters_to_list(UIdBin),
            GIdStr = unicode:characters_to_list(GIdBin),
            UId = list_to_integer(string:trim(UIdStr)),
            GId = list_to_integer(string:trim(GIdStr)),
            {ok, {UId, GId}, State2#{user_id := UId, group_id := GId}}
        end,
        Fun
    ]).

retry_unmount(State, MountPoint, MaxIntents, SleepTime, Fun) ->
    State2 = State#{unmount_left => MaxIntents, unmount_sleep => SleepTime},
    retry_unmount(State2, MountPoint, <<"failed to unmount">>, Fun).

retry_unmount(State = #{unmount_left := IntentLeft},
              _MountPoint, LastReason, _Fun)
    when IntentLeft < 1 ->
        {error, LastReason, retry_unmount_cleanup(State)};
retry_unmount(State, MountPoint, _LastReason, Fun) ->
    edifa_exec:command(State, [
        #{
            cmd => fusermount,
            args => ["-u", MountPoint],
            error_as_result => true
        },
        fun
            (State2 = #{unmount_left := MaxIntents, unmount_sleep := SleepTime},
             {error, <<"failed to unmount", _/binary>> = Reason}) ->
                State3 = State2#{unmount_left => MaxIntents - 1},
                timer:sleep(SleepTime),
                retry_unmount(State3, MountPoint, Reason, Fun);
            (State2,  {error, Reason}) ->
                {error, Reason, retry_unmount_cleanup(State2)};
            (State2, Result) ->
                Fun(State2, Result)
        end
    ]).

retry_unmount_cleanup(State) ->
    maps:remove(unmount_left, maps:remove(unmount_sleep, State)).

format_partition_type(fat32) -> <<"b">>;
format_partition_type(TypeId) when is_integer(TypeId), TypeId > 0 ->
    io_lib:format("~2.16.0B", [TypeId]).

format_partition_spec(SectorSize, #{type := Type, start := Start, size := Size})
  when is_integer(Start), Start >= 0, (Start rem SectorSize) =:= 0,
       is_integer(Size), Size >= 0, (Size rem SectorSize) =:= 0 ->
    [io_lib:format("start=~w, size=~w, type=",
                   [Start div SectorSize, Size div SectorSize]),
     format_partition_type(Type), "\n"].

format_partition_specs(SectorSize, Specs) when is_list(Specs) ->
    [<<"unit: sectors\n">>,
     <<"label: dos\n">>,
        [format_partition_spec(SectorSize, Spec) || Spec <- Specs]
    ].

format_volume_id(VolId) ->
    iolist_to_binary(io_lib:format("~8.16.0b", [VolId band 16#FFFFFFFF])).
