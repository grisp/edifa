-module(edifa_SUITE).

-behaviour(ct_suite).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).


%--- Macros --------------------------------------------------------------------

-define(MiB, * 1024 * 1024).
-define(KiB, * 1024).


%--- COMMON TEST API -----------------------------------------------------------

all() -> [{group, sequential}].

groups() -> [{sequential, [sequence], tests()}].

tests() ->
    [F || {F, 1} <- ?MODULE:module_info(exports),
                    lists:suffix("_test", atom_to_list(F))].

init_per_suite(Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    BaseTmpDir = filename:join(PrivDir, "temp"),
    [{base_tmp_dir, BaseTmpDir} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    BaseTmpDir = proplists:get_value(base_tmp_dir, Config),
    TmpDir = filename:join(BaseTmpDir, atom_to_list(TestCase)),
    ImgFile = filename:join(TmpDir, "test.img"),
    GenericOpts = #{log_handler => fun log_handler/1},
    ImgOpts = GenericOpts#{temp_dir => TmpDir},
    [{img_file, ImgFile}, {gen_opts, GenericOpts},
     {img_opts, ImgOpts}, {tmp_dir, TmpDir} | Config].

end_per_testcase(_, Config) ->
    Config.


%--- Tests ---------------------------------------------------------------------

create_test(Config) ->
    ImgFile = proplists:get_value(img_file, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    {ok, Img} = edifa:create(ImgFile, 64?MiB, ImgOpts),
    ?assert(filelib:is_file(ImgFile)),
    ?assertEqual(64?MiB, filelib:file_size(ImgFile)),
    edifa:close(Img),
    ok.

write_test(Config) ->
    TmpDir = proplists:get_value(tmp_dir, Config),
    ImgFile = proplists:get_value(img_file, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    GenOpts = proplists:get_value(gen_opts, Config),
    SrcFile = filename:join(TmpDir, "src.bin"),
    ok = filelib:ensure_dir(SrcFile),
    Data = binary:copy(<<1, 2, 3, 4, 5, 6, 7, 8>>, 3),

    ok = file:write_file(SrcFile, Data),
    {ok, Img} = edifa:create(ImgFile, 16?KiB, ImgOpts),

    ?assertEqual(binary:copy(<<0>>, 25), pread(ImgFile, 0, 25)),
    ok = edifa:write(Img, SrcFile, GenOpts),
    ?assertEqual(<<Data/binary, 0>>, pread(ImgFile, 0, 25)),

    ?assertEqual(binary:copy(<<0>>, 22), pread(ImgFile, 99, 22)),
    ok = edifa:write(Img, SrcFile, GenOpts#{seek => 100, count => 20}),
    ?assertEqual(<<0, (binary:part(Data, 0, 20))/binary, 0>>,
                 pread(ImgFile, 99, 22)),

    ?assertEqual(binary:copy(<<0>>, 22), pread(ImgFile, 199, 22)),
    ok = edifa:write(Img, SrcFile, GenOpts#{max_block_size => 5, seek => 200, count => 20}),
    ?assertEqual(<<0, (binary:part(Data, 0, 20))/binary, 0>>,
                 pread(ImgFile, 199, 22)),

    ?assertEqual(binary:copy(<<0>>, 21), pread(ImgFile, 299, 21)),
    ok = edifa:write(Img, SrcFile, GenOpts#{skip => 5, seek => 300}),
    ?assertEqual(<<0, (binary:part(Data, 5, 19))/binary, 0>>,
                 pread(ImgFile, 299, 21)),

    ?assertEqual(binary:copy(<<0>>, 21), pread(ImgFile, 399, 21)),
    ok = edifa:write(Img, SrcFile, GenOpts#{max_block_size => 3, skip => 5, seek => 400}),
    ?assertEqual(<<0, (binary:part(Data, 5, 19))/binary, 0>>,
                 pread(ImgFile, 399, 21)),

    ?assertEqual(binary:copy(<<0>>, 18), pread(ImgFile, 499, 18)),
    ok = edifa:write(Img, SrcFile, GenOpts#{skip => 4, seek => 500, count => 16}),
    ?assertEqual(<<0, (binary:part(Data, 4, 16))/binary, 0>>,
                 pread(ImgFile, 499, 18)),

    ?assertEqual(binary:copy(<<0>>, 18), pread(ImgFile, 599, 18)),
    ok = edifa:write(Img, SrcFile, GenOpts#{max_block_size => 2, skip => 4, seek => 600, count => 16}),
    ?assertEqual(<<0, (binary:part(Data, 4, 16))/binary, 0>>,
                 pread(ImgFile, 599, 18)),

    ok = edifa:close(Img, GenOpts),
    ok.

partition_test(Config) ->
    ImgFile = proplists:get_value(img_file, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    GenOpts = proplists:get_value(gen_opts, Config),
    {ok, Img} = edifa:create(ImgFile, 516?MiB, ImgOpts),
    {ok, [_, _]} = edifa:partition(Img, mbr, [
        #{type => fat32, start => 4?MiB, size => 256?MiB},
        #{type => fat32, size => 256?MiB}
    ], GenOpts),
    ok = edifa:close(Img, GenOpts),
    ok.

format_test(Config) ->
    ImgFile = proplists:get_value(img_file, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    GenOpts = proplists:get_value(gen_opts, Config),
    {ok, Img} = edifa:create(ImgFile, 68?MiB, ImgOpts),
    {ok, [P1, P2]} = edifa:partition(Img, mbr, [
        #{type => fat32, start => 1?MiB, size => 3?MiB},
        #{type => fat32, size => 64?MiB}
    ], GenOpts),
    ok = edifa:format(Img, P1, fat, GenOpts#{identifier => 123456789}),
    ok = edifa:format(Img, P2, fat, GenOpts#{type => 32, cluster_size => 1, label => "foobar"}),
    ok = edifa:close(Img, GenOpts),
    ok.

mount_and_unmount_test(Config) ->
    ImgFile = proplists:get_value(img_file, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    GenOpts = proplists:get_value(gen_opts, Config),
    {ok, Img} = edifa:create(ImgFile, 8?MiB, ImgOpts),
    {ok, [P1]} = edifa:partition(Img, mbr, [
        #{type => fat32, start => 2?MiB, size => 6?MiB}
    ], GenOpts),
    ok = edifa:format(Img, P1, fat, #{label => "foobar"}),
    {ok, MountPoint} = edifa:mount(Img, P1, GenOpts),
    ?assertEqual(true, filelib:is_dir(MountPoint)),
    Data1 = crypto:strong_rand_bytes(32),
    Filename1 = filename:join(MountPoint, "test"),
    file:write_file(Filename1, Data1),
    ok = edifa:unmount(Img, P1, GenOpts),
    ?assertEqual(false, filelib:is_file(Filename1)),
    {ok, MountPoint2} = edifa:mount(Img, P1, GenOpts),
    ?assertEqual(true, filelib:is_dir(MountPoint2)),
    {ok, Data2} = file:read_file(filename:join(MountPoint2, "test")),
    ?assertEqual(Data1, Data2),
    ok = edifa:close(Img, GenOpts),
    ok.

extract_test(Config) ->
    TmpDir = proplists:get_value(tmp_dir, Config),
    ImgOpts = proplists:get_value(img_opts, Config),
    GenOpts = proplists:get_value(gen_opts, Config),
    {ok, Img} = edifa:create(undefined, 17?MiB, ImgOpts),
    {ok, [P1, P2, P3, P4]} = edifa:partition(Img, mbr, [
        #{type => fat32, start => 1?MiB, size => 4?MiB},
        #{type => fat32, start => 1?MiB, size => 4?MiB},
        #{type => fat32, start => 1?MiB, size => 4?MiB},
        #{type => fat32, start => 1?MiB, size => 4?MiB}
    ], GenOpts),
    % Format partition 1
    ok = edifa:format(Img, P1, fat, GenOpts#{label => "foobar"}),
    % Write some random data in a file on partition 1 and unmount
    {ok, MountPoint1} = edifa:mount(Img, P1, GenOpts),
    Data1 = crypto:strong_rand_bytes(32),
    Filename1 = filename:join(MountPoint1, "test"),
    ok = file:write_file(Filename1, Data1),
    ok = edifa:unmount(Img, P1, GenOpts),
    % Extract Paritition 1
    P1Filename = filename:join(TmpDir, "p1.bin"),
    ok  = edifa:extract(Img, P1, P1, P1Filename, GenOpts),
    ?assert(filelib:is_file(P1Filename)),
    ?assertEqual(4?MiB, filelib:file_size(P1Filename)),
    % Write Parition 1 into parition 2
    ok = edifa:write(Img, P1Filename, GenOpts#{count => 4?MiB, seek => (1 + 4)?MiB}),
    % Mount Partition 2, and check the data is there
    {ok, MountPoint2} = edifa:mount(Img, P2, GenOpts),
    Filename2 = filename:join(MountPoint2, "test"),
    ?assertEqual({ok, Data1}, file:read_file(Filename2)),
    % Change the data on partition2 and unmount
    Data2 = crypto:strong_rand_bytes(32),
    ok = file:write_file(Filename2, Data2),
    ok = edifa:unmount(Img, P2, GenOpts),
    % Extract partition 1 to 2
    P1P2Filename = filename:join(TmpDir, "p1-p2.bin"),
    P1P2FilenameGz = P1P2Filename ++ ".gz",
    ok = edifa:extract(Img, P1, P2, P1P2FilenameGz, GenOpts#{compressed => true}),
    ?assertNot(filelib:is_file(P1P2Filename)),
    ?assert(filelib:is_file(P1P2FilenameGz)),
    ?assert(filelib:file_size(P1P2FilenameGz) < 8?MiB),
    os:cmd("gunzip " ++ P1P2FilenameGz),
    ?assert(filelib:is_file(P1P2Filename)),
    ?assertEqual(8?MiB, filelib:file_size(P1P2Filename)),
    % Write Parition 1 and 2 into parition 3 and 4
    ok = edifa:write(Img, P1P2Filename, #{count => 8?MiB, seek => (1 + 4 + 4)?MiB}),
    % Mount Partition 3 and 4, and check the data is from partition 1 and 2
    % Need to use explicit mount points becuase the volume names are the same
    MountPoint3 = filename:join(TmpDir, <<"p3">>),
    MountPoint4 = filename:join(TmpDir, <<"p4">>),
    {ok, MountPoint3} = edifa:mount(Img, P3, GenOpts#{mount_point => MountPoint3}),
    {ok, MountPoint4} = edifa:mount(Img, P4, GenOpts#{mount_point => MountPoint4}),
    Filename3 = filename:join(MountPoint3, "test"),
    Filename4 = filename:join(MountPoint4, "test"),
    ?assertEqual({ok, Data1}, file:read_file(Filename3)),
    ?assertEqual({ok, Data2}, file:read_file(Filename4)),
    ok = edifa:unmount(Img, P3, GenOpts),
    ok = edifa:unmount(Img, P4, GenOpts),
    ok = edifa:close(Img, GenOpts),
    ok.

log_handler_test(_Config) ->
    LogHandler = fun(Event, Acc) -> [Event | Acc] end,
    GenOpts = #{log_handler => LogHandler, log_state => []},
    {ok, Img, _LogState1} = edifa:create(undefined, 17?MiB, GenOpts),
    {ok, _LogState2} = edifa:close(Img, GenOpts),
    ok.

%--- Internal ------------------------------------------------------------------

log_handler({exec, Args}) ->
    LogLine = iolist_to_binary(lists:join(" ", Args)),
    io:format("exec: ~s", [LogLine]);
log_handler({exit, {status, Status}}) ->
    io:format("[exit ~w]", [Status]);
log_handler({exit, {signal, Sig}}) ->
    io:format("[signal ~w]", [Sig]);
log_handler({Stream, eof})
  when Stream =:= stdin; Stream =:= stdout; Stream =:= stderr ->
    io:format("[~s closed]", [Stream]);
log_handler({Stream, Data})
  when Stream =:= stdin; Stream =:= stdout; Stream =:= stderr ->
    Str = unicode:characters_to_list(Data),
    io:format("~s ~s", [log_tag(Stream), string:strip(Str, right, $\n)]);
log_handler({Tag, Term})
  when Tag =:= result; Tag =:= error ->
    io:format("~s: ~p", [Tag, Term]).

log_tag(stdin) -> "<<";
log_tag(stdout) -> "1>";
log_tag(stderr) -> "2>".

pread(Filename, Offset, Size) ->
    {ok, F} = file:open(Filename, [read, raw, binary]),
    {ok, Result} = file:pread(F, Offset, Size),
    file:close(F),
    Result.
