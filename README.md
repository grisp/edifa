# edifa

Erlang Disk and Image File Abstraction Library

## Build

    $ rebar3 compile

## Runtime Dependencies

### MacOS

Commands that must be available:

 - mkdir
 - dd
 - hdiutil
 - fdisk
 - diskutil
 - newfs_msdos


## Usage

Create an image file (64 MiB):

    {ok, Img} = edifa:create("test.img", 64 * 1024 * 1024).

Write some bootloader (2 MiB):

    ok = edifa:write(Img, "bootloader.bin", #{count => 2 * 1024 * 1024}).

Create partitions:

    {ok, P1, P2} = edifa:partition(Img, mbr, [
        #{type => fat32, size => 31 * 1024 * 1024, start => 2 * 1024 * 1024},
        #{type => fat32, size => 31 * 1024 * 1024}
    ]).

Format partition 1:

    ok = edifa:format(Img, P1, fat, #{label => "foobar"}).

Mount partition 1:

    {ok, MountPoint} = edifa:mount(Img, P1).

Create some files in partition 1 filesystem:

    ok = file:write_file(filename:join(MountPoint, "test.txt"), <<"some data\n">>).

Unmount partition 1:

    ok = edifa:unmount(Img, P1).

Extract partition 1 into its own file:

    ok = edifa:extract(Img, P1, "system.fs").

Generate a tuncated disk image containing the reserved space before the first
partition with the bootloader and the partition definition, and partition 1:

    ok = edifa:extract(Img, reserved, P1, "disk.img").

Close the image:

    ok = edifa:close(Img).


## Limitations

All partitions involved must be unmounted before extracting data.
