# edifa

Erlang Disk and Image File Abstraction Library

Library to create system images without root privilege on both Linux and MacOS.


## Build

    $ rebar3 compile


## Runtime Dependencies

### MacOS

Commands that must be available:

 - rm
 - mv
 - mkdir
 - dd
 - mktemp
 - gzip
 - hdiutil
 - fdisk
 - diskutil
 - newfs_msdos


### Linux

Commands that must be available:

 - rm
 - mv
 - mkdir
 - dd
 - mktemp
 - gzip
 - id
 - sfdisk
 - fusefat
 - fusermount
 - mkfs.vfat
 
Install dependencies:

    sudo apt-get install coreutils fdisk fusefat dosfstools gzip


## Usage

Create an image file (64 MiB):

```erlang
{ok, Img} = edifa:create("test.img", 64 * 1024 * 1024).
```

Write some bootloader (2 MiB):

```erlang
ok = edifa:write(Img, "bootloader.bin", #{count => 2 * 1024 * 1024}).
```

Create partitions:

```erlang
{ok, P1, P2} = edifa:partition(Img, mbr, [
    #{type => fat32, size => 31 * 1024 * 1024, start => 2 * 1024 * 1024},
    #{type => fat32, size => 31 * 1024 * 1024}
]).
```

Format partition 1:

```erlang
ok = edifa:format(Img, P1, fat, #{label => "foobar"}).
```

Mount partition 1:

```erlang
{ok, MountPoint} = edifa:mount(Img, P1).
```

Create some files in partition 1 filesystem:

```erlang
ok = file:write_file(filename:join(MountPoint, "test.txt"), <<"some data\n">>).
```

Unmount partition 1:

```erlang
ok = edifa:unmount(Img, P1).
```

Extract partition 1 into its own file:

```erlang
ok = edifa:extract(Img, P1, "system.fs").
```

Generate a tuncated disk image containing the reserved space before the first
partition with the bootloader and the partition definition, and partition 1:

```erlang
ok = edifa:extract(Img, reserved, P1, "disk.img").
```

Extract into a gziped file:


Close the image:

```erlang
ok = edifa:close(Img).
```

```erlang
ok = edifa:extract(Img, P2, P2, "system.fs.gz").
```


## Limitations

 - All partitions involved must be unmounted before extracting data.
 - Only support MBR partition tables for now.
 - Active/Bootable partition flag is not supported yet.
 - Only support FAT filesystem for now.
