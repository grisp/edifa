-module(edifa_linux).


%--- Exports -------------------------------------------------------------------

% API Functions
-export([init/1]).
-export([terminate/2]).

-export([create/4]).
-export([write/3]).
-export([partition/3]).
-export([format/4]).
-export([mount/3]).
-export([unmount/2]).
-export([extract/4]).


%--- API Functions -------------------------------------------------------------

init(_Opts) ->
    {ok, #{}, [mkdir, dd, sfdisk, udisksctl, {mkvfat, "mkfs.vfat"},
               truncate, gzip, cp, mv, rm]}.

terminate(_State, _Reason) ->
    ok.

create(State, Filename, Size, Opts) ->
    edifa_generic:create(State, Filename, Size, Opts).

write(State, Filename, Opts) ->
    edifa_generic:write(State, Filename, Opts).

partition(_State, mbr, _Specs) ->
    {error, not_implemented}.

format(_State, _PartId, vfat, _Opts) ->
    {error, not_implemented}.

mount(_State, _PartId, _Opts) ->
    {error, not_implemented}.

unmount(_State, _PartId) ->
    {error, not_implemented}.

extract(State, From, To, Filename) ->
    edifa_generic:extract(State, From, To, Filename).
