%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_woody_api_configurator).
-include_lib("kernel/include/file.hrl").

-export([parse_yaml_config/1]).
-export([write_files      /1]).
-export([write_file       /1]).
-export([print_sys_config /1]).
-export([print_vm_args    /1]).
-export([print_erl_inetrc /1]).

-export([guess_host_address/1]).
-export([hostname/0]).

-export([filename         /1]).
-export([file             /2]).
-export([log_level        /1]).
-export([mem_words        /1]).
-export([mem_bytes        /1]).
-export([time_interval    /1]).
-export([time_interval    /2]).
-export([proplist         /1]).
-export([ip               /1]).
-export([utf_bin          /1]).
-export([atom             /1]).
-export([conf             /3]).
-export([conf             /2]).
-export([probability      /1]).
-export([contents         /1]).

%%

-type yaml_config() :: _TODO. % hello to librares without an explicit typing 😡
-type yaml_config_path() :: [atom()].

-type vm_args() :: [{atom(), binary()}].
-type sys_config() :: [{atom, term()}].
-type erl_inetrc() :: [{atom, term()}].

-type filename() :: file:filename().
-type mem_words() :: non_neg_integer().
-type mem_bytes() :: non_neg_integer().
-type maybe(T) :: undefined | T.

-type time_interval_unit() :: 'week' | 'day' | 'hour' | 'min' | 'sec' | 'ms' | 'mu'.
-type time_interval() :: {non_neg_integer(), time_interval_unit()}.

-spec parse_yaml_config(filename()) ->
    yaml_config().
parse_yaml_config(Filename) ->
    {ok, _} = application:ensure_all_started(yamerl),
    [Config] = yamerl_constr:file(Filename),
    Config.

-type file_contents() ::
    {filename(), iolist()} |
    {filename(), iolist(), _Mode :: non_neg_integer()}.

-spec write_files([file_contents()]) ->
    ok.
write_files(Files) ->
    ok = lists:foreach(fun write_file/1, Files).

-spec write_file(file_contents()) ->
    ok.
write_file({Name, Data}) ->
    ok = file:write_file(Name, Data);
write_file({Name, Data, Mode}) ->
    % Turn write permission on temporarily
    _ = file:change_mode(Name, Mode bor 8#00200),
    % Truncate it
    ok = file:write_file(Name, <<>>),
    ok = file:change_mode(Name, Mode bor 8#00200),
    % Write contents
    ok = file:write_file(Name, Data),
    % Drop write permission (if `Mode` doesn't specify it)
    ok = file:change_mode(Name, Mode).

-spec print_sys_config(sys_config()) ->
    iolist().
print_sys_config(SysConfig) ->
    [io_lib:print(SysConfig), $., $\n].

-spec print_vm_args(vm_args()) ->
    iolist().
print_vm_args(VMArgs) ->
    lists:foldr(
        fun({Arg, Value}, Acc) ->
            [[erlang:atom_to_binary(Arg, utf8), $\s, Value, $\n]|Acc]
        end,
        [],
        VMArgs
    ).

-spec print_erl_inetrc(erl_inetrc()) ->
    iolist().
print_erl_inetrc(ERLInetrc) ->
    [[io_lib:print(E), $., $\n] || E <- ERLInetrc].

-spec filename(maybe(string())) ->
    maybe(filename()).
filename(Filename) when is_list(Filename) ->
    Filename;
filename(Filename) ->
    erlang:throw({bad_file_name, Filename}).

-spec file(maybe(string()), _AtMostMode :: non_neg_integer()) ->
    filename().
file(Filename, AtMostMode) ->
    _ = filename(Filename),
    case file:read_file_info(Filename) of
        {ok, #file_info{type = regular, mode = Mode}} ->
            case (Mode band 8#777) bor AtMostMode of
                AtMostMode ->
                    Filename;
                _ ->
                    erlang:throw({'bad file mode', Filename, io_lib:format("~.8.0B", [Mode])})
            end;
        {ok, #file_info{type = Type}} ->
            erlang:throw({'bad file type', Filename, Type});
        {error, Reason} ->
            erlang:throw({'error accessing file', Filename, Reason})
    end.

-spec guess_host_address(inet:address_family()) ->
    inet:ip_address().
guess_host_address(AddressFamilyPreference) ->
    {ok, Ifaces0} = inet:getifaddrs(),
    Ifaces1 = filter_running_ifaces(Ifaces0),
    IfaceAddrs0 = gather_iface_addrs(Ifaces1, AddressFamilyPreference),
    [{_Name, Addr} | _] = sort_iface_addrs(IfaceAddrs0),
    Addr.

-type iface_name() :: string().
-type iface()      :: {iface_name(), proplists:proplist()}.

-spec filter_running_ifaces([iface()]) ->
    [iface()].
filter_running_ifaces(Ifaces) ->
    lists:filter(
        fun ({_, Ps}) -> is_iface_running(proplists:get_value(flags, Ps)) end,
        Ifaces
    ).

-spec is_iface_running([up | running | atom()]) ->
    boolean().
is_iface_running(Flags) ->
    [] == [up, running] -- Flags.

-spec gather_iface_addrs([iface()], inet:address_family()) ->
    [{iface_name(), inet:ip_address()}].
gather_iface_addrs(Ifaces, Pref) ->
    lists:filtermap(
        fun ({Name, Ps}) -> choose_iface_address(Name, proplists:get_all_values(addr, Ps), Pref) end,
        Ifaces
    ).

-spec choose_iface_address(iface_name(), [inet:ip_address()], inet:address_family()) ->
    false | {true, {iface_name(), inet:ip_address()}}.
choose_iface_address(Name, [Addr = {_, _, _, _} | _], inet) ->
    {true, {Name, Addr}};
choose_iface_address(Name, [Addr = {_, _, _, _, _, _, _, _} | _], inet6) ->
    {true, {Name, Addr}};
choose_iface_address(Name, [_ | Rest], Pref) ->
    choose_iface_address(Name, Rest, Pref);
choose_iface_address(_, [], _) ->
    false.

-spec sort_iface_addrs([{iface_name(), inet:ip_address()}]) ->
    [{iface_name(), inet:ip_address()}].
sort_iface_addrs(IfaceAddrs) ->
    lists:sort(fun ({N1, _}, {N2, _}) -> get_iface_prio(N1) =< get_iface_prio(N2) end, IfaceAddrs).

-spec get_iface_prio(iface_name()) ->
    integer().
get_iface_prio("eth" ++ _) -> 1;
get_iface_prio("en"  ++ _) -> 1;
get_iface_prio("wl"  ++ _) -> 2;
get_iface_prio("tun" ++ _) -> 3;
get_iface_prio("lo"  ++ _) -> 4;
get_iface_prio(_)          -> 100.


-spec hostname() ->
    inet:hostname().
hostname() ->
    {ok, Name} = inet:gethostname(),
    Name.

-spec
log_level(string()  ) -> atom().
log_level("critical") -> critical;
log_level("error"   ) -> error;
log_level("warning" ) -> warning;
log_level("info"    ) -> info;
log_level("debug"   ) -> debug;
log_level("trace"   ) -> trace;
log_level(BadLevel  ) -> erlang:throw({bad_log_level, BadLevel}).


-spec mem_words(maybe(string())) ->
    maybe(mem_words()).
mem_words(undefined) ->
    undefined;
mem_words(MemStr) ->
    mem_bytes(MemStr) div erlang:system_info(wordsize).


-spec mem_bytes(maybe(string())) ->
    maybe(mem_bytes()).
mem_bytes(undefined) ->
    undefined;
mem_bytes(MemStr) ->
    Error = {'bad memory amount', MemStr},
    case string:to_upper(lists:reverse(string:strip(MemStr))) of
        "P" ++ RevTail -> pow2x0(5) * rev_str_int(RevTail, Error);
        "T" ++ RevTail -> pow2x0(4) * rev_str_int(RevTail, Error);
        "G" ++ RevTail -> pow2x0(3) * rev_str_int(RevTail, Error);
        "M" ++ RevTail -> pow2x0(2) * rev_str_int(RevTail, Error);
        "K" ++ RevTail -> pow2x0(1) * rev_str_int(RevTail, Error);
        "B" ++ RevTail -> pow2x0(0) * rev_str_int(RevTail, Error);
        _              -> erlang:throw(Error)
    end.

-spec rev_str_int(string(), Error::term()) ->
    integer().
rev_str_int(RevIntStr, Error) ->
    IntStr = lists:reverse(RevIntStr),
    try
        list_to_integer(IntStr)
    catch error:badarg ->
        erlang:throw(Error)
    end.

-spec pow2x0(integer()) ->
    integer().
pow2x0(X) ->
    1 bsl (X * 10).


-spec time_interval(maybe(string())) ->
    maybe(time_interval()).
time_interval(undefined) ->
    undefined;
time_interval(TimeStr) ->
    parse_time_interval(TimeStr).

-spec time_interval(maybe(string()), time_interval_unit()) ->
    maybe(timeout()).
time_interval(undefined, _) ->
    undefined;
time_interval("infinity", _) ->
    infinity;
time_interval(TimeStr, Unit) ->
    time_interval_in(parse_time_interval(TimeStr), Unit).

-spec parse_time_interval(string()) ->
    time_interval().
parse_time_interval(TimeStr) ->
    Error = {'bad time interval', TimeStr},
    case string:to_upper(lists:reverse(string:strip(TimeStr))) of
        "W"  ++ RevTail -> {rev_str_int(RevTail, Error), 'week'};
        "D"  ++ RevTail -> {rev_str_int(RevTail, Error), 'day' };
        "H"  ++ RevTail -> {rev_str_int(RevTail, Error), 'hour'};
        "M"  ++ RevTail -> {rev_str_int(RevTail, Error), 'min' };
        "SM" ++ RevTail -> {rev_str_int(RevTail, Error), 'ms'  };
        "UM" ++ RevTail -> {rev_str_int(RevTail, Error), 'mu'  };
        "S"  ++ RevTail -> {rev_str_int(RevTail, Error), 'sec' };
        _               -> erlang:throw(Error)
    end.

-spec time_interval_in(time_interval(), time_interval_unit()) ->
    non_neg_integer().
time_interval_in({Amount, UnitFrom}, UnitTo) ->
    time_interval_in_(Amount, time_interval_unit_to_int(UnitFrom), time_interval_unit_to_int(UnitTo)).

-spec time_interval_in_(non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
    non_neg_integer().
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom =:= UnitTo ->
    Amount;
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom < UnitTo ->
    time_interval_in_(Amount div time_interval_mul(UnitFrom + 1), UnitFrom + 1, UnitTo);
time_interval_in_(Amount, UnitFrom, UnitTo) when UnitFrom > UnitTo ->
    time_interval_in_(Amount * time_interval_mul(UnitFrom), UnitFrom - 1, UnitTo).

-spec time_interval_unit_to_int(time_interval_unit()) ->
    non_neg_integer().
time_interval_unit_to_int('week') -> 6;
time_interval_unit_to_int('day' ) -> 5;
time_interval_unit_to_int('hour') -> 4;
time_interval_unit_to_int('min' ) -> 3;
time_interval_unit_to_int('sec' ) -> 2;
time_interval_unit_to_int('ms'  ) -> 1;
time_interval_unit_to_int('mu'  ) -> 0.

-spec time_interval_mul(non_neg_integer()) ->
    non_neg_integer().
time_interval_mul(6) -> 7;
time_interval_mul(5) -> 24;
time_interval_mul(4) -> 60;
time_interval_mul(3) -> 60;
time_interval_mul(2) -> 1000;
time_interval_mul(1) -> 1000.

-spec proplist(yaml_config() | undefined) ->
    proplists:proplist() | undefined.
proplist(undefined) ->
    undefined;
proplist(Config) ->
    [{erlang:list_to_existing_atom(Key), Value} || {Key, Value} <- Config].

-spec ip(string()) ->
    inet:ip_address().
ip(Host) ->
    mg_utils:throw_if_error(inet:parse_address(Host)).

-spec utf_bin(string()) ->
    binary().
utf_bin(IDStr) ->
    unicode:characters_to_binary(IDStr, utf8).

-spec atom(string()) ->
    atom().
atom(AtomStr) ->
    erlang:binary_to_atom(utf_bin(AtomStr), utf8).

-spec conf(yaml_config_path(), yaml_config(), _) ->
    _.
conf(Path, Config, Default) ->
    conf_({default, Default}, Path, Config).

-spec conf(yaml_config_path(), yaml_config()) ->
    _.
conf(Path, Config) ->
    conf_({throw, Path}, Path, Config).

-spec conf_({throw, yaml_config_path()} | {default, _}, yaml_config_path(), yaml_config()) ->
    _.
conf_(_, [], Value) ->
    Value;
conf_(Throw, Key, Config) when is_atom(Key) andalso is_list(Config) ->
    case lists:keyfind(erlang:atom_to_list(Key), 1, Config) of
        false      -> conf_maybe_default(Throw);
        {_, Value} -> Value
    end;
conf_(Throw, [Key|Path], Config) when is_list(Path) andalso is_list(Config) ->
    case lists:keyfind(erlang:atom_to_list(Key), 1, Config) of
        false      -> conf_maybe_default(Throw);
        {_, Value} -> conf_(Throw, Path, Value)
    end.

-spec conf_maybe_default({throw, yaml_config_path()} | {default, _}) ->
    _ | no_return().
conf_maybe_default({throw, Path}) ->
    erlang:throw({'config element not found', Path});
conf_maybe_default({default, Default}) ->
    Default.

-spec probability(term()) ->
    float() | integer()| no_return().
probability(Prob) when is_number(Prob) andalso 0 =< Prob andalso Prob =< 1 ->
    Prob;
probability(Prob) ->
    throw({'bad probability', Prob}).

-spec contents(filename()) ->
    binary().
contents(Filename) ->
    case file:read_file(Filename) of
        {ok, Contents} ->
            Contents;
        {error, Reason} ->
            erlang:throw({'could not read file contents', Filename, Reason})
    end.
