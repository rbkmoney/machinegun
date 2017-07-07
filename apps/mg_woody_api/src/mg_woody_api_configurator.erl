-module(mg_woody_api_configurator).

-export([parse_yaml_config/1]).
-export([write_files      /1]).
-export([write_file       /1]).
-export([print_sys_config /1]).
-export([print_vm_args    /1]).

-export([filename        /1]).
-export([log_level       /1]).
-export([mem_words       /1]).
-export([mem_bytes       /1]).
-export([ip              /1]).
-export([utf_bin         /1]).
-export([conf            /3]).
-export([conf            /2]).

%%

-type yaml_config() :: _TODO. % hello to librares without an explicit typing 😡
-type yaml_config_path() :: [atom()].

-type vm_args() :: [{atom(), binary()}].
-type sys_config() :: [{atom, term()}].

-type filename() :: file:filename().
-type mem_words() :: non_neg_integer().
-type mem_bytes() :: non_neg_integer().
-type maybe(T) :: undefined | T.


-spec parse_yaml_config(filename()) ->
    yaml_config().
parse_yaml_config(Filename) ->
    {ok, _} = application:ensure_all_started(yamerl),
    [Config] = yamerl_constr:file(Filename),
    Config.

-spec write_files([{filename(), iolist()}]) ->
    ok.
write_files(Files) ->
    ok = lists:foreach(fun write_file/1, Files).

-spec write_file({filename(), iolist()}) ->
    ok.
write_file({Name, Data}) ->
    ok = file:write_file(Name, Data).

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


-spec filename(maybe(string())) ->
    maybe(filename()).
filename(Filename) when is_list(Filename) ->
    Filename;
filename(Filename) ->
    erlang:throw({bad_file_name, Filename}).

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
    case lists:reverse(string:strip(MemStr)) of
        "P" ++ RevTailMem -> pow2x0(5) * rev_str_int(RevTailMem);
        "T" ++ RevTailMem -> pow2x0(4) * rev_str_int(RevTailMem);
        "G" ++ RevTailMem -> pow2x0(3) * rev_str_int(RevTailMem);
        "M" ++ RevTailMem -> pow2x0(2) * rev_str_int(RevTailMem);
        "K" ++ RevTailMem -> pow2x0(1) * rev_str_int(RevTailMem);
        ""  ++ RevTailMem -> pow2x0(0) * rev_str_int(RevTailMem)
    end.

-spec rev_str_int(string()) ->
    integer().
rev_str_int(Mem) ->
    list_to_integer(lists:reverse(Mem)).

-spec pow2x0(integer()) ->
    integer().
pow2x0(X) ->
    1 bsl (X * 10).

-spec ip(string()) ->
    inet:ip_address().
ip(Host) ->
    mg_utils:throw_if_error(inet:parse_address(Host)).

-spec utf_bin(string()) ->
    binary().
utf_bin(IDStr) ->
    unicode:characters_to_binary(IDStr, utf8).

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
