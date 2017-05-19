%%%
%%% Тесты всех возможных бэкендов хранилищ.
%%%
%%% TODO:
%%%  - сделать проверку, что неймспейсы не пересекаются
%%%
-module(mg_storages_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% base group tests
-export([base_test               /1]).
-export([indexes_test            /1]).
-export([indexes_test_with_limits/1]).
-export([stress_test             /1]).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        {group, memory},
        {group, riak  }
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory, [], tests()},
        {riak  , [], tests()}
    ].

-spec tests() ->
    [{group_name(), list(_), test_name()}].
tests() ->
    [
        base_test,
        % incorrect_context_test,
        indexes_test,
        indexes_test_with_limits,
        stress_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({riakc_pb_socket, 'get_index_eq', '_'}, x),
    % dbg:tpl({riakc_pb_socket, 'get_index_range', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(Group, C) ->
    [{storage_type, Group} | C].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, _C) ->
    ok.

%%
%% base group tests
%%
-spec base_test(config()) ->
    _.
base_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"base_test_ns">>),
    _ = start_storage(Options),
    base_test(1, Options).

-spec base_test(mg:id(), mg_storage:options()) ->
    _.
base_test(ID, Options) ->
    Key = genlib:to_binary(ID),
    Value1 = #{<<"hello">> => <<"world">>},
    Value2 = [<<"hello">>, 1],

    undefined      = mg_storage:get   (Options, Key),
    Ctx1           = mg_storage:put   (Options, Key, undefined, Value1, []),
    {Ctx1, Value1} = mg_storage:get   (Options, Key),
    Ctx2           = mg_storage:put   (Options, Key, Ctx1, Value2, []),
    {Ctx2, Value2} = mg_storage:get   (Options, Key),
    ok             = mg_storage:delete(Options, Key, Ctx2),
    undefined      = mg_storage:get   (Options, Key),
    ok.

-spec indexes_test(config()) ->
    _.
indexes_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"base_test_ns">>),
    _ = start_storage(Options),

    K1  = <<"Key_24">>,
    I1  = {integer, <<"index1">>},
    IV1 = 1,

    K2  = <<"Key_42">>,
    I2  = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    {[], _} = mg_storage:search(Options, {I1, IV1}),
    {[], _} = mg_storage:search(Options, {I1, {IV1, IV2}}),
    {[], _} = mg_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx1 = mg_storage:put(Options, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),

    {[K1       ], _} = mg_storage:search(Options, {I1, IV1       }),
    {[{IV1, K1}], _} = mg_storage:search(Options, {I1, {IV1, IV2}}),
    {[K1       ], _} = mg_storage:search(Options, {I2, IV2       }),
    {[{IV2, K1}], _} = mg_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx2 = mg_storage:put(Options, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    {[K1                  ], _} = mg_storage:search(Options, {I1, IV1       }),
    {[{IV1, K1}, {IV2, K2}], _} = mg_storage:search(Options, {I1, {IV1, IV2}}),
    {[K1                  ], _} = mg_storage:search(Options, {I2, IV2       }),
    {[{IV1, K2}, {IV2, K1}], _} = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, K1, Ctx1),

    {[{IV2, K2}], _} = mg_storage:search(Options, {I1, {IV1, IV2}}),
    {[{IV1, K2}], _} = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, K2, Ctx2),

    {[], _} = mg_storage:search(Options, {I1, {IV1, IV2}}),
    {[], _} = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok.

-spec indexes_test_with_limits(config()) ->
    _.
indexes_test_with_limits(C) ->
    Options = storage_options(?config(storage_type, C), <<"base_test_ns">>),
    _ = start_storage(Options),

    K1  = <<"Key_24">>,
    I1  = {integer, <<"index1">>},
    IV1 = 1,

    K2  = <<"Key_42">>,
    I2  = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    Ctx1 = mg_storage:put(Options, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),
    Ctx2 = mg_storage:put(Options, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    {[{IV1, K1}], Cont1} = mg_storage:search(Options, {I1, {IV1, IV2}, 1, undefined}),
    {[{IV2, K2}], Cont2} = mg_storage:search(Options, {I1, {IV1, IV2}, 1, Cont1}),
    {[], undefined}      = mg_storage:search(Options, {I1, {IV1, IV2}, 1, Cont2}),

    {[{IV1, K2}, {IV2, K1}], undefined} = mg_storage:search(Options, {I2, {IV1, IV2}, inf, undefined}),

    ok = mg_storage:delete(Options, K1, Ctx1),
    ok = mg_storage:delete(Options, K2, Ctx2),

    ok.

-spec stress_test(_C) ->
    ok.
stress_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"stress_test_ns">>),
    _ = start_storage(Options),
    ProcessCount = 20,
    Processes = [stress_test_start_process(ID, ProcessCount, Options) || ID <- lists:seq(1, ProcessCount)],

    timer:sleep(5000),
    ok = stop_wait_all(Processes, shutdown, 5000),
    ok.

-spec stress_test_start_process(term(), pos_integer(), mg_storage:options()) ->
    pid().
stress_test_start_process(ID, ProcessCount, Options) ->
    erlang:spawn_link(fun() -> stress_test_process(ID, ProcessCount, 0, Options) end).

-spec stress_test_process(term(), pos_integer(), integer(), mg_storage:options()) ->
    no_return().
stress_test_process(ID, ProcessCount, RunCount, Options) ->
    % Добавляем смещение ID, чтобы не было пересечения ID машин
    ok = base_test(ID, Options),

    receive
        {stop, Reason} ->
            ct:print("Process: ~p. Number of runs: ~p", [self(), RunCount]),
            exit(Reason)
    after
        0 -> stress_test_process(ID + ProcessCount, ProcessCount, RunCount + 1, Options)
    end.


-spec stop_wait_all([pid()], _Reason, timeout()) ->
    ok.
stop_wait_all(Pids, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),

    lists:foreach(
        fun(Pid) -> send_stop(Pid, Reason) end,
        Pids
    ),

    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok      -> ok;
                timeout -> exit(stop_timeout)
            end
        end,
        Pids
    ),

    true = process_flag(trap_exit, OldTrap),
    ok.

-spec send_stop(pid(), _Reason) ->
    ok.
send_stop(Pid, Reason) ->
    Pid ! {stop, Reason},
    ok.

-spec stop_wait(pid(), _Reason, timeout()) ->
    ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    receive
        {'EXIT', Pid, Reason} -> ok
    after
        Timeout -> timeout
    end.

%%

-spec storage_options(atom(), binary()) -> config().
storage_options(Type, Namespace) ->
    Module =
        case Type of
            riak ->
                {mg_storage_riak, #{
                    host => "riakdb",
                    port => 8087,
                    pool => #{
                        init_count => 1,
                        max_count  => 10
                    }
                }};
            memory ->
                mg_storage_memory
        end,
    #{
        namespace => Namespace,
        module    => Module
    }.

-spec start_storage(mg_storage:options()) ->
    pid().
start_storage(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Options, storage)]
        )
    ).
