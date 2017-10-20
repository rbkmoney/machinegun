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
        {group, memory   },
        {group, riak     },
        {group, riak_pool}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory   , [], tests()},
        {riak     , [], tests()},
        {riak_pool, [], tests()}
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
    Options = storage_options(?config(storage_type, C), <<"base_test">>),
    _ = start_storage(Options),
    base_test(1, Options).

-spec base_test(mg:id(), mg_storage:options()) ->
    _.
base_test(ID, Options) ->
    Key = genlib:to_binary(ID),
    Value1 = #{<<"hello">> => <<"world">>},
    Value2 = [<<"hello">>, 1],

    undefined      = mg_storage:get   (Options, storage, Key),
    Ctx1           = mg_storage:put   (Options, storage, Key, undefined, Value1, []),
    {Ctx1, Value1} = mg_storage:get   (Options, storage, Key),
    Ctx2           = mg_storage:put   (Options, storage, Key, Ctx1, Value2, []),
    {Ctx2, Value2} = mg_storage:get   (Options, storage, Key),
    ok             = mg_storage:delete(Options, storage, Key, Ctx2),
    undefined      = mg_storage:get   (Options, storage, Key),
    ok.

-spec indexes_test(config()) ->
    _.
indexes_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"indexes_test">>),
    _ = start_storage(Options),

    K1  = <<"Key_24">>,
    I1  = {integer, <<"index1">>},
    IV1 = 1,

    K2  = <<"Key_42">>,
    I2  = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    [] = mg_storage:search(Options, storage, {I1, IV1}),
    [] = mg_storage:search(Options, storage, {I1, {IV1, IV2}}),
    [] = mg_storage:search(Options, storage, {I2, {IV1, IV2}}),

    Ctx1 = mg_storage:put(Options, storage, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),

    [K1       ] = mg_storage:search(Options, storage, {I1, IV1       }),
    [{IV1, K1}] = mg_storage:search(Options, storage, {I1, {IV1, IV2}}),
    [K1       ] = mg_storage:search(Options, storage, {I2, IV2       }),
    [{IV2, K1}] = mg_storage:search(Options, storage, {I2, {IV1, IV2}}),

    Ctx2 = mg_storage:put(Options, storage, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    [K1                  ] = mg_storage:search(Options, storage, {I1, IV1       }),
    [{IV1, K1}, {IV2, K2}] = mg_storage:search(Options, storage, {I1, {IV1, IV2}}),
    [K1                  ] = mg_storage:search(Options, storage, {I2, IV2       }),
    [{IV1, K2}, {IV2, K1}] = mg_storage:search(Options, storage, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, storage, K1, Ctx1),

    [{IV2, K2}] = mg_storage:search(Options, storage, {I1, {IV1, IV2}}),
    [{IV1, K2}] = mg_storage:search(Options, storage, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, storage, K2, Ctx2),

    [] = mg_storage:search(Options, storage, {I1, {IV1, IV2}}),
    [] = mg_storage:search(Options, storage, {I2, {IV1, IV2}}),

    ok.

-spec indexes_test_with_limits(config()) ->
    _.
indexes_test_with_limits(C) ->
    Options = storage_options(?config(storage_type, C), <<"indexes_test_with_limits">>),
    _ = start_storage(Options),

    K1  = <<"Key_24">>,
    I1  = {integer, <<"index1">>},
    IV1 = 1,

    K2  = <<"Key_42">>,
    I2  = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    Ctx1 = mg_storage:put(Options, storage, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),
    Ctx2 = mg_storage:put(Options, storage, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    {[{IV1, K1}], Cont1} = mg_storage:search(Options, storage, {I1, {IV1, IV2}, 1, undefined}),
    {[{IV2, K2}], Cont2} = mg_storage:search(Options, storage, {I1, {IV1, IV2}, 1, Cont1}),
    {[], undefined}      = mg_storage:search(Options, storage, {I1, {IV1, IV2}, 1, Cont2}),

    [{IV1, K2}, {IV2, K1}] = mg_storage:search(Options, storage, {I2, {IV1, IV2}, inf, undefined}),

    ok = mg_storage:delete(Options, storage, K1, Ctx1),
    ok = mg_storage:delete(Options, storage, K2, Ctx2),

    ok.

-spec stress_test(_C) ->
    ok.
stress_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"stress_test">>),
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

-spec storage_options(atom(), binary()) ->
    config().
storage_options(riak, Namespace) ->
    {mg_storage_riak, #{
        host   => "riakdb",
        port   => 8087,
        bucket => Namespace
    }};
storage_options(memory, _) ->
    mg_storage_memory;
storage_options(riak_pool, Namespace) ->
    {
        mg_storage_pool,
        #{
            worker          => storage_options(riak, mg_utils:concatenate_namespaces(Namespace, <<"pool">>)),
            size            => 10,
            queue_len_limit => 100,
            retry_attempts  => 10
        }
    }.

-spec start_storage(mg_storage:options()) ->
    pid().
start_storage(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Options, storage, {local, storage})]
        )
    ).
