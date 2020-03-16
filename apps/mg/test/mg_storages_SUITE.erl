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
-export([key_length_limit_test    /1]).
-export([indexes_test_with_limits/1]).
-export([stress_test             /1]).

-export([handle_beat/2]).
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
        {group, riak     }
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory   , [], tests()},
        {riak     , [], tests()}
    ].

-spec tests() ->
    [{group_name(), list(_), test_name()}].
tests() ->
    [
        base_test,
        % incorrect_context_test,
        indexes_test,
        key_length_limit_test,
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
    Apps = mg_ct_helper:start_applications([msgpack, gproc, riakc, pooler]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    mg_ct_helper:stop_applications(?config(apps, C)).

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
    Pid = start_storage(Options),
    base_test(1, Options),
    ok = stop_storage(Pid).

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
    Options = storage_options(?config(storage_type, C), <<"indexes_test">>),
    Pid = start_storage(Options),

    K1  = <<"Key_24">>,
    I1  = {integer, <<"index1">>},
    IV1 = 1,

    K2  = <<"Key_42">>,
    I2  = {integer, <<"index2">>},
    IV2 = 2,

    Value = #{<<"hello">> => <<"world">>},

    [] = mg_storage:search(Options, {I1, IV1}),
    [] = mg_storage:search(Options, {I1, {IV1, IV2}}),
    [] = mg_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx1 = mg_storage:put(Options, K1, undefined, Value, [{I1, IV1}, {I2, IV2}]),

    [K1       ] = mg_storage:search(Options, {I1, IV1       }),
    [{IV1, K1}] = mg_storage:search(Options, {I1, {IV1, IV2}}),
    [K1       ] = mg_storage:search(Options, {I2, IV2       }),
    [{IV2, K1}] = mg_storage:search(Options, {I2, {IV1, IV2}}),

    Ctx2 = mg_storage:put(Options, K2, undefined, Value, [{I1, IV2}, {I2, IV1}]),

    [K1                  ] = mg_storage:search(Options, {I1, IV1       }),
    [{IV1, K1}, {IV2, K2}] = mg_storage:search(Options, {I1, {IV1, IV2}}),
    [K1                  ] = mg_storage:search(Options, {I2, IV2       }),
    [{IV1, K2}, {IV2, K1}] = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, K1, Ctx1),

    [{IV2, K2}] = mg_storage:search(Options, {I1, {IV1, IV2}}),
    [{IV1, K2}] = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok = mg_storage:delete(Options, K2, Ctx2),

    [] = mg_storage:search(Options, {I1, {IV1, IV2}}),
    [] = mg_storage:search(Options, {I2, {IV1, IV2}}),

    ok = stop_storage(Pid).

-spec key_length_limit_test(config()) ->
    _.
key_length_limit_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"key_length_limit">>),
    Pid = start_storage(Options),

    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_storage:get(Options, <<"">>)),

    {logic, {invalid_key, {too_small, _}}} =
        (catch mg_storage:put(Options, <<"">>, undefined, <<"test">>, [])),

    _ = mg_storage:get(Options, binary:copy(<<"K">>, 1024)),

    {logic, {invalid_key, {too_big, _}}} =
        (catch
            mg_storage:get(Options, binary:copy(<<"K">>, 1025))
        ),

    _ = mg_storage:put(
        Options,
        binary:copy(<<"K">>, 1024),
        undefined,
        <<"test">>,
        []
    ),

    {logic, {invalid_key, {too_big, _}}} =
        (catch
            mg_storage:put(
                Options,
                binary:copy(<<"K">>, 1025),
                undefined,
                <<"test">>,
                []
            )
        ),

    ok = stop_storage(Pid).

-spec indexes_test_with_limits(config()) ->
    _.
indexes_test_with_limits(C) ->
    Options = storage_options(?config(storage_type, C), <<"indexes_test_with_limits">>),
    Pid = start_storage(Options),

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

    [{IV1, K2}, {IV2, K1}] = mg_storage:search(Options, {I2, {IV1, IV2}, inf, undefined}),

    ok = mg_storage:delete(Options, K1, Ctx1),
    ok = mg_storage:delete(Options, K2, Ctx2),

    ok = stop_storage(Pid).

-spec stress_test(_C) ->
    ok.
stress_test(C) ->
    Options = storage_options(?config(storage_type, C), <<"stress_test">>),
    Pid = start_storage(Options),
    ProcessCount = 20,
    Processes = [stress_test_start_process(ID, ProcessCount, Options) || ID <- lists:seq(1, ProcessCount)],

    timer:sleep(5000),
    ok = stop_wait_all(Processes, shutdown, 5000),
    ok = stop_storage(Pid).

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
            % ct:print("Process: ~p. Number of runs: ~p", [self(), RunCount]),
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
        name         => storage,
        pulse        => ?MODULE,
        host         => "riakdb",
        port         => 8087,
        bucket       => Namespace,
        namespace    => <<"some_ns_riak">>,
        pool_options => #{
            init_count          => 1,
            max_count           => 10,
            idle_timeout        => 1000,
            cull_interval       => 1000,
            auto_grow_threshold => 5,
            queue_max           => 100,
            metrics_mod         => pooler_no_metrics,
            metrics_api         => folsom
        }
    }};
storage_options(memory, _) ->
    {mg_storage_memory, #{
        namespace => <<"some_ns_memory">>,
        pulse => ?MODULE,
        name => storage
    }}.

-spec start_storage(mg_storage:options()) ->
    pid().
start_storage(Options) ->
    mg_utils:throw_if_error(
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Options, storage)]
        )
    ).

-spec stop_storage(pid()) ->
    ok.
stop_storage(Pid) ->
    ok = proc_lib:stop(Pid, normal, 5000),
    ok.

-spec handle_beat(_, mg_pulse:beat()) ->
    ok.
handle_beat(_, Beat) ->
    ct:pal("~p", [Beat]).
