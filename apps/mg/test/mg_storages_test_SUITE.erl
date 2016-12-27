%%%
%%% Тесты всех возможных бэкендов хранилищ.
%%%
%%% TODO вырезать тесты риака из wg_woody_api тестов
%%%

-module(mg_storages_test_SUITE).
-include_lib("common_test/include/ct.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% base group tests
-export([range_direction_test/1]).
-export([range_border_test/1]).
-export([range_missing_params_test/1]).
-export([range_no_intersection_test/1]).
-export([stress_test/1]).

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
        {group, riak},
        {group, range}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory,  [sequence], memory_tests()},
        {riak,  [sequence], riak_tests()},
        {range, [sequence], range_tests()}
    ].

-spec range_tests() ->
    [{group_name(), list(_), test_name()}].
range_tests() ->
    [
        range_direction_test,
        % range_no_intersection_test,
        range_border_test,
        range_missing_params_test
    ].

-spec memory_tests() ->
    [{group_name(), list(_), test_name()}].
memory_tests() ->
    [
        stress_test
    ].

-spec riak_tests() ->
    [{group_name(), list(_), test_name()}].
riak_tests() ->
    [
        stress_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine_event_sink, '_', '_'}, x),
    Apps = genlib_app:start_application(mg),
    [{apps, Apps} | C].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    [application:stop(App) || App <- proplists:get_value(apps, C)].

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(Group, C) ->
    [{storage, Group} | C].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, _C) ->
    ok.

-spec make_storage(config()) -> config().
make_storage(C) ->
    Group = ?config(storage, C),
    Namespace = <<"ns">>,
    make_storage(Group, Namespace).

-spec make_storage(atom(), binary()) -> config().
make_storage(riak, Namespace) ->
    {
        {mg_storage_riak, #{
            host => "riakdb",
            port => 8087,
            pool => #{
                init_count => 1,
                max_count  => 10
            }
        }},
        Namespace
    };
make_storage(memory, Namespace) ->
    {mg_storage_memory, Namespace}.

-spec start_storage(config()) ->
    config().
start_storage(C) ->
    {Storage, Namespace} = make_storage(C),
    {ok, Pid} =
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Storage, Namespace, storage)]
        ),
    [
        {storage_pid, Pid      },
        {storage    , Storage  },
        {namespace  , Namespace}
    |
        C
    ].

%%
%% base group tests
%%
-spec base_test(term(), config()) ->
    _.
base_test(ID0, C) ->
    ID = genlib:to_binary(ID0),
    AllEvents = {undefined, undefined, forward},

    Machine = #{
        status       => working,
        aux_state    => undefined,
        events_range => undefined,
        db_state     => undefined
    },

    % update
    AuxState = <<"AuxState">>,
    EventsCount = 20,
    Events = [make_event(EventID) || EventID <- lists:seq(1, EventsCount)],

    Update =
        #{
            status     => working,
            aux_state  => AuxState,
            new_events => Events
        },
    NewMachine = mg_storage:update_machine(storage(C), namespace(C), ID, Machine, Update),

    NewMachine = mg_storage:get_machine(storage(C), namespace(C), ID),
    Events     = mg_storage:get_history(storage(C), namespace(C), ID, NewMachine, AllEvents),

    ok.

-spec stress_test(_C) ->
    ok.
stress_test(C0) ->
    C = start_storage(C0),
    ProcessCount = 20,
    Processes = [stress_test_start_process(ID, ProcessCount, C) || ID <- lists:seq(1, ProcessCount)],

    timer:sleep(5000),
    ok = stop_wait_all(Processes, shutdown, 5000),
    ok.

-spec stress_test_start_process(term(), pos_integer(), config()) ->
    pid().
stress_test_start_process(ID, ProcessCount, C) ->
    erlang:spawn_link(fun() -> stress_test_process(ID, ProcessCount, 0, C) end).

-spec stress_test_process(term(), pos_integer(), integer(), config()) ->
    no_return().
stress_test_process(ID, ProcessCount, RunCount, C) ->
    % Добавляем смещение ID, чтобы не было пересечения ID машин
    ok = base_test(ID, C),

    receive
        {stop, Reason} ->
            ct:print("Process: ~p. Number of runs: ~p", [self(), RunCount]),
            exit(Reason)
    after
        0 -> stress_test_process(ID + ProcessCount, ProcessCount, RunCount + 1, C)
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

-spec range_direction_test(_C) ->
    ok.
range_direction_test(_C) ->
    ID = <<"42">>,
    Machine = #{
        status       => working,
        aux_state    => undefined,
        events_range => {1, 100}
    },

    [
        {ID, 4},
        {ID, 3},
        {ID, 2}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {5, 3, backward}),

    [
        {ID, 5},
        {ID, 6},
        {ID, 7},
        {ID, 8}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {4, 4, forward}),

    ok.

-spec range_border_test(_C) ->
    ok.
range_border_test(_C) ->
    ID = <<"42">>,
    Machine = #{
        status       => working,
        aux_state    => undefined,
        events_range => {1, 8}
    },

    [
        {ID, 1},
        {ID, 2}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {undefined, 2, forward}),

    [
        {ID, 8},
        {ID, 7}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {undefined, 2, backward}),

    [
        {ID, 6},
        {ID, 7},
        {ID, 8}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {5, 5, forward}),
    ok.

-spec range_missing_params_test(_C) ->
    ok.
range_missing_params_test(_C) ->
    ID = <<"42">>,
    Machine = #{
        status       => working,
        aux_state    => undefined,
        events_range => {1, 8}
    },

    [
        {ID, 1},
        {ID, 2},
        {ID, 3}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {undefined, 3, forward}),

    [
        {ID, 7},
        {ID, 8}
    ] = mg_storage_utils:get_machine_events_ids(ID, Machine, {6, undefined, forward}),

    ok.

-spec range_no_intersection_test(_C) ->
    ok.
range_no_intersection_test(_C) ->
    ID = <<"42">>,
    Machine = #{
        status       => working,
        aux_state    => undefined,
        events_range => {5, 10}
    },

    event_not_found = (catch mg_storage_utils:get_machine_events_ids(ID, Machine, {1, 3, forward})),

    ok.

%%
%% helpers
%%
-spec make_event(mg:event_id()) ->
    mg:event().
make_event(ID) ->
    #{
        id         => ID,
        created_at => erlang:system_time(),
        body       => <<(integer_to_binary(ID))/binary>>
    }.

-spec storage(config()) ->
    mg_storage:storage().
storage(C) ->
    ?config(storage, C).

-spec namespace(config()) ->
    mg:ns().
namespace(C) ->
    ?config(namespace, C).
