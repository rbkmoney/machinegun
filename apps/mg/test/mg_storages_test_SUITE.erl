%%%
%%% Тесты всех возможных бэкендов хранилищ.
%%%
%%% TODO:
%%%  - множество ns
%%%  - множество машин
%%%  - простой нагрузочный
%%%
%%% После выполнения задач выше, можно будет вырезать тесты риака из wg_woody_api тестов
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
-export([base_test/1]).
-export([utils_test/1]).
-export([riak_stress_test/1]).

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
        {group, base},
        {group, riak}
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {base, [sequence], base_tests()},
        {riak, [sequence], riak_tests()}
    ].

-spec base_tests() ->
    [{group_name(), list(_), test_name()}].
base_tests() ->
    [
        base_test,
        utils_test
    ].

-spec riak_tests() ->
    [{group_name(), list(_), test_name()}].
riak_tests() ->
    [
        riak_stress_test
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
init_per_group(base, C) ->
    start_storage(mg_storage_test, <<"ns">>, C);
init_per_group(memory, C) ->
    start_storage(mg_storage_test, <<"ns">>, C);
init_per_group(riak, C) ->
    start_storage(
        {mg_storage_riak, #{
            host => "riakdb",
            port => 8087,
            pool => #{
                init_count => 1,
                max_count  => 10
            }
        }},
        <<"ns">>,
        C
    ).

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, C) ->
    true = erlang:exit(?config(storage_pid, C), kill).

-spec start_storage(mg_storage:storage(), mg:ns(), config()) ->
    config().
start_storage(Storage, Namespace, C) ->
    {ok, Pid} =
        mg_utils_supervisor_wrapper:start_link(
            #{strategy => one_for_all},
            [mg_storage:child_spec(Storage, Namespace, storage)]
        ),
    true = unlink(Pid),
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
-spec base_test(config()) ->
    _.
base_test(C) ->
    ID = <<"42">>,
    AllEvents = {undefined, undefined, forward},

    % create
    Machine = mg_storage:create_machine(storage(C), namespace(C), ID, Args),
    #{
        status       := {created, Args},
        aux_state    := undefined,
        events_range := undefined
    } = Machine,

    % get
    Machine = mg_storage:get_machine(storage(C), namespace(C), ID),
    []      = mg_storage:get_history(storage(C), namespace(C), ID, Machine, AllEvents),

    AuxState = <<"AuxState">>,
    EventsCount = 100,
    Events = [make_event(EventID) || EventID <- lists:seq(1, EventsCount)],

    Update =
        #{
            status     => working,
            aux_state  => AuxState,
            new_events => Events
        },
    NewMachine = mg_storage:update_machine(storage(C), namespace(C), ID, Machine, Update),
    #{
        status       := working,
        aux_state    := AuxState,
        events_range := {1, EventsCount}
    } = NewMachine,

    NewMachine = mg_storage:get_machine(storage(C), namespace(C), ID),
    Events     = mg_storage:get_history(storage(C), namespace(C), ID, NewMachine, AllEvents),

    ok.

-spec base_riak_test(binary(), binary(), binary()) -> term().
base_riak_test(Storage, Namespace, ID) ->
    Args = <<"Args">>,
    AllEvents = {undefined, undefined, forward},

    % create
    Machine = mg_storage_riak:create_machine(Storage, Namespace, ID, Args),
    #{
        status       := {created, Args},
        aux_state    := undefined,
        events_range := undefined
    } = Machine,

    % get
    Machine = mg_storage_riak:get_machine(Storage, Namespace, ID),
    []      = mg_storage_riak:get_history(Storage, Namespace, ID, Machine, AllEvents),

    AuxState = <<"AuxState">>,
    EventsCount = 100,
    Events = [make_event(EventID) || EventID <- lists:seq(1, EventsCount)],

    Update =
        #{
            status     => working,
            aux_state  => AuxState,
            new_events => Events
        },
    NewMachine = mg_storage_riak:update_machine(Storage, Namespace, ID, Machine, Update),
    #{
        status       := working,
        aux_state    := AuxState,
        events_range := {1, EventsCount}
    } = NewMachine,

    NewMachine = mg_storage_riak:get_machine(Storage, Namespace, ID),
    Events     = mg_storage_riak:get_history(Storage, Namespace, ID, NewMachine, AllEvents),

    ok.

-spec riak_stress_test(_C) -> term().
riak_stress_test(C) ->
    ProcessCount = 50,
    Processes = [stress_test_start_process(C) || _ <- lists:seq(1, ProcessCount)],
    ok = stop_wait_all(Processes, shutdown, 1000).

-spec stress_test_start_process(config()) ->
    pid().
stress_test_start_process(Options) ->
    erlang:spawn_link(fun() -> stress_test_process(Options) end).

-spec stress_test_process(config()) ->
    no_return().
stress_test_process(Options) ->
    ID = genlib:to_binary(rand:uniform(10)),
    Namespace = genlib:to_binary(rand:uniform(10)),
    Storage = storage(Options),
    ok = base_riak_test(Storage, Namespace, ID),
    stress_test_process(Options).


-spec stop_wait_all([pid()], _Reason, timeout()) ->
    ok.
stop_wait_all(Pids, Reason, Timeout) ->
    lists:foreach(
        fun(Pid) ->
            case stop_wait(Pid, Reason, Timeout) of
                ok      -> ok;
                timeout -> exit(stop_timeout)
            end
        end,
        Pids
    ).

-spec stop_wait(pid(), _Reason, timeout()) ->
    ok | timeout.
stop_wait(Pid, Reason, Timeout) ->
    OldTrap = process_flag(trap_exit, true),
    erlang:exit(Pid, Reason),
    R =
        receive
            {'EXIT', Pid, Reason} -> ok
        after
            Timeout -> timeout
        end,
    process_flag(trap_exit, OldTrap),
    R.

%%
%% utils_test
%%
-spec utils_test(config()) ->
    _.
utils_test(C) ->
    ID = <<"42">>,
    Args = <<"Args">>,

    % create
    Machine = mg_storage:create_machine(storage(C), namespace(C), ID, Args),
    #{
        status       := {created, Args},
        aux_state    := undefined,
        events_range := undefined
    } = Machine,

    EventsCount = 100,
    Events = [make_event(EventID) || EventID <- lists:seq(1, EventsCount)],
    AuxState = <<"AuxState">>,

    Update =
        #{
            status     => working,
            aux_state  => AuxState,
            new_events => Events
        },
    NewMachine = mg_storage:update_machine(storage(C), namespace(C), ID, Machine, Update),

    [
        {<<"42">>, 6},
        {<<"42">>, 7},
        {<<"42">>, 8}
    ] = mg_storage_utils:get_machine_events_ids(ID, NewMachine, {5, 3, forward}),

    [
        {<<"42">>, 4},
        {<<"42">>, 3},
        {<<"42">>, 2}
    ] = mg_storage_utils:get_machine_events_ids(ID, NewMachine, {5, 3, backward}).

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
