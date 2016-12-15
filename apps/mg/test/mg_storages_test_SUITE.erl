%%%
%%% Тесты всех возможных бэкендов хранилищ.
%%%
%%% TODO:
%%%  - тесты диапазонов
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
-export([memory_stress_test/1]).
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
        {group, memory},
        {group, riak  }
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory, [sequence], memory_tests()},
        {riak  , [sequence], riak_tests()}
    ].

-spec tests() ->
    [{group_name(), list(_), test_name()}].
memory_tests() ->
    [
        base_test,
        utils_test,
        memory_stress_test
    ].

riak_tests() ->
    [
        base_test,
        utils_test,
        riak_stress_test
    ]

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
    Args = <<"Args">>,
    AllEvents = {undefined, undefined, forward},

    undefined = mg_storage:get_machine(storage(C), namespace(C), ID),

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
-spec batch_create_machines(C, IDs, Args) ->
    Machine = mg_storage:create_machine(storage(C), namespace(C), ID, Args),
    #{
        status       := {created, Args},
        aux_state    := undefined,
        events_range := undefined
    } = Machine,

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
