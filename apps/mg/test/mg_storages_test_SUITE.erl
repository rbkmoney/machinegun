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
-export([base_test/1]).
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
    {{mg_storage_riak, #{
        host => "riakdb",
        port => 8087,
        pool => #{
            init_count => 1,
            max_count  => 10
        }
    }},
    Namespace};
make_storage(base, Namespace) ->
    {mg_storage_test, Namespace}.

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
-spec base_test(config()) ->
    _.
base_test(C) ->
    ID = <<"42">>,
    Args = <<"Args">>,
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

    [
        {<<"42">>, 6},
        {<<"42">>, 7},
        {<<"42">>, 8}
    ] = mg_storage_utils:get_machine_events_ids(ID, NewMachine, {5, 3, forward}),

    [
        {<<"42">>, 4},
        {<<"42">>, 3},
        {<<"42">>, 2}
    ] = mg_storage_utils:get_machine_events_ids(ID, NewMachine, {5, 3, backward}),

    ok.

-spec stress_test(_C) -> term().
stress_test(C0) ->
    C = start_storage(C0),
    ProcessCount = 5,
    Processes = [stress_test_start_process(C) || _ <- lists:seq(1, ProcessCount)],
    ok = stop_wait_all(Processes, shutdown, 5000).

-spec stress_test_start_process(config()) ->
    pid().
stress_test_start_process(C) ->
    erlang:spawn_link(fun() -> stress_test_process(C) end).

-spec stress_test_process(config()) ->
    no_return().
stress_test_process(C) ->
    ok = base_test(C),
    stress_test_process(C).

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
    true = erlang:exit(Pid, Reason),
    R =
        receive
            {'EXIT', Pid, Reason} -> ok
        after
            Timeout -> timeout
        end,
    true = process_flag(trap_exit, OldTrap),
    R.

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
