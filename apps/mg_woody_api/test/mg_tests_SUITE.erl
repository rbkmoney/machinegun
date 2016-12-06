%%%
%%% TODO сделать нормальный тест автомата, как вариант, через пропер
%%%
-module(mg_tests_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

%% base group tests
-export([namespace_not_found   /1]).
-export([machine_start         /1]).
-export([machine_already_exists/1]).
-export([machine_call_by_id    /1]).
-export([machine_id_not_found  /1]).
-export([machine_tag_not_found /1]).
-export([machine_call_by_tag   /1]).

%% repair group tests
-export([machine_processor_error    /1]).
-export([failed_machine_call        /1]).
-export([failed_machine_repair_error/1]).
-export([failed_machine_repair      /1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).
-export([event_sink_incorrect_event_id   /1]).
-export([event_sink_incorrect_sink_id    /1]).
-export([event_sink_lots_events_ordering /1]).

%% test_door group tests
-export([machine_test_door/1]).

-export([config_with_multiple_event_sinks/1]).

-define(NS(C), <<"mg_test_", (proplists:get_value(test_instance, C))/binary, "_ns">>).
-define(ID, <<"ID">>).
-define(Tag, <<"Tag">>).
-define(Ref, {tag, ?Tag}).
-define(ES_ID, <<"test_event_sink">>).


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
        {group, riak  },
        config_with_multiple_event_sinks
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory, [sequence], tests_groups()},
        {riak  , [sequence], tests_groups()}
    ].

-spec tests_groups() ->
    [{group_name(), list(_), test_name()}].
tests_groups() ->
    [
        % TODO проверить отмену таймера
        {base, [sequence], [
            namespace_not_found,
            machine_start,
            machine_already_exists,
            machine_call_by_id,
            machine_id_not_found,
            machine_tag_not_found,

            % machine_tag_action,
            machine_call_by_tag

            % machine_timer_action
            % machine_get_history_by,

            % machine_get_unexisted_event
            % machine_double_tagging

            % machine_timer_timeout_test
            % machine_timer_deadline_test

            % machine_negative_timeout
            % machine_negative_deadline
        ]},

        {repair, [sequence], [
            machine_start,
            machine_processor_error,
            failed_machine_call,
            failed_machine_repair_error,
            failed_machine_call,
            failed_machine_repair,
            machine_call_by_id
        ]},

        {event_sink, [sequence], [
            event_sink_get_empty_history,
            event_sink_get_not_empty_history,
            event_sink_get_last_event,
            % TODO event_not_found
            % event_sink_incorrect_event_id,
            event_sink_incorrect_sink_id,
            event_sink_lots_events_ordering
        ]}

        % {test_door, [sequence], [
        %     machine_test_door
        % ]}
    ].


%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine_event_sink, '_', '_'}, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(memory, C) ->
    [{storage, mg_storage_test} | C];
init_per_group(riak, C) ->
    [{storage, {mg_storage_riak, #{
        host => "riakdb",
        port => 8087,
        pool => #{
            init_count => 1,
            max_count  => 10
        }
    }}} | C];
init_per_group(TestGroup, C0) ->
    C = [{test_instance, erlang:atom_to_binary(TestGroup, utf8)} | C0],
    %% TODO сделать нормальную генерацию урлов
    Apps =
        genlib_app:start_application_with(lager, [
            {handlers, [{lager_common_test_backend, info}]},
            {async_threshold, undefined}
            % {error_logger_hwm, undefined}
        ])
        ++
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 1}])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(TestGroup, C))
    ,

    {ok, ProcessorPid} = mg_machine_test_door:start_link({{0, 0, 0, 0}, 8023, "/processor"}),
    true = erlang:unlink(ProcessorPid),

    [
        {apps              , Apps                             },
        {processor_pid     , ProcessorPid                     },
        {automaton_options , {"http://localhost:8022", ?NS(C)}},
        {event_sink_options,  "http://localhost:8022"         }
    |
        C
    ].

-spec mg_woody_api_config(atom(), config()) ->
    list().
mg_woody_api_config(event_sink, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS(C) => #{
                processor  => #{ url => <<"http://localhost:8023/processor">>, recv_timeout => 5000 },
                event_sink => ?ES_ID
            }
        }}
    ];
mg_woody_api_config(_, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS(C) => #{
                processor => #{ url => <<"http://localhost:8023/processor">> }
            }
        }}
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(memory, _) ->
    ok;
end_per_group(riak, _) ->
    ok;
end_per_group(_, C) ->
    true = erlang:exit(?config(processor_pid, C), kill),
    [application_stop(App) || App <- proplists:get_value(apps, C)].

-spec application_stop(atom()) ->
    _.
application_stop(App=sasl) ->
    %% hack for preventing sasl deadlock
    %% http://erlang.org/pipermail/erlang-questions/2014-May/079012.html
    error_logger:delete_report_handler(cth_log_redirect),
    application:stop(App),
    error_logger:add_report_handler(cth_log_redirect),
    ok;
application_stop(App) ->
    application:stop(App).

%%
%% base group tests
%%
-spec namespace_not_found(config()) -> _.
namespace_not_found(C) ->
    {URL, _NS} = a_opts(C),
    #'NamespaceNotFound'{} = (catch mg_machine_test_door:start({URL, <<"incorrect_NS">>}, ?ID, ?Tag)).

-spec machine_start(config()) -> _.
machine_start(C) ->
    ok = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag).

-spec machine_already_exists(config()) -> _.
machine_already_exists(C) ->
    #'MachineAlreadyExists'{} = (catch mg_machine_test_door:start(a_opts(C), ?ID, <<"another_Tag">>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    ok = mg_machine_test_door:do_action(a_opts(C), touch, {id, ?ID}).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
    #'MachineNotFound'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, {id, <<"incorrect_ID">>})).


-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(C) ->
    ok = mg_machine_test_door:do_action(a_opts(C), touch, {tag, ?Tag}).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(C) ->
    #'MachineNotFound'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, {tag, <<"incorrect_Tag">>})).

%% get history к несущестующему эвенту
%% двойное тэгирование

%%
%% repair group tests
%%
%% падение машины
-spec machine_processor_error(config()) ->
    _.
machine_processor_error(C) ->
    #'MachineFailed'{} = (catch mg_machine_test_door:do_action(a_opts(C), fail, {id, ?ID})).

-spec failed_machine_call(config()) ->
    _.
failed_machine_call(C) ->
    #'MachineFailed'{} = (catch mg_machine_test_door:do_action(a_opts(C), touch, {id, ?ID})).

-spec failed_machine_repair_error(config()) ->
    _.
failed_machine_repair_error(C) ->
    #'MachineFailed'{} = (catch mg_machine_test_door:repair(a_opts(C), {id, ?ID}, error)).

-spec failed_machine_repair(config()) ->
    _.
failed_machine_repair(C) ->
    ok = (catch mg_machine_test_door:repair(a_opts(C), {id, ?ID}, ok)).

%%
%% event_sink group test
%%
-spec event_sink_get_empty_history(config()) ->
    _.
event_sink_get_empty_history(C) ->
    [] = mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=forward}).

-spec event_sink_get_not_empty_history(config()) ->
    _.
event_sink_get_not_empty_history(C) ->
    _ID = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag),
    ok = test_door_do_action(C, close, {id, ?ID}),
    ok = test_door_do_action(C, open , {id, ?ID}),
    NS = ?NS(C),
    [
        #'SinkEvent'{id = 1, source_id = ?ID, source_ns = NS, event = #'Event'{}},
        #'SinkEvent'{id = 2, source_id = ?ID, source_ns = NS, event = #'Event'{}},
        #'SinkEvent'{id = 3, source_id = ?ID, source_ns = NS, event = #'Event'{}}
    ] = mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=forward}).

-spec event_sink_get_last_event(config()) ->
    _.
event_sink_get_last_event(C) ->
    NS = ?NS(C),
    [#'SinkEvent'{id = 3, source_id = ?ID, source_ns = NS, event = #'Event'{}}] =
        mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=backward, limit=1}).

-spec event_sink_incorrect_event_id(config()) ->
    _.
event_sink_incorrect_event_id(C) ->
    #'EventNotFound'{}
        = (catch mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{'after'=42})).


-spec event_sink_incorrect_sink_id(config()) ->
    _.
event_sink_incorrect_sink_id(C) ->
    #'EventSinkNotFound'{}
        = (catch mg_event_sink_client:get_history(es_opts(C), <<"incorrect_event_sink_id">>, #'HistoryRange'{})).

-spec event_sink_lots_events_ordering(config()) ->
    _.
event_sink_lots_events_ordering(C) ->
    [#'SinkEvent'{id = LastEventID}] =
        mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=backward, limit=1}),
    % N = 35,
    N = 20,
    _ = lists:foreach(
            fun(_) ->
                ok = test_door_do_action(C, close, {id, ?ID}),
                ok = test_door_do_action(C, open , {id, ?ID})
            end,
            lists:seq(1, N)
        ),

    Events = mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=forward}),
    EventsIDs = lists:seq(1, N * 2 + LastEventID),
    EventsIDs = [ID || #'SinkEvent'{id=ID} <- Events].


%%
%% test_door group tests
%%
-spec machine_test_door(config()) ->
    _.
machine_test_door(C) ->
    % запустить автомат
    _ID = mg_machine_test_door:start(a_opts(C), ?ID, ?Tag),
    CS0 = #{last_event_id => undefined, state => undefined},
    CS1 = #{state:=open} = test_door_update_state(C, CS0),

    % прогнать по стейтам
    ok = test_door_do_action(C, close),
    CS2 = #{state:=closed} = test_door_update_state(C, CS1),

    ok = test_door_do_action(C, open),
    CS3 = #{state:=open} = test_door_update_state(C, CS2),
    ok = test_door_do_action(C, close),

    CS4 = #{state:=closed} = test_door_update_state(C, CS3),
    % ждем, что таймер не сработает
    ok = timer:sleep(3000),
    CS5 = #{state:=closed} = test_door_update_state(C, CS4),
    ok = test_door_do_action(C, open),
    CS6 = #{state:=open} = test_door_update_state(C, CS5),
    % ждем, что таймер сработает
    ok = timer:sleep(3000),
    CS7 = #{state:=closed} = test_door_update_state(C, CS6),
    ok = test_door_do_action(C, {lock, <<"123">>}),
    {error, bad_passwd} = test_door_do_action(C, {unlock, <<"12">>}),
    ok = test_door_do_action(C, {unlock, <<"123">>}),
    _CS8 = #{state:=closed} = test_door_update_state(C, CS7).

-spec config_with_multiple_event_sinks(config()) ->
    _.
config_with_multiple_event_sinks(_C) ->
    Config = [
        {storage, mg_storage_test},
        {namespaces, #{
            <<"1">> => #{
                processor  => #{ url => <<"http://localhost:8023/processor">> },
                event_sink => <<"SingleES">>
            },
            <<"2">> => #{
                processor  => #{ url => <<"http://localhost:8023/processor">> },
                event_sink => <<"SingleES">>
            }
        }}
    ],

    Apps = genlib_app:start_application_with(mg_woody_api, Config),
    [application_stop(App) || App <- Apps].

%%
%% utils
%%
-spec test_door_update_state(config(), mg_machine_test_door:client_state()) ->
    mg_machine_test_door:client_state().
test_door_update_state(C, CS) ->
    mg_machine_test_door:update_state(a_opts(C), ?Ref, CS).

-spec test_door_do_action(config(), mg_machine_test_door:action()) ->
    _.
test_door_do_action(C, Action) ->
    mg_machine_test_door:do_action(a_opts(C), Action, ?Ref).

-spec test_door_do_action(config(), mg_machine_test_door:action(), mg:ref()) ->
    _.
test_door_do_action(C, Action, Ref) ->
    mg_machine_test_door:do_action(a_opts(C), Action, Ref).

-spec a_opts(config()) -> _.
a_opts(C) -> ?config(automaton_options, C).
-spec es_opts(config()) -> _.
es_opts(C) -> ?config(event_sink_options, C).


