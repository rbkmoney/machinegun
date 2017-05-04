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
-export([namespace_not_found    /1]).
-export([machine_start          /1]).
-export([machine_already_exists /1]).
-export([machine_call_by_id     /1]).
-export([machine_id_not_found   /1]).
-export([machine_set_tag        /1]).
-export([machine_call_by_tag    /1]).
-export([machine_tag_not_found  /1]).
-export([machine_remove         /1]).

%% repair group tests
-export([failed_machine_start        /1]).
-export([machine_processor_error     /1]).
-export([failed_machine_call         /1]).
-export([failed_machine_repair_error /1]).
-export([failed_machine_repair       /1]).

%% timer group tests
-export([handle_timer/1]).
-export([abort_timer /1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).
-export([event_sink_incorrect_event_id   /1]).
-export([event_sink_incorrect_sink_id    /1]).
-export([event_sink_lots_events_ordering /1]).

-export([config_with_multiple_event_sinks/1]).

-define(NS, <<"NS">>).
-define(ID, <<"ID">>).
-define(Tag, <<"tag">>).
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
        config_with_multiple_event_sinks
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        {memory, [sequence], tests_groups()}
    ].

-spec tests_groups() ->
    [{group_name(), list(_), test_name()}].
tests_groups() ->
    [
        % TODO проверить отмену таймера
        % TODO проверить отдельно get_history
        {base, [sequence], [
            namespace_not_found,
            machine_id_not_found,
            machine_start,
            machine_already_exists,
            machine_id_not_found,
            machine_call_by_id,
            machine_set_tag,
            machine_tag_not_found,
            machine_call_by_tag,
            machine_remove,
            machine_id_not_found
        ]},

        {repair, [sequence], [
            failed_machine_start,
            machine_id_not_found,
            machine_start,
            machine_processor_error,
            failed_machine_call,
            failed_machine_repair_error,
            failed_machine_repair
        ]},

        {timers, [sequence], [
            machine_start,
            handle_timer
            % handle_timer % был прецендент, что таймер срабатывал только один раз
            % abort_timer
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
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_woody_api_processor, '_', '_'}, x),
    % dbg:tpl({mg_machine_event_sink, '_', '_'}, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(memory, C) ->
    [{storage, mg_storage_memory} | C];
init_per_group(TestGroup, C0) ->
    C = [{test_instance, erlang:atom_to_binary(TestGroup, utf8)} | C0],
    %% TODO сделать нормальную генерацию урлов
    Apps =
        genlib_app:start_application_with(lager, [
            {handlers, [{lager_common_test_backend, info}]},
            {async_threshold, undefined}
        ])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(TestGroup, C))
    ,

    SetTimer = {set_timer, {timeout, 1}, {undefined, undefined, forward}, 30},

    CallFunc =
        fun({Args, _Machine}) ->
            case Args of
                <<"tag">>   -> {Args, {<<>>, [<<"tag_body"  >>]}, #{timer =>  undefined  , tag => Args     }};
                <<"event">> -> {Args, {<<>>, [<<"event_body">>]}, #{timer =>  undefined  , tag => undefined}};
                <<"nop"  >> -> {Args, {<<>>, [                ]}, #{timer =>  undefined  , tag => undefined}};
                <<"set_timer"  >> -> {Args, {<<>>, [<<"timer_body">>]}, #{timer => SetTimer   , tag => undefined}};
                <<"unset_timer">> -> {Args, {<<>>, [<<"timer_body">>]}, #{timer => unset_timer, tag => undefined}};
                <<"fail">>  -> erlang:error(fail)
            end
        end
    ,
    SignalFunc =
        fun({Args, _Machine}) ->
            case Args of
                {init  , <<"fail" >>} -> erlang:error(fail);
                {repair, <<"error">>} -> erlang:error(error);
                 timeout              -> {{<<>>, [<<"handle_timer_body">>]}, #{timer => undefined, tag => undefined}};
                _ -> mg_test_processor:default_result(signal)
            end
        end
    ,
    {ok, ProcessorPid} = start_processor({0, 0, 0, 0}, 8023, "/processor", {SignalFunc, CallFunc}),

    [
        {apps              , Apps                             },
        {automaton_options , #{
            url            => "http://localhost:8022",
            ns             => ?NS,
            retry_strategy => undefined
        }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec mg_woody_api_config(atom(), config()) ->
    list().
mg_woody_api_config(event_sink, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS => #{
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    recv_timeout   => 5000,
                    transport_opts => [{pool, ns}, {max_connections, 100}]
                },
                event_sink => ?ES_ID
            }
        }}
    ];
mg_woody_api_config(_, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS => #{
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, ns}, {max_connections, 100}]
                },
                event_sink => ?ES_ID
            }
        }}
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(memory, _) ->
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
    Opts = maps:update(ns, <<"incorrect_NS">>, automaton_options(C)),
    #'NamespaceNotFound'{} = (catch mg_automaton_client:start(Opts, ?ID, ?Tag)).

-spec machine_start(config()) -> _.
machine_start(C) ->
    ok = start_machine(C, ?ID).

-spec machine_already_exists(config()) -> _.
machine_already_exists(C) ->
    #'MachineAlreadyExists'{} = (catch mg_automaton_client:start(automaton_options(C), ?ID, ?Tag)).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
    IncorrectID = <<"incorrect_ID">>,
    #'MachineNotFound'{} =
        (catch mg_automaton_client:call(automaton_options(C), {id, IncorrectID}, <<"nop">>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    <<"nop">> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"nop">>).

-spec machine_set_tag(config()) -> _.
machine_set_tag(C) ->
    <<"tag">> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"tag">>).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(C) ->
    IncorrectTag = <<"incorrect_Tag">>,
    #'MachineNotFound'{} =
        (catch mg_automaton_client:call(automaton_options(C), {tag, IncorrectTag}, <<"nop">>)).

-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(C) ->
    <<"nop">> = mg_automaton_client:call(automaton_options(C), ?Ref, <<"nop">>).

-spec machine_remove(config()) -> _.
machine_remove(C) ->
    ok = mg_automaton_client:remove(automaton_options(C), ?ID).

%%
%% repair group tests
%%
%% падение машины
-spec failed_machine_start(config()) ->
    _.
failed_machine_start(C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:start(automaton_options(C), ?ID, <<"fail">>)).

-spec machine_processor_error(config()) ->
    _.
machine_processor_error(C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"fail">>)).

-spec failed_machine_call(config()) ->
    _.
failed_machine_call(C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"ok">>)).

-spec failed_machine_repair_error(config()) ->
    _.
failed_machine_repair_error(C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:repair(automaton_options(C), {id, ?ID}, <<"error">>)).

-spec failed_machine_repair(config()) ->
    _.
failed_machine_repair(C) ->
    ok = mg_automaton_client:repair(automaton_options(C), {id, ?ID}, <<"ok">>).

%%
%% timer
%%
-spec handle_timer(config()) ->
    _.
handle_timer(C) ->
    #{history := InitialEvents} =
        mg_automaton_client:get_machine(automaton_options(C), {id, ?ID}, {undefined, undefined, forward}),
    <<"set_timer">> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"set_timer">>),
    #{history := History1} =
        mg_automaton_client:get_machine(automaton_options(C), {id, ?ID}, {undefined, undefined, forward}),
    [StartTimerEvent] = History1 -- InitialEvents,
    ok = timer:sleep(2000),
    #{history := History2} =
        mg_automaton_client:get_machine(automaton_options(C), {id, ?ID}, {undefined, undefined, forward}),
    [StartTimerEvent, _] = History2 -- InitialEvents.

-spec abort_timer(config()) ->
    _.
abort_timer(C) ->
    #{history := InitialEvents} =
        mg_automaton_client:get_machine(automaton_options(C), {id, ?ID}, {undefined, undefined, forward}),
    <<"set_timer"  >> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"set_timer"  >>),
    <<"unset_timer">> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"unset_timer">>),
    ok = timer:sleep(2000),
    #{history := History1} =
        mg_automaton_client:get_machine(automaton_options(C), {id, ?ID}, {undefined, undefined, forward}),
    [_] = History1 -- InitialEvents.

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
    ok = start_machine(C, ?ID),

    _ = create_events(3, C, ?ID),

    [
        #'SinkEvent'{id = 1, source_id = ?ID, source_ns = ?NS, event = #'Event'{}},
        #'SinkEvent'{id = 2, source_id = ?ID, source_ns = ?NS, event = #'Event'{}},
        #'SinkEvent'{id = 3, source_id = ?ID, source_ns = ?NS, event = #'Event'{}}
    ] = mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=forward}).

-spec event_sink_get_last_event(config()) ->
    _.
event_sink_get_last_event(C) ->
    [#'SinkEvent'{id = 3, source_id = _ID, source_ns = _NS, event = #'Event'{}}] =
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
    N = 20,
    _ = create_events(N, C, ?ID),

    Events = mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=forward}),
    EventsIDs = lists:seq(1, N + LastEventID),
    EventsIDs = [ID0 || #'SinkEvent'{id=ID0} <- Events].


-spec config_with_multiple_event_sinks(config()) ->
    _.
config_with_multiple_event_sinks(_C) ->
    Config = [
        {storage, mg_storage_memory},
        {namespaces, #{
            <<"1">> => #{
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, pool1}, {max_connections, 100}]
                },
                event_sink => <<"SingleES">>
            },
            <<"2">> => #{
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, pool2}, {max_connections, 100}]
                },
                event_sink => <<"SingleES">>
            }
        }}
    ],

    Apps = genlib_app:start_application_with(mg_woody_api, Config),
    [application_stop(App) || App <- Apps].

%%
%% utils
%%
-spec start_machine(config(), mg:id()) ->
    ok.
start_machine(C, ID) ->
    mg_automaton_client:start(automaton_options(C), ID, ID).

-spec create_event(binary(), config(), mg:id()) ->
    _.
create_event(Event, C, ID) ->
    mg_automaton_client:call(automaton_options(C), {id, ID}, Event).

-spec create_events(integer(), config(), mg:id()) -> _.
create_events(N, C, ID) ->
    lists:foreach(
            fun(_) ->
                _ = create_event(<<"event">>, C, ID)
            end,
            lists:seq(1, N)
    ).

-spec start_processor(Address, Port, Path, Functions) -> {ok, pid()} when
    Address   :: mg_test_processor:host_address(),
    Port      :: integer(),
    Path      :: string(),
    Functions :: {mg_test_processor:processor_function(), mg_test_processor:processor_function()}.
start_processor(Address, Port, Path, {SignalFunc, CallFunc}) ->
    {ok, ProcessorPid} = mg_test_processor:start_link({Address, Port, Path, {SignalFunc, CallFunc}}),
    true = erlang:unlink(ProcessorPid),
    {ok, ProcessorPid}.

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).

-spec es_opts(config()) -> _.
es_opts(C) -> ?config(event_sink_options, C).
