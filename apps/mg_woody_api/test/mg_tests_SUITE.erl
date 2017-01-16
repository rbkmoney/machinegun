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
-export([machine_call_by_id    /1]).
-export([machine_id_not_found  /1]).
-export([machine_call_by_tag   /1]).
-export([machine_tag_not_found /1]).

%% repair group tests
-export([machine_processor_error     /1]).
-export([failed_machine_call         /1]).
-export([failed_machine_repair_error /1]).
-export([failed_machine_repair       /1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).
-export([event_sink_incorrect_event_id   /1]).
-export([event_sink_incorrect_sink_id    /1]).
-export([event_sink_lots_events_ordering /1]).

-export([config_with_multiple_event_sinks/1]).

-define(URL, "http://localhost:8022").
-define(Path, "/processor").
-define(NS, <<"NS">>).
-define(ID, <<"ID">>).
-define(Tag, <<"tag">>).
-define(Ref, {tag, ?Tag}).
-define(ES_ID, <<"ES_ID">>).


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
        {base, [sequence], [
            namespace_not_found,
            machine_start,
            machine_id_not_found,
            machine_call_by_id,
            machine_tag_not_found,
            machine_call_by_tag
        ]},

        {repair, [sequence], [
            machine_start,
            machine_processor_error,
            failed_machine_call,
            failed_machine_repair_error,
            failed_machine_repair
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
        genlib_app:start_application_with(woody, [{acceptors_pool_size, 1}])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(TestGroup, C))
    ,


    {_URL, Path} = {"http://localhost:8022", "/processor"},
    CallFunc =
        fun({Args, _Machine}) ->
            case Args of
                <<"tag">>   -> {Args, {<<>>, [<<"tag_body">>]}, #{timer => undefined, tag => Args}};
                <<"event">> -> {Args, {<<>>, [<<"event_body">>]}, #{timer => undefined, tag => undefined}};
                <<"fail">>  -> erlang:error(fail);
                _           -> {Args, {<<>>, []}, #{timer => undefined, tag => undefined}}
            end
        end
    ,
    SignalFunc =
        fun({Args, _Machine}) ->
            case Args of
                {repair,<<"error">>} -> erlang:error(error);
                _ -> mg_test_processor:default_result(signal)
            end
        end
    ,
    {ok, ProcessorPid} = start_processor(Path, SignalFunc, CallFunc),

    [
        {apps              , Apps                             },
        {automaton_options , {"http://localhost:8022", ?NS   }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     },
        {processor_options , {"http://localhost:8022", "/processor"}}
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
                processor  => #{ url => <<"http://localhost:8023/processor">>, recv_timeout => 5000 },
                event_sink => ?ES_ID
            }
        }}
    ];
mg_woody_api_config(_, C) ->
    [
        {storage, ?config(storage, C)},
        {namespaces, #{
            ?NS => #{
                processor => #{ url => <<"http://localhost:8023/processor">> },
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
namespace_not_found(_C) ->
    NS = <<"incorrect_NS">>,
    #'NamespaceNotFound'{} = (catch mg_automaton_client:start({?URL, NS}, ?ID, ?Tag)).

-spec machine_start(config()) -> _.
machine_start(_C) ->
    ok = mg_automaton_client:start({?URL, ?NS}, ?ID, ?Tag).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(_C) ->
    IncorrectID = <<"incorrect_ID">>,
    #'MachineNotFound'{} = (catch mg_automaton_client:call({?URL, ?NS}, {id, IncorrectID}, <<"test">>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(_C) ->
    <<"test_id">> = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"test_id">>).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(_C) ->
    IncorrectTag = <<"incorrect_Tag">>,
    #'MachineNotFound'{} = (catch mg_automaton_client:call({?URL, ?NS}, {tag, IncorrectTag}, <<"test">>)).

-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(_C) ->
    <<"tag">> = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"tag">>),
    <<"test_id">> = mg_automaton_client:call({?URL, ?NS}, ?Ref, <<"test_id">>).

%%
%% repair group tests
%%
%% падение машины
-spec machine_processor_error(config()) ->
    _.
machine_processor_error(_C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"fail">>)).

-spec failed_machine_call(config()) ->
    _.
failed_machine_call(_C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"ok">>)).

-spec failed_machine_repair_error(config()) ->
    _.
failed_machine_repair_error(_C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:repair({?URL, ?NS}, {id, ?ID}, <<"error">>)).

-spec failed_machine_repair(config()) ->
    _.
failed_machine_repair(_C) ->
    ok = mg_automaton_client:repair({?URL, ?NS}, {id, ?ID}, <<"ok">>).

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
    ok = mg_automaton_client:start({?URL, ?NS}, ?ID, ?Tag),

    <<"event">> = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"event">>),
    <<"event">> = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"event">>),
    <<"event">> = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"event">>),

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
    _ = lists:foreach(
            fun(_) ->
                _ = mg_automaton_client:call({?URL, ?NS}, {id, ?ID}, <<"event">>)
            end,
            lists:seq(1, N)
        ),

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
-spec start_processor(term(), atom(), atom()) -> _.
start_processor(Path, SignalFunc, CallFunc) ->
    {ok, ProcessorPid} = mg_test_processor:start_link({{0, 0, 0, 0}, 8023, Path, {SignalFunc, CallFunc}}),
    true = erlang:unlink(ProcessorPid),
    {ok, ProcessorPid}.

-spec es_opts(config()) -> _.
es_opts(C) -> ?config(event_sink_options, C).
