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
% -export([machine_processor_error    /1]).
-export([failed_machine_call        /1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).
-export([event_sink_incorrect_event_id   /1]).
-export([event_sink_incorrect_sink_id    /1]).
-export([event_sink_lots_events_ordering /1]).

-export([config_with_multiple_event_sinks/1]).

% -define(NS(C), <<"mg_test_", (proplists:get_value(test_instance, C))/binary, "_ns">>).
-define(NS(_C), <<"mg_test_ns">>).
-define(ID, <<"mg_test_id">>).
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
            machine_call_by_id,
            machine_id_not_found,
            machine_tag_not_found,
            machine_call_by_tag
        ]}

        % {repair, [sequence], [
        %     machine_start,
        %     failed_machine_call,
        %     machine_call_by_id
        % ]},

        % {event_sink, [sequence], [
        %     event_sink_get_empty_history,
        %     event_sink_get_not_empty_history,
        %     event_sink_get_last_event,
        %     % TODO event_not_found
        %     % event_sink_incorrect_event_id,
        %     event_sink_incorrect_sink_id,
        %     event_sink_lots_events_ordering
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

    [
        {apps              , Apps                             },
        {automaton_options , {"http://localhost:8022", ?NS(C)}},
        {event_sink_options,  "http://localhost:8022"         },
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
                processor => #{ url => <<"http://localhost:8023/processor">> },
                event_sink => ?ES_ID
            }
        }}
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(memory, _) ->
    ok;
end_per_group(_, _C) ->
    % true = erlang:exit(?config(processor_pid, C), kill),
    % [application_stop(App) || App <- proplists:get_value(apps, C)],
    ok.
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
    {URL, Path, _NS, Tag, ID} = test_opts(<<"_namespace_not_found_id">>, C),
    NS = <<"incorrect_NS">>,
    CallFunc =
        fun({Args, _Machine}) ->
            {Args, {<<>>, []}, #{timer => undefined, tag => undefined}}
        end
    ,
    _ = start_processor(Path, default_func, CallFunc),
    #'NamespaceNotFound'{} = (catch mg_automaton_client:start({URL, NS}, ID, Tag)).

-spec machine_start(config()) -> _.
machine_start(C) ->
    {URL, _Path, NS, Tag, ID} = test_opts(<<"_machine_start_id">>, C),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    {URL, _Path, NS, Tag, ID} = test_opts(<<"_machine_call_by_id">>, C),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),
    <<"test_id">> = mg_automaton_client:call({URL, NS}, {id, ID}, <<"test_id">>).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
    {URL, _Path, NS, Tag, ID} = test_opts(<<"_machine_id_not_found">>, C),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),
    IncorrectID = <<"incorrect_ID">>,
    #'MachineNotFound'{} = (catch mg_automaton_client:call({URL, NS}, {id, IncorrectID}, <<"test">>)).

-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(C) ->
    {URL, _Path, NS, _Tag, ID} = test_opts(<<"_machine_call_by_tag">>, C),
    Tag = <<"Tag_namespace_not_found">>,
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),
    <<"test_id">> = mg_automaton_client:call({URL, NS}, {tag, Tag}, <<"test_id">>).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(C) ->
    {URL, _Path, NS, Tag, ID} = test_opts(<<"_machine_tag_not_found">>, C),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),
    IncorrectTag = <<"incorrect_Tag">>,
    #'MachineNotFound'{} = (catch mg_automaton_client:call({URL, NS}, {tag, IncorrectTag}, <<"test">>)).
%%
%% repair group tests
%%
%% падение машины
-spec failed_machine_call(config()) ->
    _.
failed_machine_call(C) ->
    {URL, Path, NS, Tag, ID} = test_opts(<<"_failed_machine_call">>, C),
    CallFunc =
        fun(_Args) ->
            erlang:error(fail)
        end
    ,
    {ok, _ProcessorPid} = start_processor(Path, default_func, CallFunc),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),

    #'MachineFailed'{} = (catch mg_automaton_client:call({URL, NS}, {id, ID}, <<"test_fail">>)).

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
    {URL, Path, NS, Tag, ID} = test_opts(<<"_event_sink_get_not_empty_history">>, C),
    CallFunc =
        fun({Args, _Machine}) ->
            {Args, {<<>>, [<<"event_body">>]}, #{timer => undefined, tag => undefined}}
        end
    ,
    {ok, _ProcessorPid} = start_processor(Path, default_func, CallFunc),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),

    <<"test1">> = mg_automaton_client:call({URL, NS}, {id, ID}, <<"test1">>),
    <<"test2">> = mg_automaton_client:call({URL, NS}, {id, ID}, <<"test2">>),
    <<"test3">> = mg_automaton_client:call({URL, NS}, {id, ID}, <<"test3">>),

    [
        #'SinkEvent'{id = 1, source_id = ID, source_ns = NS, event = #'Event'{}},
        #'SinkEvent'{id = 2, source_id = ID, source_ns = NS, event = #'Event'{}},
        #'SinkEvent'{id = 3, source_id = ID, source_ns = NS, event = #'Event'{}}
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
    {URL, Path, NS, Tag, ID} = test_opts(<<"_event_sink_lots_events_ordering">>, C),
    CallFunc =
        fun({Args, _Machine}) ->
            {Args, {<<>>, [<<"event_body1">>]}, #{timer => undefined, tag => undefined}}
        end
    ,
    {ok, _ProcessorPid} = start_processor(Path, default_func, CallFunc),
    ok = mg_automaton_client:start({URL, NS}, ID, Tag),

    [#'SinkEvent'{id = LastEventID}] =
        mg_event_sink_client:get_history(es_opts(C), ?ES_ID, #'HistoryRange'{direction=backward, limit=1}),
    N = 20,
    _ = lists:foreach(
            fun(_) ->
                _ = mg_automaton_client:call({URL, NS}, {id, ID}, <<"test">>)
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
    true = erlang:unlink(ProcessorPid).

-spec test_opts(binary(), config()) -> _.
test_opts(Name, C) ->
    {URL, Path} = ?config(processor_options, C),
    NS = ?NS(C),
    Tag = <<?Tag/binary, Name/binary>>,
    ID = <<(proplists:get_value(test_instance, C))/binary, Name/binary>>,
    {URL, Path, NS, Tag, ID}.

-spec es_opts(config()) -> _.
es_opts(C) -> ?config(event_sink_options, C).
