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
-export([namespace_not_found        /1]).
-export([machine_start_empty_id     /1]).
-export([machine_start              /1]).
-export([machine_already_exists     /1]).
-export([machine_call_by_id         /1]).
-export([machine_id_not_found       /1]).
-export([machine_empty_id_not_found /1]).
-export([machine_set_tag            /1]).
-export([machine_call_by_tag        /1]).
-export([machine_tag_not_found      /1]).
-export([machine_remove             /1]).
-export([machine_remove_by_action   /1]).

%% repair group tests
-export([failed_machine_start        /1]).
-export([machine_start_timeout       /1]).
-export([machine_processor_error     /1]).
-export([failed_machine_call         /1]).
-export([failed_machine_repair_error /1]).
-export([failed_machine_repair       /1]).
-export([failed_machine_simple_repair/1]).
-export([working_machine_repair      /1]).

%% timer group tests
-export([handle_timer/1]).
-export([abort_timer /1]).

%% deadline group tests
-export([success_call_with_deadline/1]).
-export([timeout_call_with_deadline/1]).

%% event_sink group tests
-export([event_sink_get_empty_history    /1]).
-export([event_sink_get_not_empty_history/1]).
-export([event_sink_get_last_event       /1]).
-export([event_sink_incorrect_event_id   /1]).
-export([event_sink_incorrect_sink_id    /1]).
-export([event_sink_lots_events_ordering /1]).

%% mwc group tests
-export([mwc_get_statuses_distrib/1]).
-export([mwc_get_failed_machines /1]).
-export([mwc_get_machine         /1]).
-export([mwc_get_events_machine  /1]).
%%

-export([config_with_multiple_event_sinks/1]).

-define(NS, <<"NS">>).
-define(ID, <<"ID">>).
-define(EMPTY_ID, <<"">>).
-define(Tag, <<"tag">>).
-define(Ref, {tag, ?Tag}).
-define(ES_ID, <<"test_event_sink">>).

-define(DEADLINE_TIMEOUT, 1000).

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
        {group, base      },
        {group, repair    },
        {group, timers    },
        {group, event_sink},
        {group, mwc       },
        {group, deadline  },
        config_with_multiple_event_sinks
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [
        % TODO проверить отмену таймера
        % TODO проверить отдельно get_history
        {base, [sequence], [
            namespace_not_found,
            machine_id_not_found,
            machine_empty_id_not_found,
            machine_start_empty_id,
            machine_start,
            machine_already_exists,
            machine_id_not_found,
            machine_call_by_id,
            machine_set_tag,
            machine_tag_not_found,
            machine_call_by_tag,
            machine_remove,
            machine_id_not_found,
            machine_start,
            machine_remove_by_action,
            machine_id_not_found
        ]},

        {repair, [sequence], [
            failed_machine_start,
            machine_start_timeout,
            machine_id_not_found,
            machine_start,
            machine_processor_error,
            failed_machine_call,
            failed_machine_repair_error,
            failed_machine_repair,
            machine_call_by_id,
            working_machine_repair,
            machine_remove,
            machine_start,
            machine_processor_error,
            failed_machine_simple_repair,
            machine_call_by_id,
            machine_remove
        ]},

        {timers, [sequence], [
            machine_start,
            handle_timer
            % handle_timer % был прецендент, что таймер срабатывал только один раз
            % abort_timer
        ]},

        {deadline, [sequence], [
            machine_start,
            success_call_with_deadline,
            timeout_call_with_deadline
        ]},

        {event_sink, [sequence], [
            event_sink_get_empty_history,
            event_sink_get_not_empty_history,
            event_sink_get_last_event,
            % TODO event_not_found
            % event_sink_incorrect_event_id,
            event_sink_incorrect_sink_id,
            event_sink_lots_events_ordering
        ]},

        {mwc, [sequence], [
            machine_start,
            machine_processor_error,
            mwc_get_statuses_distrib,
            mwc_get_failed_machines,
            mwc_get_machine,
            mwc_get_events_machine
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    % dbg:tracer(), dbg:p(all, c),
    % dbg:tpl({mg_machine, retry_strategy, '_'}, x),
    C.

-spec end_per_suite(config()) ->
    ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(mwc, C) ->
    init_per_group([{storage, mg_storage_memory} | C]);
init_per_group(_, C) ->
    init_per_group([{storage, {mg_storage_memory, #{random_transient_fail => 0.1 }}} | C]).

-spec init_per_group(config()) ->
    config().
init_per_group(C) ->
    %% TODO сделать нормальную генерацию урлов
    Apps =
        genlib_app:start_application_with(lager, [
            {handlers, [
                {lager_common_test_backend, [
                    info,
                    {lager_default_formatter, [time, " ", severity, " ", metadata, " ", message]}
                ]}
            ]},
            {async_threshold, undefined}
        ])
        ++
        genlib_app:start_application_with(mg_woody_api, mg_woody_api_config(C))
    ,

    SetTimer = {set_timer, {timeout, 1}, {undefined, undefined, forward}, 30},

    CallFunc =
        fun({Args, _Machine}) ->
            case Args of
                <<"tag"  >> -> {Args, {<<>>, [<<"tag_body"  >>]}, #{tag => Args}};
                <<"event">> -> {Args, {<<>>, [<<"event_body">>]}, #{}};
                <<"nop"  >> -> {Args, {<<>>, [                ]}, #{}};
                <<"set_timer"  >> -> {Args, {<<>>, [<<"timer_body">>]}, #{timer => SetTimer   }};
                <<"unset_timer">> -> {Args, {<<>>, [<<"timer_body">>]}, #{timer => unset_timer}};
                <<"fail"  >> -> erlang:error(fail);
                <<"sleep">> -> timer:sleep(?DEADLINE_TIMEOUT * 2), {Args, {<<>>, [<<"sleep">>]}, #{}};
                <<"remove">> -> {Args, {<<>>, [<<"removed">>]}, #{remove => remove}}
            end
        end
    ,
    SignalFunc =
        fun({Args, _Machine}) ->
            case Args of
                {init  , <<"fail" >>  } -> erlang:error(fail);
                {init  , <<"timeout">>} -> timer:sleep(infinity);
                {repair, <<"error">>  } -> erlang:error(error);
                 timeout                -> {{<<>>, [<<"handle_timer_body">>]}, #{timer => undefined, tag => undefined}};
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

-spec mg_woody_api_config(config()) ->
    list().
mg_woody_api_config(C) ->
    [
        {woody_server, #{ip => {0,0,0,0,0,0,0,0}, port => 8022, limits => #{}}},
        {namespaces, #{
            ?NS => #{
                storage    => ?config(storage, C),
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, ns}, {max_connections, 100}]
                },
                default_processing_timeout => 5000,
                scheduled_tasks => #{
                    timers         => #{ interval => 100, limit => 10 },
                    timers_retries => #{ interval => 100, limit => 10 },
                    overseer       => #{ interval => 100, limit => 10 }
                },
                retries => #{
                    % вообще этого тут быть не должно,
                    % но ввиду того, что events_machine — это процессор,
                    % то проблемы с events_storage приводят к тому,
                    % что срабатывают именно эти ретраи
                    % TODO это нужно исправить
                    processor => {exponential, infinity, 1, 10},
                    storage   => {exponential, infinity, 1, 10},
                    timers    => {exponential, infinity, 1, 10}
                },
                % сейчас существуют проблемы, которые не дают включить на постоянной основе эту опцию
                % (а очень хочется, чтобы проверять работоспособность идемпотентных ретраев)
                % TODO в будущем нужно это сделать
                % сейчас же можно иногда включать и смотреть
                % suicide_probability => 0.1,
                event_sink => ?ES_ID
            }
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
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

-spec machine_start_empty_id(config()) -> _.
machine_start_empty_id(C) ->
    {'EXIT', {{woody_error, _}, _}} = % создание машины с невалидным ID не обрабатывается по протоколу
        (catch mg_automaton_client:start(automaton_options(C), ?EMPTY_ID, ?Tag)),
    ok.

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

-spec machine_empty_id_not_found(config()) -> _.
machine_empty_id_not_found(C) ->
    #'MachineNotFound'{} =
        (catch mg_automaton_client:call(automaton_options(C), {id, ?EMPTY_ID}, <<"nop">>)).

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

-spec machine_remove_by_action(config()) -> _.
machine_remove_by_action(C) ->
    <<"remove">> = mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"remove">>).

%%
%% repair group tests
%%
%% падение машины
-spec failed_machine_start(config()) ->
    _.
failed_machine_start(C) ->
    #'MachineFailed'{} = (catch mg_automaton_client:start(automaton_options(C), ?ID, <<"fail">>)).

-spec machine_start_timeout(config()) ->
    _.
machine_start_timeout(C) ->
    {'EXIT', {{woody_error, _}, _}} =
        (catch mg_automaton_client:start(automaton_options(C), ?ID, <<"timeout">>, mg_utils:timeout_to_deadline(1000))),
    #'MachineNotFound'{} =
        (catch mg_automaton_client:call(automaton_options(C), {id, ?ID}, <<"nop">>)).

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

-spec failed_machine_simple_repair(config()) ->
    _.
failed_machine_simple_repair(C) ->
    ok = mg_automaton_client:simple_repair(automaton_options(C), {id, ?ID}).

-spec working_machine_repair(config()) ->
    _.
working_machine_repair(C) ->
    #'MachineAlreadyWorking'{} = (catch mg_automaton_client:repair(automaton_options(C), {id, ?ID}, <<"ok">>)).

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
%% deadline
%%
-spec timeout_call_with_deadline(config()) ->
    _.
timeout_call_with_deadline(C) ->
    DeadlineFn = fun() -> mg_utils:timeout_to_deadline(?DEADLINE_TIMEOUT) end,
    Options = no_timeout_automaton_options(C),
    {'EXIT', {Reason, _Stack}} = (catch mg_automaton_client:call(Options, {id, ?ID}, <<"sleep">>, DeadlineFn())),
    {woody_error, {external, result_unknown, <<"{timeout,", _Rest/binary>>}} = Reason,
    #'MachineAlreadyWorking'{} = (catch mg_automaton_client:repair(Options, {id, ?ID}, <<"ok">>, DeadlineFn())).

-spec success_call_with_deadline(config()) ->
    _.
success_call_with_deadline(C) ->
    Deadline = mg_utils:timeout_to_deadline(?DEADLINE_TIMEOUT * 3),
    Options = no_timeout_automaton_options(C),
    <<"sleep">> = mg_automaton_client:call(Options, {id, ?ID}, <<"sleep">>, Deadline).

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

% проверяем, что просто ничего не падает, для начала этого хватит
-spec mwc_get_statuses_distrib(config()) ->
    _.
mwc_get_statuses_distrib(_C) ->
    _ = mwc:get_statuses_distrib(?NS).

-spec mwc_get_failed_machines(config()) ->
    _.
mwc_get_failed_machines(_C) ->
    _ = mwc:get_failed_machines(?NS).

-spec mwc_get_machine(config()) ->
    _.
mwc_get_machine(_C) ->
    _ = mwc:get_machine(?NS, ?ID).

-spec mwc_get_events_machine(config()) ->
    _.
mwc_get_events_machine(_C) ->
    _ = mwc:get_events_machine(?NS, {id, ?ID}).

-spec config_with_multiple_event_sinks(config()) ->
    _.
config_with_multiple_event_sinks(_C) ->
    Config = [
        {woody_server, #{ip => {0,0,0,0,0,0,0,0}, port => 8022, limits => #{}}},
        {namespaces, #{
            <<"1">> => #{
                storage    => mg_storage_memory,
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, pool1}, {max_connections, 100}]
                },
                default_processing_timeout => 30000,
                scheduled_tasks => #{
                    timers   => #{ interval => 100, limit => 10 },
                    overseer => #{ interval => 100, limit => 10 }
                },
                retries => #{},
                event_sink => <<"SingleES">>
            },
            <<"2">> => #{
                storage    => mg_storage_memory,
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => [{pool, pool2}, {max_connections, 100}]
                },
                default_processing_timeout => 5000,
                scheduled_tasks => #{
                    timers   => #{ interval => 100, limit => 10 },
                    overseer => #{ interval => 100, limit => 10 }
                },
                retries => #{},
                event_sink => <<"SingleES">>
            }
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
        }},
        {event_sinks, [<<"SingleES">>]}
    ],
    Apps = genlib_app:start_application_with(mg_woody_api, Config),
    [application_stop(App) || App <- Apps].

%%
%% utils
%%
-spec start_machine(config(), mg:id()) ->
    ok.
start_machine(C, ID) ->
    case catch mg_automaton_client:start(automaton_options(C), ID, ID) of
        ok ->
            ok
        % сейчас это не идемпотентная операция
        % #'MachineAlreadyExists'{} ->
        %     ok
    end.

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

-spec no_timeout_automaton_options(config()) -> _.
no_timeout_automaton_options(C) ->
    Options0 = automaton_options(C),
    %% Let's force enlarge client timeout. We expect server timeout only.
    Options0#{transport_opts => [{recv_timeout, ?DEADLINE_TIMEOUT * 10}]}.
