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
-module(machinegun_tests_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
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

%%

-define(NS, <<"NS">>).
-define(ID, <<"ID">>).
-define(EMPTY_ID, <<"">>).
-define(Tag, <<"tag">>).
-define(Ref, {tag, ?Tag}).
-define(ES_ID, <<"test_event_sink_2">>).

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
        {group, base}
    ].

-spec groups() ->
    [{group_name(), list(_), [test_name()]}].
groups() ->
    [
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
    init_per_group([{storage, mg_core_storage_memory} | C]);
init_per_group(history, C) ->
    init_per_group([{storage, mg_core_storage_memory} | C]);
init_per_group(_, C) ->
    % NOTE
    % Даже такой небольшой шанс может сработать в ситуациях, когда мы в процессоре выгребаем большой кусок
    % истории машины, из-за чего реальная вероятность зафейлить операцию равна (1 - (1 - p) ^ n).
    init_per_group([{storage, {mg_core_storage_memory, #{random_transient_fail => 0.01}}} | C]).

-spec init_per_group(config()) ->
    config().
init_per_group(C) ->
    %% TODO сделать нормальную генерацию урлов
    Config = mg_woody_api_config(C),
    Apps = machinegun_ct_helper:start_applications([
        brod,
        {machinegun, Config}
    ]),
    {ok, ProcessorPid} = machinegun_test_processor:start(
        {0, 0, 0, 0}, 8023,
        genlib_map:compact(#{
            processor  => {
                "/processor", #{
                    signal => fun default_signal_handler/1,
                    call   => fun default_call_handler/1,
                    repair => fun default_repair_handler/1
                }
            }
        })
    ),

    [
        {apps              , Apps                  },
        {automaton_options , #{
            url            => "http://localhost:8022",
            ns             => ?NS,
            retry_strategy => genlib_retry:linear(3, 1)
        }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec default_signal_handler(mg_events_machine:signal_args()) ->
    mg_events_machine:signal_result().
default_signal_handler({Args, _Machine}) ->
    case Args of
        {init, <<"fail" >>} ->
            erlang:error(fail);
        {init, <<"timeout">>} ->
            timer:sleep(infinity);
        {init, [<<"fire">>, HistoryLen, EventBody, AuxState]} ->
            {
                {content(AuxState), [content(EventBody) || _ <- lists:seq(1, HistoryLen)]},
                #{timer => undefined, tag => undefined}
            };
        {repair, <<"error">>} ->
            erlang:error(error);
         timeout ->
             {{null(), [content(<<"handle_timer_body">>)]}, #{timer => undefined, tag => undefined}};
        _ ->
            machinegun_test_processor:default_result(signal, Args)
    end.

-spec default_call_handler(mg_events_machine:call_args()) ->
    mg_events_machine:call_result().
default_call_handler({Args, #{history := History}}) ->
    Evs = [N || #{body := {_Metadata, N}} <- History],
    SetTimer = {set_timer, {timeout, 1}, {undefined, undefined, forward}, 30},
    case Args of
        [<<"event">>, I]  ->
            case lists:member(I, Evs) of
                false -> {I, {null(), [content(I)]}, #{}};
                true  -> {I, {null(), []}, #{}}
            end;
        <<"tag"  >>       -> {Args, {null(), [content(<<"tag_body"  >>)]}, #{tag => Args}};
        <<"nop"  >>       -> {Args, {null(), [                ]}, #{}};
        <<"set_timer"  >> -> {Args, {null(), [content(<<"timer_body">>)]}, #{timer => SetTimer   }};
        <<"unset_timer">> -> {Args, {null(), [content(<<"timer_body">>)]}, #{timer => unset_timer}};
        <<"fail"  >>      -> erlang:error(fail);
        <<"sleep">>       -> timer:sleep(?DEADLINE_TIMEOUT * 2), {Args, {null(), [content(<<"sleep">>)]}, #{}};
        <<"remove">>      -> {Args, {null(), [content(<<"removed">>)]}, #{remove => remove}}
    end.

-spec default_repair_handler(mg_events_machine:repair_args()) ->
    mg_events_machine:repair_result().
default_repair_handler({Args, _Machine}) ->
    case Args of
        <<"error">> ->
            erlang:error(error);
        <<"business_error">> ->
            erlang:throw(#mg_stateproc_RepairFailed{reason = {bin, <<"because">>}});
        _ ->
            {Args, {null(), []}, #{}}
    end.

-spec null() -> mg_events:content().
null() ->
    content(null).

-spec content(mg_storage:opaque()) -> mg_events:content().
content(Body) ->
    {#{format_version => 42}, Body}.

-spec mg_woody_api_config(config()) ->
    list().
mg_woody_api_config(C) ->
    Scheduler = #{
        scan_interval => #{continue => 500, completed => 15000},
        task_quota    => <<"scheduler_tasks_total">>
    },
    [
        {woody_server, #{ip => {0,0,0,0,0,0,0,0}, port => 8022, limits => #{}}},
        {quotas, [
            #{
                name => <<"scheduler_tasks_total">>,
                limit => #{ value => 10 },
                update_interval => 100
            }
        ]},
        {namespaces, #{
            ?NS => #{
                storage    =>  ?config(storage, C),
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => #{pool => ns, max_connections => 100}
                },
                default_processing_timeout => 5000,
                schedulers => #{
                    timers         => Scheduler,
                    timers_retries => Scheduler,
                    overseer       => Scheduler
                },
                retries => #{
                    storage   => {exponential, infinity, 1, 10},
                    timers    => {exponential, infinity, 1, 10}
                },
                % сейчас существуют проблемы, которые не дают включить на постоянной основе эту опцию
                % (а очень хочется, чтобы проверять работоспособность идемпотентных ретраев)
                % TODO в будущем нужно это сделать
                % сейчас же можно иногда включать и смотреть
                % suicide_probability => 0.1,
                event_sinks => [
                    {mg_core_events_sink_machine, #{
                        name => machine,
                        machine_id => ?ES_ID
                    }},
                    {mg_core_events_sink_kafka, #{
                        name => kafka,
                        topic => ?ES_ID,
                        client => mg_kafka_client
                    }}
                ]
            }
        }},
        {event_sink_ns, #{
            storage => mg_core_storage_memory,
            default_processing_timeout => 5000
        }}
    ].

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, C) ->
    ok = proc_lib:stop(?config(processor_pid, C)),
    machinegun_ct_helper:stop_applications(?config(apps, C)).

%%
%% base group tests
%%
-spec namespace_not_found(config()) -> _.
namespace_not_found(C) ->
    Opts = maps:update(ns, <<"incorrect_NS">>, automaton_options(C)),
    #mg_stateproc_NamespaceNotFound{} = (catch machinegun_automaton_client:start(Opts, ?ID, ?Tag)).

-spec machine_start_empty_id(config()) -> _.
machine_start_empty_id(C) ->
    {'EXIT', {{woody_error, _}, _}} = % создание машины с невалидным ID не обрабатывается по протоколу
        (catch machinegun_automaton_client:start(automaton_options(C), ?EMPTY_ID, ?Tag)),
    ok.

-spec machine_start(config()) -> _.
machine_start(C) ->
    ok = start_machine(C, ?ID).

-spec machine_already_exists(config()) -> _.
machine_already_exists(C) ->
    #mg_stateproc_MachineAlreadyExists{} = (catch machinegun_automaton_client:start(automaton_options(C), ?ID, ?Tag)).

-spec machine_id_not_found(config()) -> _.
machine_id_not_found(C) ->
     _ = code:load_file(mg_core_storage_memory),
    IncorrectID = <<"incorrect_ID">>,
    #mg_stateproc_MachineNotFound{} =
        (catch machinegun_automaton_client:call(automaton_options(C), {id, IncorrectID}, <<"nop">>)).

-spec machine_empty_id_not_found(config()) -> _.
machine_empty_id_not_found(C) ->
    #mg_stateproc_MachineNotFound{} =
        (catch machinegun_automaton_client:call(automaton_options(C), {id, ?EMPTY_ID}, <<"nop">>)).

-spec machine_call_by_id(config()) -> _.
machine_call_by_id(C) ->
    <<"nop">> = machinegun_automaton_client:call(automaton_options(C), {id, ?ID}, <<"nop">>).

-spec machine_set_tag(config()) -> _.
machine_set_tag(C) ->
    <<"tag">> = machinegun_automaton_client:call(automaton_options(C), {id, ?ID}, <<"tag">>).

-spec machine_tag_not_found(config()) -> _.
machine_tag_not_found(C) ->
    IncorrectTag = <<"incorrect_Tag">>,
    #mg_stateproc_MachineNotFound{} =
        (catch machinegun_automaton_client:call(automaton_options(C), {tag, IncorrectTag}, <<"nop">>)).

-spec machine_call_by_tag(config()) -> _.
machine_call_by_tag(C) ->
    <<"nop">> = machinegun_automaton_client:call(automaton_options(C), ?Ref, <<"nop">>).

-spec machine_remove(config()) -> _.
machine_remove(C) ->
    ok = machinegun_automaton_client:remove(automaton_options(C), ?ID).

-spec machine_remove_by_action(config()) -> _.
machine_remove_by_action(C) ->
    <<"nop">> = machinegun_automaton_client:call(automaton_options(C), {id, ?ID}, <<"nop">>),
    <<"remove">> = try
        machinegun_automaton_client:call(automaton_options(C), {id, ?ID}, <<"remove">>)
    catch
        throw:#mg_stateproc_MachineNotFound{} ->
            % The request had been retried
            <<"remove">>
    end.

%%
%% utils
%%
-spec start_machine(config(), machinegun_core:id()) ->
    ok.
start_machine(C, ID) ->
    start_machine(C, ID, ID).

-spec start_machine(config(), machinegun_core:id(), mg_event_machine:args()) ->
    ok.
start_machine(C, ID, Args) ->
    case catch machinegun_automaton_client:start(automaton_options(C), ID, Args) of
        ok ->
            ok;
        #mg_stateproc_MachineAlreadyExists{} ->
            ok
    end.

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).
