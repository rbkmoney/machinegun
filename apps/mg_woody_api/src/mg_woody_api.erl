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
%%% Главный модуль приложения.
%%% Тут из конфига строится дерево супервизоров и генерируются структуры с настройками.
%%%
-module(mg_woody_api).

-export_type([config/0]).

%% API
-export([start/0]).
-export([stop /0]).

%%
-export([events_machine_options/2]).
-export([machine_options       /2]).

%% application callbacks
-behaviour(application).
-export([start/2]).
-export([stop /1]).

%%
%% API
%%
-type processor() :: mg_woody_api_processor:options().
-type modernizer() :: #{
    current_format_version := mg_events:format_version(),
    handler                := mg_woody_api_modernizer:options()
}.
-type woody_server() :: #{
    ip             := tuple(),
    port           := inet:port_number(),
    transport_opts => woody_server_thrift_http_handler:transport_opts(),
    protocol_opts  => woody_server_thrift_http_handler:protocol_opts(),
    limits         => woody_server_thrift_http_handler:handler_limits()
}.
-type events_machines() :: #{
    processor                  := processor(),
    modernizer                 => modernizer(),
    worker                     => mg_workers_manager:options(), % all but `worker_options.worker` option
    storage                    := mg_machine:storage_options(),
    event_sinks                => [mg_events_sink:handler()],
    retries                    := mg_machine:retry_opt(),
    schedulers                 := mg_machine:schedulers_opt(),
    default_processing_timeout := timeout(),
    suicide_probability        => mg_machine:suicide_probability(),
    event_stash_size           := non_neg_integer()
}.
-type event_sink_ns() :: #{
    default_processing_timeout := timeout(),
    storage                    => mg_storage:options(),
    worker                     => mg_worker:options()
}.
-type config_element() ::
      {woody_server   , woody_server()                 }
    | {health_check   , erl_health:check()             }
    | {namespaces     , #{mg:ns() => events_machines()}}
    | {event_sink_ns  , event_sink_ns()                }
.
-type config() :: [config_element()].

-define(EVENT_SINK_NS, <<"_event_sinks">>).

-spec start() ->
    {ok, _}.
start() ->
    application:ensure_all_started(?MODULE).

-spec stop() ->
    ok.
stop() ->
    application:stop(?MODULE).

%%
%% Application callbacks
%%
-spec start(normal, any()) ->
    {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    Config = application:get_all_env(?MODULE),
    mg_utils_supervisor_wrapper:start_link(
        {local, ?MODULE},
        #{strategy => rest_for_one},
        quotas_child_specs(Config, quota)
        ++
        [event_sink_ns_child_spec(Config, event_sink)]
        ++
        events_machines_child_specs(Config)
        ++
        [woody_server_child_spec(Config, woody_server)]
        ++
        [woody_metrics_handler_childspec()]
    ).

-spec stop(any()) ->
    ok.
stop(_State) ->
    ok.

%%
%% local
%%

-spec quotas_child_specs(config(), atom()) ->
    [supervisor:child_spec()].
quotas_child_specs(Config, ChildID) ->
    [
        mg_quota_worker:child_spec(Options, {ChildID, maps:get(name, Options)})
        || Options <- proplists:get_value(quotas, Config, [])
    ].

-spec events_machines_child_specs(config()) ->
    [supervisor:child_spec()].
events_machines_child_specs(Config) ->
    NSs = proplists:get_value(namespaces, Config),
    [
        mg_events_machine:child_spec(events_machine_options(NS, Config), binary_to_atom(NS, utf8))
        || NS <- maps:keys(NSs)
    ].

-spec event_sink_ns_child_spec(config(), atom()) ->
    supervisor:child_spec().
event_sink_ns_child_spec(Config, ChildID) ->
    mg_events_sink_machine:child_spec(event_sink_namespace_options(Config), ChildID).

-spec woody_server_child_spec(config(), atom()) ->
    supervisor:child_spec().
woody_server_child_spec(Config, ChildID) ->
    WoodyConfig = proplists:get_value(woody_server, Config),
    HealthCheck = proplists:get_value(health_check, Config, #{}),
    HealthRoute = erl_health_handle:get_route(enable_health_logging(HealthCheck)),
    woody_server:child_spec(
        ChildID,
        #{
            protocol       => thrift,
            transport      => http,
            ip             => maps:get(ip             , WoodyConfig),
            port           => maps:get(port           , WoodyConfig),
            transport_opts => maps:get(transport_opts , WoodyConfig, #{}),
            protocol_opts  => maps:get(protocol_opts  , WoodyConfig, #{}),
            event_handler  => {mg_woody_api_event_handler, pulse()},
            handler_limits => maps:get(limits         , WoodyConfig, #{}),
            handlers       => [
                mg_woody_api_automaton :handler(api_automaton_options (Config)),
                mg_woody_api_event_sink:handler(api_event_sink_options(Config))
            ],
            additional_routes => [HealthRoute]
        }
    ).

-spec enable_health_logging(erl_health:check()) ->
    erl_health:check().
enable_health_logging(Check) ->
    EvHandler = {erl_health_event_handler, []},
    maps:map(fun (_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check).

-spec woody_metrics_handler_childspec() ->
    supervisor:child_spec().
woody_metrics_handler_childspec() ->
    hay_metrics_handler:child_spec(woody_api_hay, woody_metrics_handler).

-spec api_automaton_options(config()) ->
    mg_woody_api_automaton:options().
api_automaton_options(Config) ->
    NSs = proplists:get_value(namespaces, Config),
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{NS => maps:merge(
                #{
                    machine => events_machine_options(NS, Config)
                },
                modernizer_options(maps:get(modernizer, ConfigNS, undefined))
            )}
        end,
        #{},
        NSs
    ).

-spec events_machine_options(mg:ns(), config()) ->
    mg_events_machine:options().
events_machine_options(NS, Config) ->
    NSs = proplists:get_value(namespaces, Config),
    NSConfigs = maps:get(NS, NSs),
    #{processor := ProcessorConfig, storage := Storage} = NSConfigs,
    EventSinks = [
        event_sink_options(SinkConfig, Config)
        || SinkConfig <- maps:get(event_sinks, NSConfigs, [])
    ],
    EventsStorage = add_storage_metrics(NS, events, sub_storage_options(<<"events">>, Storage)),
    #{
        namespace                  => NS,
        processor                  => processor(ProcessorConfig),
        tagging                    => tags_options(NS, NSConfigs),
        machines                   => machine_options(NS, NSConfigs),
        events_storage             => EventsStorage,
        event_sinks                => EventSinks,
        pulse                      => pulse(),
        default_processing_timeout => maps:get(default_processing_timeout, NSConfigs),
        event_stash_size           => maps:get(event_stash_size, NSConfigs, 0)
    }.

-spec event_sink_options(mg_events_sink:handler(), config()) ->
    mg_events_sink:handler().
event_sink_options({mg_events_sink_machine, EventSinkConfig}, Config) ->
    EventSinkNS = event_sink_namespace_options(Config),
    {mg_events_sink_machine, maps:merge(EventSinkNS, EventSinkConfig)};
event_sink_options({mg_events_sink_kafka, EventSinkConfig}, _Config) ->
    {mg_events_sink_kafka, EventSinkConfig#{
        pulse            => pulse(),
        encoder          => fun mg_woody_api_event_sink:serialize/3
    }}.

-spec event_sink_namespace_options(config()) ->
    mg_events_sink_machine:ns_options().
event_sink_namespace_options(Config) ->
    EventSinkNS = #{storage := Storage} = proplists:get_value(event_sink_ns, Config),
    NS = <<"_event_sinks">>,
    MachinesStorage = add_storage_metrics(NS, machines, sub_storage_options(<<"machines">>, Storage)),
    EventsStorage = add_storage_metrics(NS, events, sub_storage_options(<<"events">>, Storage)),
    EventSinkNS#{
        namespace        => NS,
        pulse            => pulse(),
        storage          => MachinesStorage,
        events_storage   => EventsStorage,
        worker           => worker_manager_options(EventSinkNS)
    }.

-spec tags_options(mg:ns(), events_machines()) ->
    mg_machine_tags:options().
tags_options(NS, #{retries := Retries, storage := Storage} = Config) ->
    TagsNS = mg_utils:concatenate_namespaces(NS, <<"tags">>),
    % по логике тут должен быть sub namespace, но его по историческим причинам нет
    TagsStorage = add_storage_metrics(TagsNS, tags, Storage),
    #{
        namespace => TagsNS,
        storage   => TagsStorage,
        worker    => worker_manager_options(Config),
        pulse     => pulse(),
        retries   => Retries
    }.

-spec machine_options(mg:ns(), events_machines()) ->
    mg_machine:options().
machine_options(NS, Config) ->
    #{storage := Storage} = Config,
    Options = maps:with(
        [
            retries,
            timer_processing_timeout
        ],
        Config
    ),
    MachinesStorage = add_storage_metrics(NS, machines, sub_storage_options(<<"machines">>, Storage)),
    Options#{
        namespace           => NS,
        storage             => MachinesStorage,
        worker              => worker_manager_options(Config),
        schedulers          => maps:get(schedulers, Config, #{}),
        pulse               => pulse(),
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec worker_manager_options(map()) ->
    mg_workers_manager:options().
worker_manager_options(Config) ->
    maps:merge(
        #{
            registry => mg_procreg_gproc,
            sidecar  => mg_woody_api_hay
        },
        maps:get(worker, Config, #{})
    ).

-spec processor(processor()) ->
    mg_utils:mod_opts().
processor(Processor) ->
    {mg_woody_api_processor, Processor#{event_handler => {mg_woody_api_event_handler, pulse()}}}.

-spec modernizer_options(modernizer() | undefined) ->
    #{modernizer => mg_events_modernizer:options()}.
modernizer_options(#{current_format_version := CurrentFormatVersion, handler := WoodyClient}) ->
    #{modernizer => #{
        current_format_version => CurrentFormatVersion,
        handler => {mg_woody_api_modernizer, WoodyClient#{event_handler => {mg_woody_api_event_handler, pulse()}}}
    }};
modernizer_options(undefined) ->
    #{}.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    EventSinkMachines  = collect_event_sink_machines(Config),
    {EventSinkMachines, event_sink_namespace_options(Config)}.

-spec collect_event_sink_machines(config()) ->
    [mg:id()].
collect_event_sink_machines(Config) ->
    NSs = proplists:get_value(namespaces, Config),
    NSConfigs = maps:values(NSs),
    EventSinks = ordsets:from_list([
        maps:get(machine_id, SinkConfig)
        || NSConfig <- NSConfigs, {mg_events_sink_machine, SinkConfig} <- maps:get(event_sinks, NSConfig, [])
    ]),
    ordsets:to_list(EventSinks).

-spec sub_storage_options(mg:ns(), mg_machine:storage_options()) ->
    mg_machine:storage_options().
sub_storage_options(SubNS, Storage0) ->
    Storage1 = mg_utils:separate_mod_opts(Storage0, #{}),
    Storage2 = add_bucket_postfix(SubNS, Storage1),
    Storage2.

-spec add_bucket_postfix(mg:ns(), mg_storage:options()) ->
    mg_storage:options().
add_bucket_postfix(_, {mg_storage_memory, _} = Storage) ->
    Storage;
add_bucket_postfix(SubNS, {mg_storage_riak, #{bucket := Bucket} = Options}) ->
    {mg_storage_riak, Options#{bucket := mg_utils:concatenate_namespaces(Bucket, SubNS)}}.

-spec add_storage_metrics(mg:ns(), _type, mg_machine:storage_options()) ->
    mg_machine:storage_options().
add_storage_metrics(NS, Type, Storage0) ->
    Storage1 = mg_utils:separate_mod_opts(Storage0, #{}),
    do_add_storage_metrics(NS, Type, Storage1).

-spec do_add_storage_metrics(mg:ns(), atom(), mg_machine:storage_options()) ->
    mg_machine:storage_options().
do_add_storage_metrics(_NS, _Type, {mg_storage_memory, _} = Storage) ->
    Storage;
do_add_storage_metrics(NS, Type, {mg_storage_riak, Options}) ->
    PoolOptions = maps:get(pool_options, Options, #{}),
    NewOptions = Options#{
        sidecar => {mg_woody_api_riak_metric, #{
            namespace => NS,
            type => Type
        }},
        pool_options => PoolOptions#{
            metrics_mod => mg_woody_api_riak_metric,
            metrics_api => exometer
        }
    },
    {mg_storage_riak, NewOptions}.

-spec pulse() ->
    mg_pulse:handler().
pulse() ->
    mg_woody_api_pulse.
