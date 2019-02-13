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
-export([events_machine_options/3]).
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
% упс, а вот и протечка абстракции.
% в woody этот тип не экспортируется, а хочется
-type woody_server_protocol_opts() :: cowboy_protocol:opts().
-type woody_server() :: #{
    ip             := tuple(),
    port           := inet:port_number(),
    transport_opts => woody_server_thrift_http_handler:transport_opts(),
    protocol_opts  => woody_server_protocol_opts(),
    limits         => woody_server_thrift_http_handler:handler_limits()
}.
-type events_machines() :: #{
    processor                  := processor(),
    modernizer                 => modernizer(),
    storage                    := mg_storage:options(),
    event_sink                 => mg:id(),
    retries                    := mg_machine:retry_opt(),
    schedulers                 := mg_machine:schedulers_opt(),
    default_processing_timeout := timeout(),
    message_queue_len_limit    := mg_workers_manager:queue_limit(),
    suicide_probability        => mg_machine:suicide_probability()
}.
-type event_sink_ns() :: #{
    default_processing_timeout := timeout(),
    storage                    => mg_storage:options(),
    duplicate_search_batch     => mg_storage:index_limit()
}.
-type config_element() ::
      {woody_server   , woody_server()                 }
    | {health_checkers, [erl_health:checker()]         }
    | {namespaces     , #{mg:ns() => events_machines()}}
    | {event_sink_ns  , event_sink_ns()                }
.
-type config() :: [config_element()].

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
    ok = metrics_init(Config),
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
    NSs         = proplists:get_value(namespaces   , Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    [
        mg_events_machine:child_spec(events_machine_options(NS, ConfigNS, EventSinkNS), binary_to_atom(NS, utf8))
        || {NS, ConfigNS} <- maps:to_list(NSs)
    ].

-spec event_sink_ns_child_spec(config(), atom()) ->
    supervisor:child_spec().
event_sink_ns_child_spec(Config, ChildID) ->
    mg_events_sink:child_spec(event_sink_options(proplists:get_value(event_sink_ns, Config)), ChildID).

-spec woody_server_child_spec(config(), atom()) ->
    supervisor:child_spec().
woody_server_child_spec(Config, ChildID) ->
    WoodyConfig    = proplists:get_value(woody_server   , Config),
    HealthCheckers = proplists:get_value(health_checkers, Config),
    woody_server:child_spec(
        ChildID,
        #{
            protocol       => thrift,
            transport      => http,
            ip             => maps:get(ip             , WoodyConfig),
            port           => maps:get(port           , WoodyConfig),
            transport_opts => maps:get(transport_opts , WoodyConfig, []),
            protocol_opts  => maps:get(protocol_opts  , WoodyConfig, []),
            event_handler  => {mg_woody_api_event_handler, mg_woody_api_pulse},
            handler_limits => maps:get(limits         , WoodyConfig, #{}),
            handlers       => [
                mg_woody_api_automaton :handler(api_automaton_options (Config)),
                mg_woody_api_event_sink:handler(api_event_sink_options(Config))
            ],
            additional_routes => [
                erl_health_handle:get_route(HealthCheckers)
            ]
        }
    ).

-spec api_automaton_options(config()) ->
    mg_woody_api_automaton:options().
api_automaton_options(Config) ->
    NSs         = proplists:get_value(namespaces, Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{NS => maps:merge(
                #{
                    machine => events_machine_options(NS, ConfigNS, EventSinkNS)
                },
                modernizer_options(maps:get(modernizer, ConfigNS, undefined))
            )}
        end,
        #{},
        NSs
    ).

-spec events_machine_options(mg:ns(), events_machines(), event_sink_ns()) ->
    mg_events_machine:options().
events_machine_options(NS, Config = #{processor := ProcessorConfig, storage := Storage}, EventSinkNS) ->
    EventSinkOptions = event_sink_options(EventSinkNS),
    events_machine_options_event_sink(
        maps:get(event_sink, Config, undefined),
        EventSinkOptions,
        #{
            namespace                  => NS,
            processor                  => processor(ProcessorConfig),
            tagging                    => tags_options(NS, Config),
            machines                   => machine_options(NS, Config),
            events_storage             => add_bucket_postfix(<<"events">>, Storage),
            pulse                      => pulse(),
            default_processing_timeout => maps:get(default_processing_timeout, Config),
            message_queue_len_limit    => maps:get(message_queue_len_limit, Config)
        }
    ).

-spec tags_options(mg:ns(), events_machines()) ->
    mg_machine_tags:options().
tags_options(NS, #{retries := Retries, storage := Storage, message_queue_len_limit := QLimit}) ->
    #{
        namespace => mg_utils:concatenate_namespaces(NS, <<"tags">>),
        storage   => Storage, % по логике тут должен быть sub namespace, но его по историческим причинам нет
        pulse     => pulse(),
        retries   => Retries,
        message_queue_len_limit => QLimit
    }.

-spec machine_options(mg:ns(), events_machines()) ->
    mg_machine:options().
machine_options(NS, Config) ->
    #{storage := Storage} = Config,
    Options = maps:with(
        [
            retries,
            schedulers,
            reschedule_timeout,
            timer_processing_timeout,
            message_queue_len_limit
        ],
        Config
    ),
    Options#{
        namespace           => NS,
        storage             => add_bucket_postfix(<<"machines">>, Storage),
        pulse               => pulse(),
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec events_machine_options_event_sink(mg:id(), mg_events_sink:options(), mg_events_machine:options()) ->
    mg_events_machine:options().
events_machine_options_event_sink(undefined, _, Options) ->
    Options;
events_machine_options_event_sink(EventSinkID, EventSinkOptions, Options) ->
    Options#{
        event_sink => {EventSinkID, EventSinkOptions}
    }.

-spec processor(processor()) ->
    mg_utils:mod_opts().
processor(Processor) ->
    {mg_woody_api_processor, Processor#{event_handler => mg_woody_api_event_handler}}.

-spec modernizer_options(modernizer() | undefined) ->
    #{modernizer => mg_events_modernizer:options()}.
modernizer_options(#{current_format_version := CurrentFormatVersion, handler := WoodyClient}) ->
    #{modernizer => #{
        current_format_version => CurrentFormatVersion,
        handler => {mg_woody_api_modernizer, WoodyClient#{event_handler => mg_woody_api_event_handler}}
    }};
modernizer_options(undefined) ->
    #{}.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    EventSinks  = collect_event_sinks(Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    {EventSinks, event_sink_options(EventSinkNS)}.

-spec event_sink_options(event_sink_ns()) ->
    mg_events_sink:options().
event_sink_options(EventSinkNS = #{storage := Storage, default_processing_timeout := Timeout}) ->
    EventSinkNS#{
        namespace        => <<"_event_sinks">>,
        pulse            => pulse(),
        storage          => add_bucket_postfix(<<"machines">>, Storage),
        events_storage   => add_bucket_postfix(<<"events"  >>, Storage),
        default_processing_timeout => Timeout
    }.

-spec collect_event_sinks(config()) ->
    [mg:id()].
collect_event_sinks(Config) ->
    ordsets:to_list(maps:fold(
        fun
            (_, #{event_sink:=EventSinkID}, Acc) ->
                ordsets:add_element(EventSinkID, Acc);
            (_, _, Acc) ->
                Acc
        end,
        ordsets:new(),
        proplists:get_value(namespaces, Config)
    )).

-spec add_bucket_postfix(mg:ns(), mg_storage:options()) ->
    mg_storage:options().
add_bucket_postfix(_, Storage = mg_storage_memory) ->
    Storage;
add_bucket_postfix(_, Storage = {mg_storage_memory, _}) ->
    Storage;
add_bucket_postfix(SubNS, {mg_storage_pool, Options = #{worker := Worker}}) ->
    {mg_storage_pool, Options#{worker := add_bucket_postfix(SubNS, Worker)}};
add_bucket_postfix(SubNS, {mg_storage_riak, Options = #{bucket := Bucket}}) ->
    {mg_storage_riak, Options#{bucket := mg_utils:concatenate_namespaces(Bucket, SubNS)}}.

-spec metrics_init(config()) ->
    ok | {error, _Details}.
metrics_init(Config) ->
    ConfigNSs = proplists:get_value(namespaces, Config),
    MachineNSs =  maps:keys(ConfigNSs),
    TagNSs = [mg_utils:concatenate_namespaces(NS, <<"tags">>) || NS <- MachineNSs],
    AllNS = [<<"_event_sinks_machines">>] ++ MachineNSs ++ TagNSs,
    Metrics = mg_woody_api_pulse_metric:get_all_metrics(AllNS),
    register_metrics(Metrics).

-spec register_metrics([how_are_you:metric()]) ->
    ok | {error, _Details}.
register_metrics([]) ->
    ok;
register_metrics([M | Metrics]) ->
    case how_are_you:metric_register(M) of
        ok ->
            register_metrics(Metrics);
        {error, _Reason} = Error ->
            Error
    end.

-spec pulse() ->
    mg_pulse:handler().
pulse() ->
    mg_woody_api_pulse.
