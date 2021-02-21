-module(machinegun_configurator).

-export([construct_child_specs/1]).

-type modernizer() :: #{
    current_format_version := mg_core_events:format_version(),
    handler := mg_woody_api_modernizer:options()
}.

-type events_machines() :: #{
    processor := processor(),
    modernizer => modernizer(),
    % all but `worker_options.worker` option
    worker => mg_core_workers_manager:options(),
    storage := mg_core_machine:storage_options(),
    event_sinks => [mg_core_events_sink:handler()],
    retries := mg_core_machine:retry_opt(),
    schedulers := mg_core_machine:schedulers_opt(),
    default_processing_timeout := timeout(),
    suicide_probability => mg_core_machine:suicide_probability(),
    event_stash_size := non_neg_integer()
}.

-type event_sink_ns() :: #{
    default_processing_timeout := timeout(),
    storage => mg_core_storage:options(),
    worker => mg_core_worker:options()
}.

-type namespaces() :: #{mg_core:ns() => events_machines()}.

-type config() :: #{
    woody_server := machinegun_woody_api:woody_server(),
    event_sink_ns := event_sink_ns(),
    namespaces := namespaces(),
    pulse := pulse(),
    quotas => [mg_core_quota_worker:options()],
    health_check => erl_health:check()
}.

-type processor() :: mg_woody_api_processor:options().

-type pulse() :: mg_core_pulse:handler().

-spec construct_child_specs(config()) -> [supervisor:child_spec()].
construct_child_specs(
    #{
        woody_server := WoodyServer,
        event_sink_ns := EventSinkNS,
        namespaces := Namespaces,
        pulse := Pulse
    } = Config
) ->
    Quotas = maps:get(quotas, Config, []),
    HealthChecks = maps:get(health_check, Config, #{}),

    QuotasChildSpec = quotas_child_specs(Quotas, quota),
    EventSinkChildSpec = event_sink_ns_child_spec(EventSinkNS, event_sink, Pulse),
    EventMachinesChildSpec = events_machines_child_specs(Namespaces, EventSinkNS, Pulse),
    WoodyServerChildSpec = machinegun_woody_api:child_spec(
        woody_server,
        #{
            pulse => Pulse,
            automaton => api_automaton_options(Namespaces, EventSinkNS, Pulse),
            event_sink => api_event_sink_options(Namespaces, EventSinkNS, Pulse),
            woody_server => WoodyServer,
            additional_routes => [get_health_route(HealthChecks), get_prometheus_route()]
        }
    ),

    lists:flatten([
        QuotasChildSpec,
        EventSinkChildSpec,
        EventMachinesChildSpec,
        WoodyServerChildSpec
    ]).

%%

-spec get_health_route(erl_health:check()) -> {iodata(), module(), _Opts :: any()}.
get_health_route(Check0) ->
    EvHandler = {erl_health_event_handler, []},
    Check = maps:map(fun(_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check0),
    erl_health_handle:get_route(Check).

-spec get_prometheus_route() -> {iodata(), module(), _Opts :: any()}.
get_prometheus_route() ->
    {"/metrics/[:registry]", prometheus_cowboy2_handler, []}.

-spec quotas_child_specs([mg_core_quota_worker:options()], atom()) -> [supervisor:child_spec()].
quotas_child_specs(Quotas, ChildID) ->
    [
        mg_core_quota_worker:child_spec(Options, {ChildID, maps:get(name, Options)})
     || Options <- Quotas
    ].

-spec events_machines_child_specs(namespaces(), event_sink_ns(), pulse()) -> [supervisor:child_spec()].
events_machines_child_specs(NSs, EventSinkNS, Pulse) ->
    [
        mg_core_events_machine:child_spec(events_machine_options(NS, NSs, EventSinkNS, Pulse), binary_to_atom(NS, utf8))
     || NS <- maps:keys(NSs)
    ].

-spec events_machine_options(mg_core:ns(), namespaces(), event_sink_ns(), pulse()) -> mg_core_events_machine:options().
events_machine_options(NS, NSs, EventSinkNS, Pulse) ->
    NSConfigs = maps:get(NS, NSs),
    #{processor := ProcessorConfig, storage := Storage} = NSConfigs,
    EventSinks = [
        event_sink_options(SinkConfig, EventSinkNS, Pulse)
     || SinkConfig <- maps:get(event_sinks, NSConfigs, [])
    ],
    EventsStorage = add_storage_metrics(NS, events, sub_storage_options(<<"events">>, Storage)),
    #{
        namespace => NS,
        processor => processor(ProcessorConfig, Pulse),
        tagging => tags_options(NS, NSConfigs, Pulse),
        machines => machine_options(NS, NSConfigs, Pulse),
        events_storage => EventsStorage,
        event_sinks => EventSinks,
        pulse => Pulse,
        default_processing_timeout => maps:get(default_processing_timeout, NSConfigs),
        event_stash_size => maps:get(event_stash_size, NSConfigs, 0)
    }.

-spec machine_options(mg_core:ns(), events_machines(), pulse()) -> mg_core_machine:options().
machine_options(NS, Config, Pulse) ->
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
        namespace => NS,
        storage => MachinesStorage,
        worker => worker_manager_options(Config),
        schedulers => maps:get(schedulers, Config, #{}),
        pulse => Pulse,
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined)
    }.

-spec api_automaton_options(namespaces(), event_sink_ns(), pulse()) -> mg_woody_api_automaton:options().
api_automaton_options(NSs, EventSinkNS, Pulse) ->
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{
                NS => maps:merge(
                    #{
                        machine => events_machine_options(NS, NSs, EventSinkNS, Pulse)
                    },
                    modernizer_options(maps:get(modernizer, ConfigNS, undefined), Pulse)
                )
            }
        end,
        #{},
        NSs
    ).

-spec event_sink_options(mg_core_events_sink:handler(), event_sink_ns(), pulse()) -> mg_core_events_sink:handler().
event_sink_options({mg_core_events_sink_machine, EventSinkConfig}, EvSinks, Pulse) ->
    EventSinkNS = event_sink_namespace_options(EvSinks, Pulse),
    {mg_core_events_sink_machine, maps:merge(EventSinkNS, EventSinkConfig)};
event_sink_options({mg_core_events_sink_kafka, EventSinkConfig}, _Config, Pulse) ->
    {mg_core_events_sink_kafka, EventSinkConfig#{
        pulse => Pulse,
        encoder => fun mg_woody_api_event_sink:serialize/3
    }}.

-spec event_sink_ns_child_spec(event_sink_ns(), atom(), pulse()) -> supervisor:child_spec().
event_sink_ns_child_spec(EventSinkNS, ChildID, Pulse) ->
    mg_core_events_sink_machine:child_spec(event_sink_namespace_options(EventSinkNS, Pulse), ChildID).

-spec api_event_sink_options(namespaces(), event_sink_ns(), pulse()) -> mg_woody_api_event_sink:options().
api_event_sink_options(NSs, EventSinkNS, Pulse) ->
    EventSinkMachines = collect_event_sink_machines(NSs),
    {EventSinkMachines, event_sink_namespace_options(EventSinkNS, Pulse)}.

-spec collect_event_sink_machines(namespaces()) -> [mg_core:id()].
collect_event_sink_machines(NSs) ->
    NSConfigs = maps:values(NSs),
    EventSinks = ordsets:from_list([
        maps:get(machine_id, SinkConfig)
     || NSConfig <- NSConfigs, {mg_core_events_sink_machine, SinkConfig} <- maps:get(event_sinks, NSConfig, [])
    ]),
    ordsets:to_list(EventSinks).

-spec event_sink_namespace_options(event_sink_ns(), pulse()) -> mg_core_events_sink_machine:ns_options().
event_sink_namespace_options(#{storage := Storage} = EventSinkNS, Pulse) ->
    NS = <<"_event_sinks">>,
    MachinesStorage = add_storage_metrics(NS, machines, sub_storage_options(<<"machines">>, Storage)),
    EventsStorage = add_storage_metrics(NS, events, sub_storage_options(<<"events">>, Storage)),
    EventSinkNS#{
        namespace => NS,
        pulse => Pulse,
        storage => MachinesStorage,
        events_storage => EventsStorage,
        worker => worker_manager_options(EventSinkNS)
    }.

-spec worker_manager_options(map()) -> mg_core_workers_manager:options().
worker_manager_options(Config) ->
    maps:merge(
        #{
            registry => mg_core_procreg_gproc,
            sidecar => machinegun_hay
        },
        maps:get(worker, Config, #{})
    ).

-spec tags_options(mg_core:ns(), events_machines(), pulse()) -> mg_core_machine_tags:options().
tags_options(NS, #{retries := Retries, storage := Storage} = Config, Pulse) ->
    TagsNS = mg_core_utils:concatenate_namespaces(NS, <<"tags">>),
    % по логике тут должен быть sub namespace, но его по историческим причинам нет
    TagsStorage = add_storage_metrics(TagsNS, tags, Storage),
    #{
        namespace => TagsNS,
        storage => TagsStorage,
        worker => worker_manager_options(Config),
        pulse => Pulse,
        retries => Retries
    }.

-spec processor(processor(), pulse()) -> mg_core_utils:mod_opts().
processor(Processor, Pulse) ->
    {mg_woody_api_processor, Processor#{event_handler => {mg_woody_api_event_handler, Pulse}}}.

-spec sub_storage_options(mg_core:ns(), mg_core_machine:storage_options()) -> mg_core_machine:storage_options().
sub_storage_options(SubNS, Storage0) ->
    Storage1 = mg_core_utils:separate_mod_opts(Storage0, #{}),
    Storage2 = add_bucket_postfix(SubNS, Storage1),
    Storage2.

-spec add_bucket_postfix(mg_core:ns(), mg_core_storage:options()) -> mg_core_storage:options().
add_bucket_postfix(_, {mg_core_storage_memory, _} = Storage) ->
    Storage;
add_bucket_postfix(SubNS, {mg_core_storage_riak, #{bucket := Bucket} = Options}) ->
    {mg_core_storage_riak, Options#{bucket := mg_core_utils:concatenate_namespaces(Bucket, SubNS)}}.

-spec modernizer_options(modernizer() | undefined, pulse()) -> #{modernizer => mg_core_events_modernizer:options()}.
modernizer_options(#{current_format_version := CurrentFormatVersion, handler := WoodyClient}, Pulse) ->
    #{
        modernizer => #{
            current_format_version => CurrentFormatVersion,
            handler => {mg_woody_api_modernizer, WoodyClient#{event_handler => {mg_woody_api_event_handler, Pulse}}}
        }
    };
modernizer_options(undefined, _Pulse) ->
    #{}.

-spec add_storage_metrics(mg_core:ns(), _Type, mg_core_machine:storage_options()) -> mg_core_machine:storage_options().
add_storage_metrics(NS, Type, Storage0) ->
    Storage1 = mg_core_utils:separate_mod_opts(Storage0, #{}),
    do_add_storage_metrics(NS, Type, Storage1).

-spec do_add_storage_metrics(mg_core:ns(), atom(), mg_core_machine:storage_options()) ->
    mg_core_machine:storage_options().
do_add_storage_metrics(_NS, _Type, {mg_core_storage_memory, _} = Storage) ->
    Storage;
do_add_storage_metrics(NS, Type, {mg_core_storage_riak, Options}) ->
    PoolOptions = maps:get(pool_options, Options, #{}),
    NewOptions = Options#{
        sidecar =>
            {machinegun_riak_metric, #{
                namespace => NS,
                type => Type
            }},
        pool_options => PoolOptions#{
            metrics_mod => machinegun_riak_metric,
            metrics_api => exometer
        }
    },
    {mg_core_storage_riak, NewOptions}.
