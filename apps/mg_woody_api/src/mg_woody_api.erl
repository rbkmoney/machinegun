%%%
%%% Главный модуль приложения.
%%% Тут из конфига строится дерево супервизоров и генерируются структуры с настройками.
%%%
-module(mg_woody_api).

%% API
-export([start/0]).
-export([stop /0]).

%% application callbacks
-behaviour(application).
-export([start/2]).
-export([stop /1]).

%%
%% API
%%
-type processor() :: mg_woody_api_processor:options().
-type woody_server_net_opts() :: list(tuple()).
-type woody_server() :: #{
    ip       => tuple(),
    port     => inet:port_number(),
    net_opts => woody_server_net_opts(),
    limits   => woody_server_thrift_http_handler:handler_limits()
}.
-type events_machines() :: #{
    processor       => processor(),
    storage         => mg_storage:storage(),
    event_sink      => mg:id(),
    retryings       => mg_machine:retrying_opt(),
    scheduled_tasks => mg_machine:scheduled_tasks_opt()
}.
-type event_sink_ns() :: #{
    storage                => mg_storage:storage(),
    duplicate_search_batch => mg_storage:index_limit()
}.
-type config_element() ::
      {woody_server , woody_server()                 }
    | {namespaces   , #{mg:ns() => events_machines()}}
    | {event_sink_ns, event_sink_ns()                }
    | {event_sinks  , [mg:id()]                      }
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
    mg_utils_supervisor_wrapper:start_link(
        {local, ?MODULE},
        #{strategy => rest_for_one},
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
-define(logger, mg_woody_api_logger).

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
    WoodyConfig = proplists:get_value(woody_server, Config),
    woody_server:child_spec(
        ChildID,
        #{
            protocol       => thrift,
            transport      => http,
            ip             => maps:get(ip      , WoodyConfig),
            port           => maps:get(port    , WoodyConfig),
            net_opts       => maps:get(net_opts, WoodyConfig),
            event_handler  => {mg_woody_api_event_handler, server},
            handler_limits => maps:get(limits  , WoodyConfig),
            handlers       => [
                mg_woody_api_automaton :handler(api_automaton_options (Config)),
                mg_woody_api_event_sink:handler(api_event_sink_options(Config))
            ]
        }
    ).

-spec api_automaton_options(config()) ->
    mg_woody_api_automaton:options().
api_automaton_options(Config) ->
    NSs         = proplists:get_value(namespaces   , Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{NS => events_machine_options(NS, ConfigNS, EventSinkNS)}
        end,
        #{},
        NSs
    ).

-spec events_machine_options(mg:ns(), events_machines(), mg_events_sink:options()) ->
    mg_events_machine:options().
events_machine_options(NS, Config = #{processor := ProcessorConfig, storage := Storage}, EventSinkNS) ->
    EventSinkOptions = event_sink_options(EventSinkNS),
    events_machine_options_event_sink(
        EventSinkOptions,
        Config#{
            namespace      => NS,
            processor      => processor(ProcessorConfig),
            logger         => ?logger,
            tagging        => tags_options(NS, Storage),
            events_storage => Storage
        }
    ).

-spec tags_options(mg:ns(), mg_storage:storage()) ->
    mg_machine_tags:options().
tags_options(NS, Storage) ->
    #{
        namespace => NS,
        storage   => Storage,
        logger    => ?logger
    }.

-spec events_machine_options_event_sink(mg_events_sink:options(), mg_events_machine:options()) ->
    mg_events_machine:options().
events_machine_options_event_sink(EventSinkOptions, Options = #{event_sink := EventSinkID}) ->
    Options#{
        event_sink => {EventSinkID, EventSinkOptions}
    };
events_machine_options_event_sink(_, Options) ->
    Options.

-spec processor(processor()) ->
    mg_utils:mod_opts().
processor(Processor) ->
    {mg_woody_api_processor, Processor#{event_handler => mg_woody_api_event_handler}}.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    EventSinks  = collect_event_sinks(Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    {EventSinks, event_sink_options(EventSinkNS)}.

-spec event_sink_options(event_sink_ns()) ->
    mg_events_sink:options().
event_sink_options(EventSinkNS) ->
    EventSinkNS#{
        namespace => <<"_event_sinks">>,
        logger    => ?logger
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
