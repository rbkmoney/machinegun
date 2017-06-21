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
    Config = mg_woody_api_config:parse_env(application:get_all_env(?MODULE)),
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

-spec events_machines_child_specs(mg_woody_api_config:config()) ->
    [supervisor:child_spec()].
events_machines_child_specs(#{events_machines_nss := NSs, event_sink_ns := EventSinkNS}) ->
    [
        mg_events_machine:child_spec(events_machine_options(NS, ConfigNS, EventSinkNS), binary_to_atom(NS, utf8))
        || {NS, ConfigNS} <- maps:to_list(NSs)
    ].

-spec event_sink_ns_child_spec(mg_woody_api_config:config(), atom()) ->
    supervisor:child_spec().
event_sink_ns_child_spec(#{event_sink_ns := EventSinkNS}, ChildID) ->
    mg_events_sink:child_spec(event_sink_options(EventSinkNS), ChildID).

-spec woody_server_child_spec(mg_woody_api_config:config(), atom()) ->
    supervisor:child_spec().
woody_server_child_spec(Config = #{woody_server := WoodyConfig}, ChildID) ->
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

-spec api_automaton_options(mg_woody_api_config:config()) ->
    mg_woody_api_automaton:options().
api_automaton_options(#{events_machines_nss := NSs, event_sink_ns := EventSinkNS}) ->
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{NS => events_machine_options(NS, ConfigNS, EventSinkNS)}
        end,
        #{},
        NSs
    ).

-spec events_machine_options(mg:ns(), mg_woody_api_config:events_machines(), mg_events_sink:options()) ->
    mg_events_machine:options().
events_machine_options(NS, Config = #{processor := ProcessorConfig}, EventSinkNS) ->
    EventSinkOptions = event_sink_options(EventSinkNS),
    Storage = maps:get(storage, Config),
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

-spec processor(mg_woody_api_config:processor()) ->
    mg_utils:mod_opts().
processor(Processor) ->
    {mg_woody_api_processor, Processor#{event_handler => mg_woody_api_event_handler}}.

-spec api_event_sink_options(mg_woody_api_config:config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(#{event_sinks := EventSinks, event_sink_ns := EventSinkNS}) ->
    {EventSinks, event_sink_options(EventSinkNS)}.

-spec event_sink_options(mg_woody_api_config:event_sink_ns()) ->
    mg_events_sink:options().
event_sink_options(EventSinkNS) ->
    EventSinkNS#{
        namespace => <<"_event_sinks">>,
        logger    => ?logger
    }.
