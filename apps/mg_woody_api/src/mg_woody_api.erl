%%%
%%% Про woody context.
%%% В процеессе обработки process_signal новый, т.к. нет вызывающего запроса,
%%% в process_call же вызывающий запрос есть и контекст берётся от него.
%%%
-module(mg_woody_api).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([config/0]).

-export([start/0]).
-export([stop /0]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

%% application callbacks
-behaviour(application).
-export([start/2]).
-export([stop /1]).

%% API

%% config
-type processor_config() :: mg_woody_api_processor:options().
-type config_ns() :: #{
    processor  => processor_config(),
    event_sink => mg:id()
}.
-type config_nss() :: #{mg:ns() => config_ns()}.
-type net_opts() :: woody_server_thrift_http_handler:net_opts().
-type config_element() ::
      {namespaces,         config_nss()}
    | {ip        ,             string()}
    | {port      ,   inet:port_number()}
    | {net_opts  ,           net_opts()}
    | {storage   , mg_storage:storage()}
.
-type config() :: [config_element()].

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
%% Supervisor callbacks
%%
-spec init([]) ->
    mg_utils:supervisor_ret().
init([]) ->
    Config     = application:get_all_env(?MODULE),
    ConfigNSs  = get_config_element(namespaces, Config),
    Storage    = get_config_element(storage   , Config),
    EventSinks = collect_event_sinks(ConfigNSs),
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags,
        [
            mg_events_machine:child_spec(ns_options(NS, ConfigNS, Storage), NS)
            || {NS, ConfigNS} <- maps:to_list(ConfigNSs)
        ]
        ++
        [mg_events_sink:child_spec(event_sink_options(Storage), ESID) || ESID <- EventSinks]
        ++
        [woody_child_spec(Config, woody_api)]
    }}.

%%
%% Application callbacks
%%
-spec start(normal, any()) ->
    {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) ->
    ok.
stop(_State) ->
    ok.

%%
%% local
%%
-spec woody_child_spec(config(), atom()) ->
    supervisor:child_spec().
woody_child_spec(Config, ChildID) ->
    {ok, Ip} = inet:parse_address(get_config_element(ip, Config, "::")),
    woody_server:child_spec(
        ChildID,
        #{
            protocol      => thrift,
            transport     => http,
            ip            => Ip,
            port          => get_config_element(port    , Config, 8022),
            net_opts      => get_config_element(net_opts, Config, #{} ),
            event_handler => {mg_woody_api_event_handler, server},
            handlers      => [
                mg_woody_api_automaton :handler(api_automaton_options (Config)),
                mg_woody_api_event_sink:handler(api_event_sink_options(Config))
            ]
        }
    ).

-spec api_automaton_options(config()) ->
    mg_woody_api_automaton:options().
api_automaton_options(Config) ->
    ConfigNSs = get_config_element(namespaces, Config),
    Storage   = get_config_element(storage   , Config),
    maps:fold(
        fun(NS, ConfigNS, Options) ->
            Options#{NS => ns_options(NS, ConfigNS, Storage)}
        end,
        #{},
        ConfigNSs
    ).

-spec ns_options(mg:ns(), config_ns(), mg_storage:storage()) ->
    mg_events_machine:options().
ns_options(NS, #{processor:=ProcessorConfig, event_sink:=EventSinkID}, Storage) ->
    #{
        namespace  => NS,
        storage    => Storage,
        processor  => processor(ProcessorConfig),
        tagging    => tags_options(NS, Storage),
        event_sink => {EventSinkID, event_sink_options(Storage)}
    };
ns_options(NS, #{processor:=ProcessorConfig}, Storage) ->
    #{
        namespace => NS,
        storage   => Storage,
        processor => processor(ProcessorConfig),
        tagging   => tags_options(NS, Storage)
    }.

-spec processor(processor_config()) ->
    mg_utils:mod_opts().
processor(ProcessorConfig) ->
    {mg_woody_api_processor, ProcessorConfig#{event_handler => mg_woody_api_event_handler}}.

-spec tags_options(mg:ns(), mg_storage:storage()) ->
    mg_machine_tags:options().
tags_options(NS, Storage) ->
    #{
        namespace => NS,
        storage   => Storage
    }.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    {
        collect_event_sinks(get_config_element(namespaces, Config)),
        event_sink_options(get_config_element(storage, Config))
    }.

-spec event_sink_options(mg_storage:storage()) ->
    mg_events_sink:options().
event_sink_options(Storage) ->
    #{
        namespace => <<"_event_sinks">>,
        storage   => Storage
    }.

-spec collect_event_sinks(config_nss()) ->
    [mg:id()].
collect_event_sinks(ConfigNSs) ->
    ordsets:to_list(maps:fold(
        fun
            (_, #{event_sink:=EventSinkID}, Acc) ->
                ordsets:add_element(EventSinkID, Acc);
            (_, _, Acc) ->
                Acc
        end,
        ordsets:new(),
        ConfigNSs
    )).

%%
%% config utils
%%
-spec get_config_element(atom(), config()) ->
    term() | no_return().
get_config_element(Element, Config) ->
    case lists:keyfind(Element, 1, Config) of
        false ->
            erlang:throw({config_element_not_found, Element});
        {Element, Value} ->
            Value
    end.

-spec get_config_element(atom(), config(), term()) ->
    term().
get_config_element(Element, Config, Default) ->
    case lists:keyfind(Element, 1, Config) of
        false ->
            Default;
        {Element, Value} ->
            Value
    end.
