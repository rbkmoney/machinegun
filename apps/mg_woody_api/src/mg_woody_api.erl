%%%
%%% Про woody context.
%%% В процеессе обработки process_signal новый, т.к. нет вызывающего запроса,
%%% в process_call же вызывающий запрос есть и контекст берётся от него.
%%%
-module(mg_woody_api).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export_type([ns        /0]).
-export_type([id        /0]).
-export_type([tag       /0]).
-export_type([args      /0]).
-export_type([event_id  /0]).
-export_type([event_body/0]).

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
-type ns        () :: binary().
-type id        () :: binary().
-type tag       () :: binary().
-type args      () :: binary().
-type event_id  () :: binary().
-type event_body() :: binary().

%% config
-type config_ns() :: #{
    url        => _URL,
    event_sink => mg_event_sink:id()
}.
-type config_nss() :: #{ns() => config_ns()}.
-type config_element() ::
      {namespaces,      config_nss   ()}
    | {api_host  , inet:ip_address   ()}
    | {api_port  , inet:port_number  ()}
    | {net_opts  , []                  } % в вуди нет для этого типа :(
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
        [mg_machine:child_spec(NS, ns_options(NS, ConfigNS, Storage)) || {NS, ConfigNS} <- maps:to_list(ConfigNSs)]
        ++
        [mg_event_sink:child_spec(event_sink_options(Storage), EventSinkID, EventSinkID) || EventSinkID <- EventSinks]
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
    woody_server:child_spec(
        ChildID,
        #{
            ip            => get_config_element(host    , Config, {0, 0, 0, 0}),
            port          => get_config_element(port    , Config, 8022        ),
            net_opts      => get_config_element(net_opts, Config, []          ),
            event_handler => mg_woody_api_event_handler,
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
    mg_machine:options().
ns_options(NS, #{url:=URL, event_sink:=EventSinkID}, Storage) ->
    #{
        namespace => NS,
        storage   => Storage,
        processor => {mg_woody_api_processor, URL},
        observer  => {mg_event_sink, {event_sink_options(Storage), NS, EventSinkID}}
    };
ns_options(NS, #{url:=URL}, Storage) ->
    #{
        namespace => NS,
        storage   => Storage,
        processor => {mg_woody_api_processor, URL}
    }.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    event_sink_options(get_config_element(storage, Config)).

-spec event_sink_options(mg_storage:storage()) ->
    mg_event_sink:options().
event_sink_options(Storage) ->
    #{
        storage => Storage
    }.

-spec collect_event_sinks(config_nss()) ->
    [mg_event_sink:id()].
collect_event_sinks(ConfigNSs) ->
    maps:fold(
        fun
            (_, #{event_sink:=EventSinkID}, Acc) ->
                [EventSinkID|Acc];
            (_, _, Acc) ->
                Acc
        end,
        [],
        ConfigNSs
    ).

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
