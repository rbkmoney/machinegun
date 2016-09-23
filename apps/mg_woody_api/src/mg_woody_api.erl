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
-type config_ns() :: {mg_woody_api:ns(), _URL}.
-type config_nss() :: [config_ns()].
-type config_element() ::
      {nss     ,      config_nss   ()}
    | {ip      , inet:ip_address   ()}
    | {port    , inet:port_number  ()}
    | {net_opts, []                  } % в вуди нет для этого типа :(
    | {storage , mg_storage:storage()}
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
    Config = application:get_all_env(?MODULE),
    ConfigNSs = get_config_element(nss    , Config),
    Storage   = get_config_element(storage, Config),
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags,
        [mg_machine:child_spec(NS, ns_options(ConfigNS, Storage)) || ConfigNS={NS, _} <- ConfigNSs]
        ++
        [
            mg_event_sink:child_spec(event_sink_options(Storage), event_sink),
            woody_child_spec(Config, woody_api)
        ]
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
    ConfigNSs = get_config_element(nss    , Config),
    Storage   = get_config_element(storage, Config),
    lists:foldl(
        fun(ConfigNS={NS, _}, Options) ->
            NSBin = NS,
            Options#{NSBin => ns_options(ConfigNS, Storage)}
        end,
        #{},
        ConfigNSs
    ).

-spec ns_options(config_ns(), mg_storage:storage()) ->
    mg_machine:options().
ns_options({NS, URL}, Storage) ->
    #{
        namespace => NS,
        storage   => Storage,
        processor => {mg_woody_api_processor, URL},
        observer  => {mg_event_sink, {event_sink_options(Storage), NS}}
    }.

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    event_sink_options(get_config_element(storage, Config)).

-spec event_sink_options(mg_storage:storage()) ->
    mg_event_sink:options().
event_sink_options(Storage) ->
    #{
        id        => <<"event_sink">>,
        namespace => <<"event_sink">>,
        storage   => Storage
    }.

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
