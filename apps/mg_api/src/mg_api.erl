-module(mg_api).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% Types
-export_type([config/0]).

%% config
-type config_ns() ::
    {mg_woody_api:ns(), _URL}
.
-type config_nss() :: [config_ns()].
-type config_element() ::
      {nss, config_nss()}
.
-type config() :: [config_element()].


%% API
-export([start/0]).
-export([stop /0]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

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
%% Supervisor callbacks
%%
-spec init([]) ->
    mg_utils:supervisor_ret().
init([]) ->
    Config = application:get_all_env(?MODULE),
    ConfigNSs = proplists:get_value(nss, Config),
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags,
        [mg_machine:child_spec(NS, ns_options(ConfigNS)) || ConfigNS={NS, _} <- ConfigNSs]
        ++
        [
            mg_event_sink:child_spec(event_sink_options(), event_sink),
            mg_woody_api:child_spec(api_options(ConfigNSs))
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
-define(db_mod, mg_db_test).

-spec ns_options(config_ns()) ->
    mg_machine:options().
ns_options({NS, URL}) ->
    #{
        db        => {?db_mod, erlang:binary_to_atom(NS, utf8)},
        processor => {mg_woody_api_processor, URL},
        observer  => {mg_event_sink, event_sink_options()}
    }.

-spec event_sink_options() ->
    mg_event_sink:options().
event_sink_options() ->
    {?db_mod, event_sink}.

-spec api_options(config_nss()) ->
    mg_woody_api:options().
api_options(ConfigNSs) ->
    #{
        automaton  => api_automaton_options(ConfigNSs),
        event_sink => api_event_sink_options()
    }.

-spec api_automaton_options(config_nss()) ->
    mg_woody_api_automaton:options().
api_automaton_options(ConfigNSs) ->
    lists:foldl(
        fun(ConfigNS={NS, _}, Options) ->
            NSBin = NS,
            Options#{NSBin => ns_options(ConfigNS)}
        end,
        #{},
        ConfigNSs
    ).

-spec api_event_sink_options() ->
    mg_woody_api_event_sink:options().
api_event_sink_options() ->
    event_sink_options().
