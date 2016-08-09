-module(mg).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% Types
-export_type([ns/0]).

-export_type([id      /0]).
-export_type([tag     /0]).
-export_type([args    /0]).
-export_type([history /0]).
-export_type([timer   /0]).
-export_type([ref     /0]).
-export_type([signal  /0]).

-export_type([event_id     /0]).
-export_type([event        /0]).
-export_type([event_body   /0]).
-export_type([events_bodies/0]).

-export_type([tag_action      /0]).
-export_type([set_timer_action/0]).
-export_type([complex_action  /0]).

-export_type([signal_args  /0]).
-export_type([call_args    /0]).
-export_type([signal_result/0]).
-export_type([call_result  /0]).

-export_type([history_range/0]).

-export_type([config/0]).

-type ns      () :: mg_proto_base_thrift            :'Namespace'().
-type id      () :: mg_proto_base_thrift            :'ID'       ().
-type tag     () :: mg_proto_state_processing_thrift:'Tag'      ().
-type args    () :: mg_proto_state_processing_thrift:'Args'     ().
-type history () :: mg_proto_state_processing_thrift:'History'  ().
-type timer   () :: mg_proto_state_processing_thrift:'Timer'    ().
-type ref     () :: mg_proto_state_processing_thrift:'Reference'().
-type signal  () :: mg_proto_state_processing_thrift:'Signal'   ().

-type event_id     () :: mg_proto_base_thrift            :'EventID'    ().
-type event        () :: mg_proto_state_processing_thrift:'Event'      ().
-type event_body   () :: mg_proto_state_processing_thrift:'EventBody'  ().
-type events_bodies() :: mg_proto_state_processing_thrift:'EventBodies'().

-type tag_action      () :: mg_proto_state_processing_thrift:'TagAction'     ().
-type set_timer_action() :: mg_proto_state_processing_thrift:'SetTimerAction'().
-type complex_action  () :: mg_proto_state_processing_thrift:'ComplexAction' ().

-type signal_args  () :: mg_proto_state_processing_thrift:'SignalArgs'  ().
-type call_args    () :: mg_proto_state_processing_thrift:'CallArgs'    ().
-type signal_result() :: mg_proto_state_processing_thrift:'SignalResult'().
-type call_result  () :: mg_proto_state_processing_thrift:'CallResult'  ().

-type history_range() :: mg_proto_state_processing_thrift:'HistoryRange'().

%% config
-type config_ns() ::
    {ns(), _URL}
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
    ConfigNSs = get_config_nss(),
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags,
        mg_machines_child_specs(ConfigNSs)
        ++
        [mg_woody_api:child_spec(make_automaton_options(ConfigNSs))]
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
%% machines child specs
%%
-spec mg_machines_child_specs(config_nss()) ->
    [supervisor:child_spec()].
mg_machines_child_specs(ConfigNSs) ->
    [mg_machine_child_spec(ConfigNS) || ConfigNS <- ConfigNSs].

-spec mg_machine_child_spec(config_ns()) ->
    [supervisor:child_spec()].
mg_machine_child_spec(ConfigNS={NS, _}) ->
    mg_machine:child_spec(NS, make_ns_options(ConfigNS)).

%%
%% utils
%%
-spec make_automaton_options(config_nss()) ->
    mg_woody_api_automaton:options().
make_automaton_options(ConfigNSs) ->
    lists:foldl(
        fun(ConfigNS={NS, _}, Options) ->
            NSBin = NS,
            Options#{NSBin => make_ns_options(ConfigNS)}
        end,
        #{},
        ConfigNSs
    ).

-spec make_ns_options(config_ns()) ->
    mg_machine:options().
make_ns_options({NS, URL}) ->
    {{mg_woody_api_processor, URL}, {mg_db_test, NS}}.

-spec get_config_nss() ->
    config_nss().
get_config_nss() ->
    {ok, ConfigNSs} = application:get_env(?MODULE, nss),
    ConfigNSs.
