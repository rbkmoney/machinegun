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
% упс, а вот и протечка абстракции.
% в woody этот тип не экспортируется, а хочется
-type woody_server_net_opts() :: cowboy_protocol:opts().
-type woody_server() :: #{
    ip       => tuple(),
    port     => inet:port_number(),
    net_opts => woody_server_net_opts(),
    limits   => woody_server_thrift_http_handler:handler_limits()
}.
-type events_machines() :: #{
    processor           := processor(),
    storage             := mg_storage:options(),
    event_sink          => mg:id(),
    retries             := mg_machine:retry_opt(),
    scheduled_tasks     := mg_machine:scheduled_tasks_opt(),
    suicide_probability => mg_machine:suicide_probability(),
    raft                := raft_options()
}.
-type event_sink_ns() :: #{
    storage                := mg_storage:options(),
    duplicate_search_batch := mg_storage:index_limit(),
    raft                   := raft_options()
}.
-type raft_options() :: #{
    self              := raft_rpc:endpoint(),
    cluster           := ordsets:ordset(raft_rpc:endpoint()),
    election_timeout  := raft:timeout_ms() | {raft:timeout_ms(), raft:timeout_ms()},
    broadcast_timeout := raft:timeout_ms(),
    storage           := raft_storage:storage(),
    rpc               := raft_rpc_erl,
    logger            := raft_logger:logger()
}.
-type config_element() ::
      {woody_server , woody_server()                 }
    | {namespaces   , #{mg:ns() => events_machines()}}
    | {event_sink_ns, event_sink_ns()                }
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

-spec events_machine_options(mg:ns(), events_machines(), event_sink_ns()) ->
    mg_events_machine:options().
events_machine_options(NS, Config = #{processor := ProcessorConfig, storage := Storage}, EventSinkNS) ->
    EventSinkOptions = event_sink_options(EventSinkNS),
    events_machine_options_event_sink(
        maps:get(event_sink, Config, undefined),
        EventSinkOptions,
        #{
            namespace        => NS,
            processor        => processor(ProcessorConfig),
            tagging          => tags_options(NS, Config),
            machines         => machine_options(NS, Config),
            events_storage   => add_bucket_postfix(<<"events">>, Storage)
        }
    ).

-spec tags_options(mg:ns(), events_machines()) ->
    mg_machine_tags:options().
tags_options(NS, #{retries := Retries, storage := Storage, raft := Raft}) ->
    #{
        namespace => mg_utils:concatenate_namespaces(NS, <<"tags">>),
        storage   => Storage, % по логике тут должен быть sub namespace, но его по историческим причинам нет
        logger    => logger({machine_tags, NS}),
        retries   => Retries,
        raft      => Raft
    }.

-spec machine_options(mg:ns(), events_machines()) ->
    mg_machine:options().
machine_options(NS, Config) ->
    #{retries := Retries, scheduled_tasks := STasks, storage := Storage, raft := Raft} = Config,
    #{
        namespace           => NS,
        storage             => add_bucket_postfix(<<"machines">>, Storage),
        logger              => logger({machine, NS}),
        retries             => Retries,
        scheduled_tasks     => STasks,
        % TODO сделать аналогично в event_sink'е и тэгах
        suicide_probability => maps:get(suicide_probability, Config, undefined),
        raft                => Raft
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

-spec api_event_sink_options(config()) ->
    mg_woody_api_event_sink:options().
api_event_sink_options(Config) ->
    EventSinks  = collect_event_sinks(Config),
    EventSinkNS = proplists:get_value(event_sink_ns, Config),
    {EventSinks, event_sink_options(EventSinkNS)}.

-spec event_sink_options(event_sink_ns()) ->
    mg_events_sink:options().
event_sink_options(EventSinkNS = #{storage := Storage, raft := Raft}) ->
    EventSinkNS#{
        namespace      => <<"_event_sinks">>,
        logger         => logger(event_sink),
        storage        => add_bucket_postfix(<<"machines">>, Storage),
        events_storage => add_bucket_postfix(<<"events"  >>, Storage),
        raft           => Raft
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

-spec logger(mg_woody_api_logger:subj()) ->
    mg_machine_logger:handler().
logger(Subj) ->
    {mg_woody_api_logger, Subj}.
