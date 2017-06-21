%%%
%%% Модуль для работы с конфигом.
%%% Тут происходит маппинг грамматики конфига в граматику внутреннего представления
%%% для последующего чтения в других модулях.
%%% Вся валидация (и установка дефолтов?) должна происходит тут же.
%%%
-module(mg_woody_api_config).

%% API
-export_type([processor      /0]).
-export_type([woody_server   /1]).
-export_type([events_machines/0]).
-export_type([event_sink_ns  /0]).
-export_type([config         /0]).
-export_type([env            /0]).

-export([parse_env/1]).

%%
%% API
%%
%% application presentation
-type processor() :: mg_woody_api_processor:options().
-type woody_server_net_opts() :: list(tuple()).
-type woody_server(IP) :: #{
    ip       => IP,
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
-type config() :: #{
    woody_server        => woody_server(tuple()),
    events_machines_nss => #{mg:ns() => events_machines()},
    event_sink_ns       => event_sink_ns(),
    event_sinks         => [mg:id()]
}.

%% external (application environment) presentation
-type namespace() :: #{
    processor  => processor(),
    event_sink => mg:id()
}.
-type namespaces() :: #{mg:ns() => namespace()}.
-type env_element() ::
      {namespaces  , namespaces()          }
    | {woody_server, woody_server(string())}
    | {storage     , mg_storage:storage()  }
.
-type env() :: [env_element()].

%%

-spec parse_env(env()) ->
    config().
parse_env(Env) ->
    NSs        = get_env_element(namespaces, Env, #{}),
    Storage    = get_env_element(storage   , Env),
    EventSinks = collect_event_sinks(NSs),
    #{
        woody_server        => parse_woody_server(get_env_element(woody_server, Env, #{})),
        events_machines_nss => parse_namespaces(NSs, Storage),
        event_sink_ns       => event_sink_ns(Storage),
        event_sinks         => EventSinks
    }.

-spec parse_woody_server(woody_server(string())) ->
    woody_server(tuple()).
parse_woody_server(WoodyServer) ->
    #{
        ip       => mg_utils:throw_if_error(inet:parse_address(maps:get(ip, WoodyServer, "::"))),
        port     => maps:get(ip      , WoodyServer, 8022),
        limits   => maps:get(limits  , WoodyServer, #{} ),
        net_opts => maps:get(net_opts, WoodyServer, []  )
    }.
-spec parse_namespaces(namespaces(), mg_storage:storage()) ->
    #{mg:ns() => events_machines()}.
parse_namespaces(NSs, Storage) ->
    maps:map(
        fun(_, NS) ->
            parse_namespace(NS, Storage)
        end,
        NSs
    ).

-spec parse_namespace(namespace(), mg_storage:storage()) ->
    events_machines().
parse_namespace(Namespace = #{processor := Processor}, Storage) ->
    parse_namespace_add_event_sink(
        Namespace,
        #{
            processor       => Processor,
            storage         => Storage,
            retryings => #{
                storage   => {exponential, infinity, 2, 10, 60000},
                processor => {exponential, 86400000, 2, 10, 60000}  % 24h
            },
            scheduled_tasks => #{
                timers   => #{ interval => 1000, limit => 10 }, % | disable
                overseer => #{ interval => 1000, limit => 10 } % | disable
            }
        }
    ).

-spec parse_namespace_add_event_sink(namespace(), events_machines()) ->
    events_machines().
parse_namespace_add_event_sink(#{event_sink := EventSink}, EventsMachines) ->
    EventsMachines#{event_sink => EventSink};
parse_namespace_add_event_sink(#{}, EventsMachines) ->
    EventsMachines.

-spec event_sink_ns(mg_storage:storage()) ->
    event_sink_ns().
event_sink_ns(Storage) ->
    #{
        storage                => Storage,
        duplicate_search_batch => 1000
    }.

-spec collect_event_sinks(namespaces()) ->
    [mg:id()].
collect_event_sinks(NSs) ->
    ordsets:to_list(maps:fold(
        fun
            (_, #{event_sink:=EventSinkID}, Acc) ->
                ordsets:add_element(EventSinkID, Acc);
            (_, _, Acc) ->
                Acc
        end,
        ordsets:new(),
        NSs
    )).

%%
%% config utils
%%
-spec get_env_element(atom(), env()) ->
    term() | no_return().
get_env_element(Element, Config) ->
    case lists:keyfind(Element, 1, Config) of
        false ->
            erlang:throw({config_element_not_found, Element});
        {Element, Value} ->
            Value
    end.

-spec get_env_element(atom(), env(), term()) ->
    term().
get_env_element(Element, Config, Default) ->
    case lists:keyfind(Element, 1, Config) of
        false ->
            Default;
        {Element, Value} ->
            Value
    end.
