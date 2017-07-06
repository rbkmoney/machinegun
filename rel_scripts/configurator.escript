#!/usr/bin/env escript

-define(C, mg_woody_api_configurator).

%%
%% main
%%
main([YamlConfigFilename, SysConfigFilename, VMArgsFilename]) ->
    YamlConfig = ?C:parse_yaml_config(YamlConfigFilename),
    % io:format("Yaml config: ~p~n", [YamlConfig]),
    ?C:write_files([
        {SysConfigFilename, ?C:print_sys_config(sys_config(YamlConfig))},
        {VMArgsFilename   , ?C:print_vm_args   (vm_args   (YamlConfig))}
    ]).

%%
%% sys.config
%%
sys_config(YamlConfig) ->
    [
        {lager       , lager       (YamlConfig)},
        {mg_woody_api, mg_woody_api(YamlConfig)}
    ].

lager(YamlConfig) ->
    [
        {log_root , ?C:filename(?C:conf([logging, root     ], YamlConfig, "/var/log/machinegun"))},
        {crash_log, ?C:filename(?C:conf([logging, crash_log], YamlConfig, "crash.json"         ))},
        {handlers, [
            {lager_file_backend, [
                {file     , ?C:filename (?C:conf([logging, json_log], YamlConfig, "log.json"))},
                {level    , ?C:log_level(?C:conf([logging, level   ], YamlConfig, "info"    ))},
                {formatter, lager_logstash_formatter}
            ]}
        ]}
    ].

mg_woody_api(YamlConfig) ->
    [
        {woody_server , woody_server (YamlConfig)},
        {namespaces   , namespaces   (YamlConfig)},
        {event_sink_ns, event_sink_ns(YamlConfig)}
    ].

woody_server(YamlConfig) ->
    #{
        ip       => ?C:ip(?C:conf([woody_server, ip], YamlConfig, "::")),
        port     => ?C:conf([woody_server, port], YamlConfig, 8022),
        net_opts => [],
        limits   => #{
            max_heap_size       => ?C:mem_words(?C:conf([limits, process_mem], YamlConfig, undefined)),
            total_mem_threshold => ?C:mem_bytes(?C:conf([limits, total_mem  ], YamlConfig, undefined))
        }
    }.

storage(YamlConfig) ->
    case ?C:conf([storage, type], YamlConfig) of
        "memory" ->
            mg_storage_memory;
        "riak" ->
            {mg_storage_riak, #{
                host => ?C:conf([storage, host], YamlConfig),
                port => ?C:conf([storage, port], YamlConfig),
                pool => #{
                    init_count    => ?C:conf([storage, pool_size], YamlConfig, 100),
                    max_count     => ?C:conf([storage, pool_size], YamlConfig, 100),
                    cull_interval => {0, min}
                },
                pool_take_timeout => {5, 'sec'},
                connect_timeout   => 5000,
                request_timeout   => 10000
            }}
    end.

namespaces(YamlConfig) ->
    lists:foldl(
        fun(NSConfig, Acc) ->
            {Name, NS} = namespace(NSConfig, YamlConfig),
            Acc#{Name => NS}
        end,
        #{},
        ?C:conf([namespaces], YamlConfig)
    ).

namespace({NameStr, NSYamlConfig}, YamlConfig) ->
    Name = ?C:utf_bin(NameStr),
    NS0 = #{
            storage => storage(YamlConfig),
            processor  => #{
                url            => ?C:utf_bin(?C:conf([processor], NSYamlConfig)),
                transport_opts => [{pool, erlang:list_to_atom(NameStr)}, {max_connections, 100}, {recv_timeout, 60000}]
            },
            retryings => #{
                storage   => {exponential, infinity, 2, 10, 60000},
                processor => {exponential, 86400000, 2, 10, 60000}  % 24h
            },
            scheduled_tasks => #{
                timers   => #{ interval => 1000, limit => 10 }, % | disable
                overseer => #{ interval => 1000, limit => 10 } % | disable
            }
        },
    NS1 =
        case ?C:conf([event_sink], NSYamlConfig, undefined) of
            undefined -> NS0;
            EventSink -> NS0#{event_sink => ?C:utf_bin(EventSink)}
        end,
    {Name, NS1}.

event_sink_ns(YamlConfig) ->
    #{
        storage                => storage(YamlConfig),
        duplicate_search_batch => 1000
    }.

%%
%% vm.args
%%
vm_args(_YamlConfig) ->
    [
        {'-sname'    , <<"mg">>       },
        {'-setcookie', <<"mg_cookie">>},
        {'+K'        , <<"true">>     },
        {'+A'        , <<"10">>       }
    ].
