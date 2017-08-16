#!/usr/bin/env escript

-define(C, mg_woody_api_configurator).

%%
%% main
%%
main([YamlConfigFilename, ConfigsPath]) ->
    YamlConfig = ?C:parse_yaml_config(YamlConfigFilename),
    ERLInetrcFilename = filename:join(ConfigsPath, "erl_inetrc"),
    ?C:write_files([
        {filename:join(ConfigsPath, "sys.config"), ?C:print_sys_config(sys_config(YamlConfig                   ))},
        {filename:join(ConfigsPath, "vm.args"   ), ?C:print_vm_args   (vm_args   (YamlConfig, ERLInetrcFilename))},
        {ERLInetrcFilename                       , ?C:print_erl_inetrc(erl_inetrc(YamlConfig                   ))}
    ]).

%%
%% sys.config
%%
sys_config(YamlConfig) ->
    [
        {lager       , lager       (YamlConfig)},
        {snowflake   , snowflake   (YamlConfig)},
        {mg_woody_api, mg_woody_api(YamlConfig)}
    ].

lager(YamlConfig) ->
    [
        {error_logger_hwm, 600},
        {log_root , ?C:filename(?C:conf([logging, root     ], YamlConfig, "/var/log/machinegun"))},
        {crash_log, ?C:filename(?C:conf([logging, crash_log], YamlConfig, "crash.json"         ))},
        {handlers, [
            {lager_file_backend, [
                {file     , ?C:filename (?C:conf([logging, json_log], YamlConfig, "log.json"))},
                {level    , ?C:log_level(?C:conf([logging, level   ], YamlConfig, "info"    ))},
                {formatter, lager_logstash_formatter},
                %% disable log rotation
                {size, 0},
                {date, ""}
            ]}
        ]}
    ].

snowflake(YamlConfig) ->
    [{machine_id, ?C:conf([snowflake_machine_id], YamlConfig, 0)}].

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
        net_opts => [
            {timeout, ?C:time_interval(?C:conf([woody_server, keep_alive_timeout], YamlConfig, "5S"), 'ms')}
        ],
        limits   => genlib_map:compact(#{
            max_heap_size       => ?C:mem_words(?C:conf([limits, process_mem], YamlConfig, undefined)),
            total_mem_threshold => ?C:mem_bytes(?C:conf([limits, total_mem  ], YamlConfig, undefined))
        })
    }.

storage(NS, YamlConfig) ->
    case ?C:conf([storage, type], YamlConfig) of
        "memory" ->
            mg_storage_memory;
        "riak" ->
            {mg_storage_pool, #{
                worker =>
                    {mg_storage_riak, #{
                        host   => ?C:utf_bin(?C:conf([storage, host], YamlConfig)),
                        port   =>            ?C:conf([storage, port], YamlConfig),
                        bucket => NS,
                        connect_timeout => ?C:time_interval(?C:conf([storage, connect_timeout  ], YamlConfig, "5S" ), ms),
                        request_timeout => ?C:time_interval(?C:conf([storage, request_timeout  ], YamlConfig, "10S"), ms)
                    }},
                size => ?C:conf([storage, pool_size], YamlConfig, 100),
                queue_len_limit => 10,
                retry_attempts  => 10
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
            storage    => storage(Name, YamlConfig),
            processor  => #{
                url            => ?C:utf_bin(?C:conf([processor, url], NSYamlConfig)),
                transport_opts => [
                    {pool, erlang:list_to_atom(NameStr)},
                    {max_connections, ?C:conf([processor, pool_size], NSYamlConfig, 50)},
                    {recv_timeout, ?C:time_interval(?C:conf([processor, recv_timeout], NSYamlConfig, "5S"), ms)}
                ]
            },
            retries => #{
                storage   => {exponential, infinity           , 2, 10, 60 * 1000},
                processor => {exponential, 24 * 60 * 60 * 1000, 2, 10, 60 * 1000}
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
        storage                => storage(<<"_event_sinks">>, YamlConfig),
        duplicate_search_batch => 1000
    }.

%%
%% vm.args
%%
vm_args(YamlConfig, ERLInetrcFilename) ->
    [
        {'-sname'    , ?C:utf_bin(?C:conf([erlang, sname ], YamlConfig, "mg"       ))},
        {'-setcookie', ?C:utf_bin(?C:conf([erlang, cookie], YamlConfig, "mg_cookie"))},
        {'+K'        , <<"true">>},
        {'+A'        , <<"10">>  },
        {'-kernel'   , <<"inetrc '\"", (?C:utf_bin(ERLInetrcFilename))/binary, "\"'">>}
    ].

%%
%% erl_inetrc
%%
erl_inetrc(YamlConfig) ->
    conf_if([erlang, ipv6             ], YamlConfig, [{inet6, true}, {tcp, inet6_tcp}]) ++
    conf_if([erlang, disable_dns_cache], YamlConfig, [{cache_size, 0}                ]).

conf_if(YamlConfigPath, YamlConfig, Value) ->
    case ?C:conf(YamlConfigPath, YamlConfig, false) of
        true  -> Value;
        false -> []
    end.
