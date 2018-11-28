#!/usr/bin/env escript
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
        {statsderl   , statsderl   (YamlConfig)},
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

statsderl(YamlConfig) ->
    [
        {hostname, ?C:utf_bin(?C:conf([metrics, host], YamlConfig, "localhost"))},
        {port, ?C:conf([metrics, port], YamlConfig, "8125")},
        {pool_size, ?C:conf([metrics, pool_size], YamlConfig, 4)}
    ].

snowflake(YamlConfig) ->
    [{machine_id, ?C:conf([snowflake_machine_id], YamlConfig, 0)}].

mg_woody_api(YamlConfig) ->
    [
        {woody_server   , woody_server   (YamlConfig)},
        {health_checkers, health_checkers(YamlConfig)},
        {namespaces     , namespaces     (YamlConfig)},
        {event_sink_ns  , event_sink_ns  (YamlConfig)}
    ].

woody_server(YamlConfig) ->
    #{
        ip       => ?C:ip(?C:conf([woody_server, ip], YamlConfig, "::")),
        port     => ?C:conf([woody_server, port], YamlConfig, 8022),
        transport_opts => [
            % same as ranch defaults
            {max_connections, ?C:conf([woody_server, max_concurrent_connections], YamlConfig, 1024)}
        ],
        protocol_opts => [
            {timeout, ?C:time_interval(?C:conf([woody_server, keep_alive_timeout], YamlConfig, "5S"), 'ms')}
        ],
        limits   => genlib_map:compact(#{
            max_heap_size       => ?C:mem_words(?C:conf([limits, process_heap], YamlConfig, undefined)),
            total_mem_threshold => absolute_memory_limit(YamlConfig)
        })
    }.

health_checkers(YamlConfig) ->
    case ?C:conf([limits, disk], YamlConfig, undefined) of
        undefined ->
            [];
        DiskConfig ->
            [{erl_health, disk, [?C:conf([path], DiskConfig, "/"), percent(?C:conf([value], DiskConfig))]}]
    end ++
    case relative_memory_limit(YamlConfig) of
        undefined ->
            [];
        {TypeStr, Limit} ->
            Type =
                case TypeStr of
                    "total"   -> total;
                    "cgroups" -> cg_memory
                end,
            [{erl_health, Type, [Limit]}]
    end ++
    [{erl_health, service, [?C:utf_bin(?C:conf([service_name], YamlConfig))]}].

percent(Value) ->
    [$%|RevInt] = lists:reverse(Value),
    erlang:list_to_integer(lists:reverse(RevInt)).

relative_memory_limit(YamlConfig) ->
    case ?C:conf([limits, memory], YamlConfig, undefined) of
        undefined    -> undefined;
        MemoryConfig -> {?C:conf([type], MemoryConfig, "total"), percent(?C:conf([value], MemoryConfig))}
    end.

absolute_memory_limit(YamlConfig) ->
    {ok, _} = application:ensure_all_started(cg_mon),
    {ok, _} = application:ensure_all_started(os_mon),
    case relative_memory_limit(YamlConfig) of
        undefined ->
            undefined;
        {Type, RelativeLimit} ->
            CGMemLimit = wait_value(fun() -> memory_amount(Type) end, 1000, 10, memory_limit),
            RelativeLimit * CGMemLimit div 100
    end.

memory_amount("cgroups") -> cg_mem_sup:limit();
memory_amount("total"  ) -> proplist:get_value(memsup:get_system_memory_data()).

wait_value(_, 0, _, Key) ->
    exit({failed_fetch, Key});
wait_value(Fun, Timeout, Interval, Key) ->
    case Fun() of
        undefined ->
            timer:sleep(Interval),
            wait_value(Fun, erlang:max(0, Timeout - Interval), Interval, Key);
        Value ->
            Value
    end.

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
    Timeout = fun(Name, Default) ->
        ?C:time_interval(?C:conf([Name], NSYamlConfig, Default), ms)
    end,
    NS0 = #{
            storage   => storage(Name, YamlConfig),
            processor => #{
                url            => ?C:utf_bin(?C:conf([processor, url], NSYamlConfig)),
                transport_opts => [
                    {pool, erlang:list_to_atom(NameStr)},
                    {max_connections, ?C:conf([processor, pool_size], NSYamlConfig, 50)}
                ]
            },
            default_processing_timeout => Timeout(default_processing_timeout, "30S"),
            timer_processing_timeout => Timeout(timer_processing_timeout, "60S"),
            reschedule_timeout => Timeout(reschedule_timeout, "60S"),
            retries => #{
                storage   => {exponential, infinity, 2, 10, 60 * 1000},
                %% max_total_timeout not supported for timers yet, see mg_retry:new_strategy/2 comments
                %% actual timers sheduling resolution is one second
                timers    => {exponential, 100, 2, 1000, 30 * 60 * 1000},
                processor => {exponential, {max_total_timeout, 24 * 60 * 60 * 1000}, 2, 10, 60 * 1000}
            },
            scheduled_tasks => #{
                timers         => #{ interval => 1000, limit => 10 }, % | disable
                timers_retries => #{ interval => 1000, limit => 10 }, % | disable
                overseer       => #{ interval => 1000, limit => 10 } % | disable
            },
            suicide_probability => ?C:probability(?C:conf([suicide_probability], NSYamlConfig, 0))
        },
    NS1 =
        case ?C:conf([event_sink], NSYamlConfig, undefined) of
            undefined -> NS0;
            EventSink -> NS0#{event_sink => ?C:utf_bin(EventSink)}
        end,
    {Name, NS1}.

event_sink_ns(YamlConfig) ->
    #{
        storage                    => storage(<<"_event_sinks">>, YamlConfig),
        duplicate_search_batch     => 1000,
        default_processing_timeout => ?C:time_interval("30S", ms)
    }.

%%
%% vm.args
%%
vm_args(YamlConfig, ERLInetrcFilename) ->
    [
        {'-sname'    , ?C:utf_bin(?C:conf([service_name  ], YamlConfig, "machinegun"))},
        {'-setcookie', ?C:utf_bin(?C:conf([erlang, cookie], YamlConfig, "mg_cookie" ))},
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
