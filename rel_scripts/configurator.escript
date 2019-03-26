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
        {consuela    , consuela    (YamlConfig)},
        {how_are_you , how_are_you (YamlConfig)},
        {snowflake   , snowflake   (YamlConfig)},
        {brod        , brod        (YamlConfig)},
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

consuela(YamlConfig) ->
    lists:append(
        conf_with([consuela, registry], YamlConfig, fun (RegConfig) -> [
            {registry, #{
                nodename  => ?C:conf([nodename], RegConfig),
                namespace => ?C:utf_bin(?C:conf([namespace], RegConfig, "mg")),
                consul    => consul_client(mg_consuela_registry, YamlConfig),
                shutdown  => ?C:time_interval(?C:conf([shutdown_timeout], DiscoveryConfig, undefined), 'ms'),
                keeper    => #{
                    pulse => mg_consuela_pulse_adapter:pulse(session_keeper, mg_woody_api_pulse)
                },
                reaper    => #{
                    pulse => mg_consuela_pulse_adapter:pulse(zombie_reaper, mg_woody_api_pulse)
                },
                registry  => #{
                    pulse => mg_consuela_pulse_adapter:pulse(registry_server, mg_woody_api_pulse)
                }
            }}
        ] end),
        conf_with([consuela, discovery], YamlConfig, [], fun (DiscoveryConfig) -> [
            {discovery, #{
                name      => service_name(YamlConfig),
                tags      => [?C:utf_bin(T) || T <- ?C:conf([tags], DiscoveryConfig, [])],
                consul    => consul_client(mg_consuela_discovery, YamlConfig),
                opts      => #{
                    interval => genlib_map:compact(#{
                        init => ?C:time_interval(?C:conf([interval, init], DiscoveryConfig, undefined), 'ms'),
                        idle => ?C:time_interval(?C:conf([interval, idle], DiscoveryConfig, undefined), 'ms')
                    }),
                    pulse => mg_consuela_pulse_adapter:pulse(discovery_server, mg_woody_api_pulse)
                }
            }}
        ] end)
    ).

consul_client(Name, YamlConfig) ->
    #{
        url   => ?C:conf([consul, url], YamlConfig),
        opts  => genlib_map:compact(#{
            datacenter => ?C:conf([consul, datacenter], YamlConfig, undefined),
            acl        => ?C:conf([consul, acl_token ], YamlConfig, undefined),
            transport_opts => genlib_map:compact(#{
                pool =>
                    ?C:conf([consul, pool             ], YamlConfig, Name),
                max_connections =>
                    ?C:conf([consul, max_connections  ], YamlConfig, 8),
                max_response_size =>
                    ?C:conf([consul, max_response_size], YamlConfig, undefined),
                connect_timeout =>
                    ?C:time_interval(?C:conf([consul, connect_timeout], YamlConfig, undefined), 'ms'),
                recv_timeout =>
                    ?C:time_interval(?C:conf([consul, recv_timeout   ], YamlConfig, undefined), 'ms'),
                ssl_options =>
                    ?C:proplist(?C:conf([consul, ssl_options      ], YamlConfig, undefined)),
                pulse =>
                    mg_consuela_pulse_adapter:pulse(client, mg_woody_api_pulse)
            })
        })
    }.

how_are_you(YamlConfig) ->
    Publishers = hay_statsd_publisher(YamlConfig),
    [
        {metrics_publishers, Publishers},
        {metrics_handlers, [
            hay_vm_handler,
            hay_cgroup_handler,
            {mg_woody_api_hay, #{
                namespaces => namespaces_list(YamlConfig)
            }}
        ]}
    ].

hay_statsd_publisher(YamlConfig) ->
    conf_with([metrics, publisher, statsd], YamlConfig, [], fun (Config) -> [
        {hay_statsd_publisher, #{
            key_prefix => <<(?C:utf_bin(?C:conf([service_name], YamlConfig)))/binary, ".">>,
            host => ?C:utf_bin(?C:conf([host], Config, "localhost")),
            port => ?C:conf([port], Config, 8125),
            interval => 15000
        }}
    ] end).

snowflake(YamlConfig) ->
    [{machine_id, ?C:conf([snowflake_machine_id], YamlConfig, 0)}].

brod(YamlConfig) ->
    Clients = ?C:conf([kafka], YamlConfig, []),
    [
        {clients, [
            {?C:atom(Name), brod_client(ClientConfig)}
            || {Name, ClientConfig} <- Clients
        ]}
    ].

brod_client(ClientConfig) ->
    ProducerConfig = ?C:conf([producer], ClientConfig, []),
    [
        {endpoints, [
            {?C:conf([host], Endpoint), ?C:conf([port], Endpoint)}
            || Endpoint <- ?C:conf([endpoints], ClientConfig)
        ]},
        {restart_delay_seconds, 10},
        {auto_start_producers, true},
        {default_producer_config, [
            {topic_restart_delay_seconds, 10},
            {partition_restart_delay_seconds, 2},
            {partition_buffer_limit, ?C:conf([partition_buffer_limit], ProducerConfig, 256)},
            {partition_onwire_limit, ?C:conf([partition_onwire_limit], ProducerConfig, 1)},
            {max_batch_size, ?C:mem_bytes(?C:conf([max_batch_size], ProducerConfig, "1M"))},
            {max_retries, ?C:conf([max_retries], ProducerConfig, 3)},
            {retry_backoff_ms, ?C:time_interval(?C:conf([retry_backoff], ProducerConfig, "500ms"), ms)},
            {required_acks, ?C:atom(?C:conf([required_acks], ProducerConfig, "all_isr"))},
            {ack_timeout, ?C:time_interval(?C:conf([ack_timeout], ProducerConfig, "10s"), ms)},
            {compression, ?C:atom(?C:conf([compression], ProducerConfig, "no_compression"))},
            {max_linger_ms, ?C:time_interval(?C:conf([max_linger], ProducerConfig, "0ms"), ms)},
            {max_linger_count, ?C:conf([max_linger_count], ProducerConfig, 0)}
        ]},
        {ssl, brod_client_ssl(?C:conf([ssl], ClientConfig, false))}
    ].

brod_client_ssl(false) ->
    false;
brod_client_ssl(SslConfig) ->
    Opts = [
        {certfile, ?C:conf([certfile], SslConfig, undefined)},
        {keyfile, ?C:conf([keyfile], SslConfig, undefined)},
        {cacertfile, ?C:conf([cacertfile], SslConfig, undefined)}
    ],
    [Opt || Opt = {_Key, Value} <- Opts, Value =/= undefined].

mg_woody_api(YamlConfig) ->
    [
        {woody_server   , woody_server   (YamlConfig)},
        {health_checkers, health_checkers(YamlConfig)},
        {quotas         , quotas         (YamlConfig)},
        {namespaces     , namespaces     (YamlConfig)},
        {event_sink_ns  , event_sink_ns  (YamlConfig)}
    ].

woody_server(YamlConfig) ->
    #{
        ip       => ?C:ip(?C:conf([woody_server, ip], YamlConfig, "::")),
        port     => ?C:conf([woody_server, port], YamlConfig, 8022),
        transport_opts => #{
            % same as ranch defaults
            max_connections => ?C:conf([woody_server, max_concurrent_connections], YamlConfig, 1024)
        },
        protocol_opts => #{
            request_timeout => ?C:time_interval(?C:conf([woody_server, request_timeout], YamlConfig, "5S"), 'ms'),
            idle_timeout    => ?C:time_interval(?C:conf([woody_server, idle_timeout   ], YamlConfig, "5S"), 'ms')
        },
        limits   => genlib_map:compact(#{
            max_heap_size       => ?C:mem_words(?C:conf([limits, process_heap], YamlConfig, undefined)),
            total_mem_threshold => absolute_memory_limit(YamlConfig)
        })
    }.

health_checkers(YamlConfig) ->
    conf_with([limits, disk], YamlConfig, [], fun (DiskConfig) ->
        [{erl_health, disk, [?C:conf([path], DiskConfig, "/"), percent(?C:conf([value], DiskConfig))]}]
    end) ++
    relative_memory_limit(YamlConfig, [], fun ({TypeStr, Limit}) ->
        Type =
            case TypeStr of
                "total"   -> total;
                "cgroups" -> cg_memory
            end,
        [{erl_health, Type, [Limit]}]
    end) ++
    [{erl_health, service, [?C:utf_bin(?C:conf([service_name], YamlConfig))]}].

quotas(YamlConfig) ->
    SchedulerLimit = ?C:conf([limits, scheduler_tasks], YamlConfig, 5000),
    [
        #{
            name => <<"scheduler_tasks_total">>,
            limit => #{ value => SchedulerLimit },
            update_interval => 1000
        }
    ].

percent(Value) ->
    [$%|RevInt] = lists:reverse(Value),
    erlang:list_to_integer(lists:reverse(RevInt)).

relative_memory_limit(YamlConfig, Default, Fun) ->
    conf_with([limits, memory], YamlConfig, Default, fun (MemoryConfig) ->
        Fun({?C:conf([type], MemoryConfig, "total"), percent(?C:conf([value], MemoryConfig))})
    end).

absolute_memory_limit(YamlConfig) ->
    {ok, _} = application:ensure_all_started(cg_mon),
    {ok, _} = application:ensure_all_started(os_mon),
    relative_memory_limit(YamlConfig, undefined, fun({Type, RelativeLimit}) ->
        CGMemLimit = wait_value(fun() -> memory_amount(Type) end, 1000, 10, memory_limit),
        RelativeLimit * CGMemLimit div 100
    end).

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

namespaces_list(YamlConfig) ->
    NsNames = [
        erlang:list_to_binary(NameStr)
        || {NameStr, _NSYamlConfig} <- ?C:conf([namespaces], YamlConfig)
    ],
    lists:flatten([<<"_event_sinks_machines">>] ++ [
        [NS, <<NS/binary, "_tags">>]
        || NS <- NsNames
    ]).

namespace({NameStr, NSYamlConfig}, YamlConfig) ->
    Name = ?C:utf_bin(NameStr),
    Timeout = fun(TimeoutName, Default) ->
        ?C:time_interval(?C:conf([TimeoutName], NSYamlConfig, Default), ms)
    end,
    {Name, #{
        storage   => storage(Name, YamlConfig),
        processor => #{
            url            => ?C:utf_bin(?C:conf([processor, url], NSYamlConfig)),
            transport_opts => #{
                pool => erlang:list_to_atom(NameStr),
                max_connections => ?C:conf([processor, pool_size], NSYamlConfig, 50)
            }
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
        schedulers => #{
            timers         => #{
                interval     => 1000,
                limit        => <<"scheduler_tasks_total">>,
                share        => 2
            },
            timers_retries => #{
                interval     => 1000,
                limit        => <<"scheduler_tasks_total">>,
                share        => 1
            },
            overseer       => #{
                interval     => 1000,
                limit        => <<"scheduler_tasks_total">>,
                no_task_wait => 10 * 60 * 1000,  % 10 min
                share        => 0
            }
        },
        event_sinks => [event_sink(ES) || ES <- ?C:conf([event_sinks], NSYamlConfig, [])],
        suicide_probability => ?C:probability(?C:conf([suicide_probability], NSYamlConfig, 0))
    }}.

event_sink_ns(YamlConfig) ->
    #{
        storage                    => storage(<<"_event_sinks">>, YamlConfig),
        duplicate_search_batch     => 1000,
        default_processing_timeout => ?C:time_interval("30S", ms)
    }.

event_sink({Name, ESYamlConfig}) ->
    event_sink(?C:atom(?C:conf([type], ESYamlConfig)), Name, ESYamlConfig).

event_sink(machine, Name, ESYamlConfig) ->
    {mg_events_sink_machine, #{
        name       => ?C:atom(Name),
        machine_id => ?C:utf_bin(?C:conf([machine_id], ESYamlConfig))
    }};
event_sink(kafka, Name, ESYamlConfig) ->
    {mg_events_sink_kafka, #{
        name       => ?C:atom(Name),
        client     => ?C:atom(?C:conf([client], ESYamlConfig)),
        topic      => ?C:utf_bin(?C:conf([topic], ESYamlConfig))
    }}.

%%
%% vm.args
%%
vm_args(YamlConfig, ERLInetrcFilename) ->
    [
        {'-sname'    , service_name(YamlConfig)},
        {'-setcookie', ?C:utf_bin(?C:conf([erlang, cookie], YamlConfig, "mg_cookie" ))},
        {'+K'        , <<"true">>},
        {'+A'        , <<"10">>  },
        {'-kernel'   , <<"inetrc '\"", (?C:utf_bin(ERLInetrcFilename))/binary, "\"'">>}
    ] ++
    conf_if([erlang, ipv6], YamlConfig, [
        {'-proto_dist', <<"inet6_tcp">>}
    ]).

service_name(YamlConfig) ->
    ?C:utf_bin(?C:conf([service_name], YamlConfig, "machinegun")).

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

conf_with(YamlConfigPath, YamlConfig, Fun) ->
    Fun(?C:conf(YamlConfigPath, YamlConfig)).

conf_with(YamlConfigPath, YamlConfig, Default, Fun) ->
    case ?C:conf(YamlConfigPath, YamlConfig, undefined) of
        undefined -> Default;
        Value     -> Fun(Value)
    end.
