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
    ok = logger:set_primary_config(level, error),
    {ok, [[Home]]} = init:get_argument(home),
    YamlConfig = ?C:parse_yaml_config(YamlConfigFilename),
    ERLInetrcFilename = filename:join(ConfigsPath, "erl_inetrc"),
    ErlangCookieFilename = filename:join(Home, ".erlang.cookie"),
    ?C:write_files([
        {filename:join(ConfigsPath, "sys.config"), ?C:print_sys_config(sys_config(YamlConfig))},
        {filename:join(ConfigsPath, "vm.args"   ), ?C:print_vm_args   (vm_args   (YamlConfig, ERLInetrcFilename))},
        {ERLInetrcFilename                       , ?C:print_erl_inetrc(erl_inetrc(YamlConfig))},
        % TODO
        % Writing distribution cookie to the file which BEAM looks for when setting up the
        % distribution under *nix, as a fallback mechanism when missing `-setcookie` arg from
        % command line.
        % It's the only method not to expose cookie contents in the BEAM command line in a way which
        % doesn't break various start script functions (e.g. remsh or ping), however it may still
        % appear there for a brief amount of time while running them.
        % One must take care to run service under its own UID because `~/.erlang.cookie` is supposed
        % to be shared between every BEAM instance run by some user.
        {ErlangCookieFilename, cookie(YamlConfig), 8#00400}
    ]).

%%
%% sys.config
%%
sys_config(YamlConfig) ->
    [
        {os_mon      , os_mon      (YamlConfig)},
        {kernel, [
            {logger_level, logger_level(YamlConfig)},
            {logger      , logger      (YamlConfig)}
        ]},
        {consuela    , consuela    (YamlConfig)},
        {how_are_you , how_are_you (YamlConfig)},
        {snowflake   , snowflake   (YamlConfig)},
        {brod        , brod        (YamlConfig)},
        {hackney     , hackney     (YamlConfig)},
        {mg_woody_api, mg_woody_api(YamlConfig)}
    ].

os_mon(_YamlConfig) ->
    [
        % for better compatibility with busybox coreutils
        {disksup_posix_only, true}
    ].

logger_level(YamlConfig) ->
    ?C:log_level(?C:conf([logging, level], YamlConfig, "info")).

logger(YamlConfig) ->
    Root = ?C:filename(?C:conf([logging, root], YamlConfig, "/var/log/machinegun")),
    LogfileName = ?C:filename (?C:conf([logging, json_log], YamlConfig, "log.json")),
    FullLogname = filename:join(Root, LogfileName),
    [
        {handler, default, logger_std_h, #{
            level => debug,
            config => #{
                type => file,
                file => FullLogname,
                burst_limit_enable => ?C:conf([logging, burst_limit_enable], YamlConfig,  true),
                sync_mode_qlen => ?C:conf([logging, sync_mode_qlen], YamlConfig,  100),
                drop_mode_qlen => ?C:conf([logging, drop_mode_qlen], YamlConfig, 1000),
                flush_qlen     => ?C:conf([logging, flush_qlen],     YamlConfig, 2000)
            },
            formatter => {logger_logstash_formatter, #{}}
        }}
    ].

consuela(YamlConfig) ->
    lists:append([
        conf_with([consuela, presence], YamlConfig, [], fun (PresenceConfig) -> [
            {presence, #{
                name      => service_presence_name(YamlConfig),
                consul    => consul_client(mg_consuela_presence, YamlConfig),
                shutdown  => ?C:time_interval(?C:conf([shutdown_timeout], PresenceConfig, "5s"), 'ms'),
                session_opts => #{
                    interval => ?C:time_interval(?C:conf([check_interval], PresenceConfig, "5s"), 'sec'),
                    pulse    => mg_consuela_pulse_adapter:pulse(presence_session, mg_woody_api_pulse)
                }
            }}
        ] end),
        conf_with([consuela, registry], YamlConfig, [], fun (RegConfig) -> [
            {registry, #{
                nodename  => ?C:conf([nodename], RegConfig, ?C:hostname()),
                namespace => ?C:utf_bin(?C:conf([namespace], RegConfig, "mg")),
                session   => maps:merge(
                    #{
                        ttl        => ?C:time_interval(?C:conf([session_ttl], RegConfig, "30s"), 'sec'),
                        lock_delay => ?C:time_interval(?C:conf([session_lock_delay], RegConfig, "10s"), 'sec')
                    },
                    conf_with([consuela, presence], YamlConfig, #{}, fun (_) -> #{
                        presence => service_presence_name(YamlConfig)
                    } end)
                ),
                consul    => consul_client(mg_consuela_registry, YamlConfig),
                shutdown  => ?C:time_interval(?C:conf([shutdown_timeout], RegConfig, "5s"), 'ms'),
                keeper    => maps:merge(
                    #{
                    pulse => mg_consuela_pulse_adapter:pulse(session_keeper, mg_woody_api_pulse)
                },
                    conf_with([session_renewal_interval], RegConfig, #{}, fun (V) -> #{
                        interval => ?C:time_interval(V, 'sec')
                    } end)
                ),
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
                name      => service_presence_name(YamlConfig),
                tags      => [?C:utf_bin(T) || T <- ?C:conf([tags], DiscoveryConfig, [])],
                consul    => consul_client(mg_consuela_discovery, YamlConfig),
                opts      => #{
                    interval => #{
                        init => ?C:time_interval(?C:conf([interval, init], DiscoveryConfig,  "5s"), 'ms'),
                        idle => ?C:time_interval(?C:conf([interval, idle], DiscoveryConfig, "10m"), 'ms')
                    },
                    pulse => mg_consuela_pulse_adapter:pulse(discovery_server, mg_woody_api_pulse)
                }
            }}
        ] end)
    ]).

service_presence_name(YamlConfig) ->
    erlang:iolist_to_binary([service_name(YamlConfig), "-consuela"]).

consul_client(Name, YamlConfig) ->
    ACLToken = conf_with(
        [consul, acl_token_file], YamlConfig,
        undefined,
        fun (V) -> {file, ?C:file(V, 8#600)} end
    ),
    #{
        url   => ?C:conf([consul, url], YamlConfig),
        opts  => genlib_map:compact(#{
            datacenter => ?C:conf([consul, datacenter], YamlConfig, undefined),
            acl        => ACLToken,
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
                    ?C:proplist(?C:conf([consul, ssl_options      ], YamlConfig, undefined))
            }),
            pulse => mg_consuela_pulse_adapter:pulse(client, mg_woody_api_pulse)
        })
    }.

how_are_you(YamlConfig) ->
    Publishers = hay_statsd_publisher(YamlConfig),
    [
        {metrics_publishers, Publishers},
        {metrics_handlers, [
            hay_vm_handler,
            hay_cgroup_handler
        ]}
    ].

hay_statsd_publisher(YamlConfig) ->
    conf_with([metrics, publisher, statsd], YamlConfig, [], fun (Config) -> [
        {hay_statsd_publisher, #{
            key_prefix => <<(service_name(YamlConfig))/binary, ".">>,
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

hackney(_YamlConfig) ->
    [
        {mod_metrics, woody_client_metrics}
    ].

mg_woody_api(YamlConfig) ->
    [
        {woody_server   , woody_server   (YamlConfig)},
        {health_check   , health_check   (YamlConfig)},
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
            request_timeout => ?C:time_interval(
                ?C:conf([woody_server, http_keep_alive_timeout], YamlConfig, "5S"), 'ms'
            ),
            % idle_timeout must be greater then any possible deadline
            idle_timeout    => ?C:time_interval(?C:conf([woody_server, idle_timeout], YamlConfig, "infinity"), 'ms'),
            logger          => logger
        },
        limits   => genlib_map:compact(#{
            max_heap_size       => ?C:mem_words(?C:conf([limits, process_heap], YamlConfig, undefined)),
            total_mem_threshold => absolute_memory_limit(YamlConfig)
        })
    }.

health_check(YamlConfig) ->
    lists:foldl(
        fun maps:merge/2,
        #{},
        [
            conf_with([limits, disk], YamlConfig, #{}, fun (DiskConfig) ->
                DiskPath = ?C:conf([path], DiskConfig, "/"),
                #{disk => {erl_health, disk, [DiskPath, percent(?C:conf([value], DiskConfig))]}}
            end),
            relative_memory_limit(YamlConfig, #{}, fun ({TypeStr, Limit}) ->
                Type =
                    case TypeStr of
                        "total"   -> total;
                        "cgroups" -> cg_memory
                    end,
                #{memory => {erl_health, Type, [Limit]}}
            end),
            #{service => {erl_health, service, [service_name(YamlConfig)]}}
        ]
    ).

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
memory_amount("total"  ) -> proplists:get_value(total_memory, memsup:get_system_memory_data()).

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
            {mg_storage_riak, #{
                host   => ?C:utf_bin(?C:conf([storage, host], YamlConfig)),
                port   =>            ?C:conf([storage, port], YamlConfig),
                bucket => NS,
                connect_timeout => ?C:time_interval(?C:conf([storage, connect_timeout  ], YamlConfig, "5S" ), ms),
                request_timeout => ?C:time_interval(?C:conf([storage, request_timeout  ], YamlConfig, "10S"), ms),
                pool_options => #{
                    % If `init_count` is greater than zero, then the service will not start
                    % if the riak is unavailable. The `pooler` synchronously creates `init_count`
                    % connections at the start.
                    init_count          => 0,
                    max_count           => ?C:conf([storage, pool, size], YamlConfig, 100),
                    idle_timeout        => timer:seconds(60),
                    cull_interval       => timer:seconds(10),
                    queue_max           => ?C:conf([storage, pool, queue_max], YamlConfig, 1000)
                }
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
    Timeout = fun(TimeoutName, Default) ->
        timeout(TimeoutName, NSYamlConfig, Default, ms)
    end,
    {Name, #{
        storage   => storage(Name, YamlConfig),
        processor => #{
            url            => ?C:utf_bin(?C:conf([processor, url], NSYamlConfig)),
            transport_opts => #{
                pool => erlang:list_to_atom(NameStr),
                timeout => ?C:time_interval(?C:conf([processor, http_keep_alive_timeout], NSYamlConfig, "4S"), 'ms'),
                max_connections => ?C:conf([processor, pool_size], NSYamlConfig, 50)
            },
            resolver_opts => #{
                ip_picker => random
            }
        },
        worker => #{
            registry          => procreg(YamlConfig),
            worker_options    => #{
                hibernate_timeout => Timeout(hibernate_timeout,  "5S"),
                unload_timeout    => Timeout(unload_timeout   , "60S")
            }
        },
        default_processing_timeout => Timeout(default_processing_timeout, "30S"),
        timer_processing_timeout => Timeout(timer_processing_timeout, "60S"),
        reschedule_timeout => Timeout(reschedule_timeout, "60S"),
        retries => #{
            storage      => {exponential, infinity, 2, 10, 60 * 1000},
            %% max_total_timeout not supported for timers yet, see mg_retry:new_strategy/2 comments
            %% actual timers sheduling resolution is one second
            timers       => {exponential, 100, 2, 1000, 30 * 60 * 1000},
            processor    => {exponential, {max_total_timeout, 24 * 60 * 60 * 1000}, 2, 10, 60 * 1000},
            continuation => {exponential, infinity, 2, 10, 60 * 1000}
        },
        schedulers => maps:merge(
            case ?C:conf([timers], NSYamlConfig, []) of
                "disabled" ->
                    #{};
                TimersConfig ->
                    #{
                        timers         => timer_scheduler(2, TimersConfig),
                        timers_retries => timer_scheduler(1, TimersConfig)
                    }
            end,
            case ?C:conf([overseer], NSYamlConfig, []) of
                "disabled" ->
                    #{};
                OverseerConfig ->
                    #{
                        overseer => overseer_scheduler(0, OverseerConfig)
                    }
            end
        ),
        event_sinks => [event_sink(ES) || ES <- ?C:conf([event_sinks], NSYamlConfig, [])],
        suicide_probability => ?C:probability(?C:conf([suicide_probability], NSYamlConfig, 0)),
        event_stash_size => ?C:conf([event_stash_size], NSYamlConfig, 0)
    }}.

scheduler(Share, Config) ->
    #{
        max_scan_limit => ?C:conf([scan_limit], Config, 5000),
        task_quota     => <<"scheduler_tasks_total">>,
        task_share     => Share
    }.

timer_scheduler(Share, Config) ->
    (scheduler(Share, Config))#{
        capacity       => ?C:conf([capacity], Config, 1000),
        min_scan_delay => 1000,
        target_cutoff  => timeout(scan_interval, Config, "60s", sec)
    }.

overseer_scheduler(Share, Config) ->
    (scheduler(Share, Config))#{
        capacity       => ?C:conf([capacity], Config, 1000),
        min_scan_delay => 1000,
        rescan_delay   => 10 * 60 * 1000
    }.

timeout(Name, Config, Default, Unit) ->
    ?C:time_interval(?C:conf([Name], Config, Default), Unit).

event_sink_ns(YamlConfig) ->
    #{
        registry                   => procreg(YamlConfig),
        storage                    => storage(<<"_event_sinks">>, YamlConfig),
        worker                     => #{registry => procreg(YamlConfig)},
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

procreg(YamlConfig) ->
    % Use consuela if it's set up, gproc otherwise
    conf_with(
        [consuela],
        YamlConfig,
        mg_procreg_gproc,
        {mg_procreg_consuela, #{pulse => mg_woody_api_pulse}}
    ).

%%
%% vm.args
%%
vm_args(YamlConfig, ERLInetrcFilename) ->
    [
        node_name(YamlConfig),
        {'+K'        , <<"true">>},
        {'+A'        , <<"10">>  },
        {'-kernel'   , <<"inetrc '\"", (?C:utf_bin(ERLInetrcFilename))/binary, "\"'">>}
    ] ++
    conf_if([erlang, ipv6], YamlConfig, [
        {'-proto_dist', <<"inet6_tcp">>}
    ]).

cookie(YamlConfig) ->
    ?C:contents(?C:filename(?C:conf([erlang, secret_cookie_file], YamlConfig))).

service_name(YamlConfig) ->
    ?C:utf_bin(?C:conf([service_name], YamlConfig, "machinegun")).

node_name(YamlConfig) ->
    Name = ?C:conf([dist_node_name], YamlConfig, default_node_name(YamlConfig)),
    {node_name_type(Name), ?C:utf_bin(Name)}.

node_name_type(Name) ->
    case string:split(Name, "@") of
        [_, Hostname] -> host_name_type(Hostname);
        [_]           -> '-sname'
    end.

host_name_type(Name) ->
    case inet:parse_address(Name) of
        {ok, _} ->
            '-name';
        {error, einval} ->
            case string:find(Name, ".") of
                nomatch -> '-sname';
                _       -> '-name'
            end
    end.

default_node_name(YamlConfig) ->
    ?C:conf([service_name], YamlConfig, "machinegun") ++ "@" ++ guess_host_addr(YamlConfig).

guess_host_addr(YamlConfig) ->
    inet:ntoa(?C:guess_host_address(address_family_preference(YamlConfig))).

address_family_preference(YamlConfig) ->
    conf_with([erlang, ipv6], YamlConfig, inet, fun (true) -> inet6; (false) -> inet end).

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

conf_with(YamlConfigPath, YamlConfig, Default, FunOrVal) ->
    case ?C:conf(YamlConfigPath, YamlConfig, undefined) of
        undefined -> Default;
        Value when is_function(FunOrVal) -> FunOrVal(Value);
        _Value -> FunOrVal
    end.
