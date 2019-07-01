%%%
%%% Copyright 2018 RBKmoney
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

-module(mg_hay_handler_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

-export([no_workers_test/1]).
-export([exist_workers_test/1]).

-define(NS, <<"NS">>).
-define(ID, <<"ID">>).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name () :: atom().
-type config    () :: [{atom(), _}].

-spec all() ->
    [test_name() | {group, group_name()}].
all() ->
    [
        no_workers_test,
        exist_workers_test
    ].

-spec groups() ->
    [{group_name(), list(_), test_name()}].
groups() ->
    [].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) ->
    config().
init_per_suite(C) ->
    Apps = mg_ct_helper:start_applications([
        gproc,
        consuela,
        {how_are_you, [
            {metrics_publishers, [mg_test_hay_publisher]},
            {metrics_handlers, [
                hay_vm_handler,
                {mg_woody_api_hay, #{interval => 100, namespaces => [?NS]}}
            ]}
        ]},
        {mg_woody_api, mg_woody_api_config(C)}
    ]),

    {ok, ProcessorPid} = mg_test_processor:start(
        {0, 0, 0, 0}, 8023,
        genlib_map:compact(#{
            processor  => {"/processor", {fun default_signal_handler/1, fun default_call_handler/1}}
        })
    ),

    [
        {apps              , Apps                             },
        {automaton_options , #{
            url            => "http://localhost:8022",
            ns             => ?NS,
            retry_strategy => undefined
        }},
        {event_sink_options, "http://localhost:8022"          },
        {processor_pid     , ProcessorPid                     }
    |
        C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    ok = application:set_env(how_are_you, metrics_publishers, []),
    ok = application:set_env(how_are_you, metrics_handlers, []),
    true = erlang:exit(?config(processor_pid, C), kill),
    mg_ct_helper:stop_applications(?config(apps, C)).

-spec init_per_group(group_name(), config()) ->
    config().
init_per_group(_, C) ->
    C.

-spec end_per_group(group_name(), config()) ->
    ok.
end_per_group(_, _C) ->
    ok.

-spec mg_woody_api_config(config()) ->
    list().
mg_woody_api_config(_C) ->
    [
        {woody_server, #{ip => {0,0,0,0,0,0,0,0}, port => 8022, limits => #{}}},
        {namespaces, #{
            ?NS => #{
                storage    => mg_storage_memory,
                processor  => #{
                    url            => <<"http://localhost:8023/processor">>,
                    transport_opts => #{pool => ns, max_connections => 100}
                },
                default_processing_timeout => 5000,
                schedulers => #{
                    timers         => #{ interval => 100 },
                    timers_retries => #{ interval => 100 },
                    overseer       => #{ interval => 100 }
                },
                retries => #{
                    storage   => {exponential, {max_total_timeout, 1000}, 1, 10},
                    timers    => {exponential, {max_total_timeout, 1000}, 1, 10}
                }
            }
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
        }}
    ].

%% Tests

-spec no_workers_test(config()) -> _.
no_workers_test(_C) ->
    ?assertEqual(0, get_metric([mg, workers_total, number])).

-spec exist_workers_test(config()) -> _.
exist_workers_test(C) ->
    mg_automaton_client:start(automaton_options(C), <<"exist_workers_test">>, []),
    ok = timer:sleep(200),
    ?assert(get_metric([mg, workers_total, number]) > 0).

%% Utils

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).

%% Processor utils

-spec default_signal_handler(mg:signal_args()) -> mg:signal_result().
default_signal_handler({Args, _Machine}) ->
    mg_test_processor:default_result(signal, Args).

-spec default_call_handler(mg:call_args()) -> mg:call_result().
default_call_handler({Args, _Machine}) ->
    case Args of
        <<"foo">> -> {Args, {null(), [content(<<"bar">>)]}, #{}}
    end.

-spec null() -> mg_events:content().
null() ->
    content(null).

-spec content(binary()) -> mg_events:content().
content(Body) ->
    {#{format_version => 42}, Body}.

%% Metrics utils

-spec get_metric(how_are_you:metric_key()) ->
    how_are_you:metric_value() | undefined.
get_metric(Key) ->
    mg_test_hay_publisher:lookup(Key).
