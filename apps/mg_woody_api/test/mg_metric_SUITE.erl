%%%
%%% Copyright 2019 RBKmoney
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

-module(mg_metric_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("mg/include/pulse.hrl").

%% tests descriptions
-export([all             /0]).
-export([groups          /0]).
-export([init_per_suite  /1]).
-export([end_per_suite   /1]).
-export([init_per_group  /2]).
-export([end_per_group   /2]).

-export([offset_bin_metric_test/1]).
-export([fraction_and_queue_bin_metric_test/1]).
-export([duration_bin_metric_test/1]).

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
        offset_bin_metric_test,
        fraction_and_queue_bin_metric_test,
        duration_bin_metric_test
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
        {how_are_you, [
            {metrics_publishers, []},
            {metrics_handlers, []}
        ]},
        {mg_woody_api, mg_woody_api_config(C)}
    ]),

    [
        {apps              , Apps                             },
        {automaton_options , #{
            url            => "http://localhost:8022",
            ns             => ?NS,
            retry_strategy => undefined
        }},
        {event_sink_options, "http://localhost:8022"          }
    |
        C
    ].

-spec end_per_suite(config()) ->
    ok.
end_per_suite(C) ->
    ok = application:set_env(how_are_you, metrics_publishers, []),
    ok = application:set_env(how_are_you, metrics_handlers, []),
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
                worker     => #{
                    sidecar => {mg_woody_api_hay, #{interval => 100}}
                },
                default_processing_timeout => 5000,
                retries => #{
                    storage   => {exponential, {max_total_timeout, 1000}, 1, 10},
                    timers    => {exponential, {max_total_timeout, 1000}, 1, 10}
                },
                event_stash_size => 5
            }
        }},
        {event_sink_ns, #{
            storage => mg_storage_memory,
            default_processing_timeout => 5000
        }}
    ].

%% Tests

-spec offset_bin_metric_test(config()) -> _.
offset_bin_metric_test(_C) ->
    Offsets = [erlang:trunc(-10 + math:pow(2, I)) || I <- lists:seq(0, 10, 1)],
    _ = [
        ok = test_beat(#mg_timer_lifecycle_created{
            namespace = ?NS,
            target_timestamp = genlib_time:unow() + Offset
        })
        || Offset <- Offsets
    ].

-spec fraction_and_queue_bin_metric_test(config()) -> _.
fraction_and_queue_bin_metric_test(_C) ->
    Samples = lists:seq(0, 200, 1),
    _ = [
        ok = test_beat(#mg_worker_start_attempt{
            namespace = ?NS,
            msg_queue_len = Sample,
            msg_queue_limit = 100
        })
        || Sample <- Samples
    ].

-spec duration_bin_metric_test(config()) -> _.
duration_bin_metric_test(_C) ->
    Samples = [erlang:trunc(math:pow(2, I)) || I <- lists:seq(0, 20, 1)],
    _ = [
        ok = test_beat(#mg_machine_process_finished{
            namespace = ?NS,
            duration = Sample,
            processor_impact = {init, []}
        })
        || Sample <- Samples
    ].

%% Utils

%% Metrics utils

-spec test_beat(mg_woody_api_pulse:beat()) ->
    ok.
test_beat(Beat) ->
    mg_woody_api_pulse_metric:handle_beat(undefined, Beat).
