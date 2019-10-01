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

-module(mg_woody_api_metric_utils).

-export([push/1]).
-export([create_inc/1]).
-export([create_inc/2]).
-export([create_gauge/2]).
-export([create_delay_inc/2]).
-export([create_bin_inc/3]).

-type metric() :: how_are_you:metric().
-type metrics() :: [metric()].
-type nested_metrics() :: metrics() | [nested_metrics()].
-type metric_key() :: how_are_you:metric_key().
-type bin_type() :: duration | offset | queue_length | fraction.

-export_type([metric/0]).
-export_type([metrics/0]).
-export_type([nested_metrics/0]).
-export_type([metric_key/0]).
-export_type([bin_type/0]).

%% Internal types

-type bin() :: {number(), Name :: binary()}.

%% API

-spec push(metrics()) ->
    ok.
push([]) ->
    ok;
push([M | Metrics]) ->
    ok = how_are_you:metric_push(M),
    push(Metrics).

-spec create_inc(metric_key()) ->
    metric().
create_inc(Key) ->
    create_inc(Key, 1).

-spec create_inc(metric_key(), non_neg_integer()) ->
    metric().
create_inc(Key, Number) ->
    how_are_you:metric_construct(counter, Key, Number).

-spec create_gauge(metric_key(), non_neg_integer()) ->
    metric().
create_gauge(Key, Number) ->
    how_are_you:metric_construct(gauge, Key, Number).

-spec create_delay_inc(metric_key(), number() | undefined) ->
    [metric()].
create_delay_inc(_KeyPrefix, undefined) ->
    [];
create_delay_inc(Key, DelayMS) ->
    Delay = erlang:convert_time_unit(DelayMS, millisecond, native),
    [create_bin_inc(Key, duration, Delay)].

-spec create_bin_inc(metric_key(), bin_type(), number()) ->
    metric().
create_bin_inc(KeyPrefix, BinType, Value) ->
    Prepared = prepare_bin_value(BinType, Value),
    Bins = build_bins(BinType),
    BinKey = build_bin_key(Bins, Prepared),
    how_are_you:metric_construct(counter, [KeyPrefix, BinKey], 1).

%% Internals

-spec prepare_bin_value(bin_type(), number()) ->
    number().
prepare_bin_value(duration, Duration) ->
    erlang:convert_time_unit(Duration, native, microsecond);
prepare_bin_value(offset, Timestamp) ->
    erlang:max(genlib_time:unow() - Timestamp, 0);
prepare_bin_value(queue_length, Length) ->
    erlang:max(Length, 0);
prepare_bin_value(fraction, Fraction) ->
    erlang:max(Fraction, 0.0).

-spec build_bin_key(Bins :: [bin()], Value :: number()) ->
    metric_key().
build_bin_key([{HeadValue, HeadName} | _Bins], Value) when HeadValue > Value ->
    <<"less_than_", HeadName/binary>>;
build_bin_key([{LastValue, LastName}], Value) when LastValue =< Value ->
    <<"greater_than_", LastName/binary>>;
build_bin_key([{LeftValue, LeftName}, {RightValue, RightName} | _Bins], Value) when
    LeftValue =< Value andalso RightValue > Value
->
    <<"from_", LeftName/binary, "_to_", RightName/binary>>;
build_bin_key([{HeadValue, _HeadName} | Bins], Value) when HeadValue =< Value ->
    build_bin_key(Bins, Value).

-spec build_bins(bin_type()) ->
    [bin()].
build_bins(duration) ->
    [
        {1000, <<"1ms">>},
        {5 * 1000, <<"5ms">>},
        {10 * 1000, <<"10ms">>},
        {25 * 1000, <<"25ms">>},
        {50 * 1000, <<"50ms">>},
        {100 * 1000, <<"100ms">>},
        {250 * 1000, <<"250ms">>},
        {500 * 1000, <<"500ms">>},
        {1000 * 1000, <<"1s">>},
        {10 * 1000 * 1000, <<"10s">>},
        {30 * 1000 * 1000, <<"30s">>},
        {60 * 1000 * 1000, <<"1m">>},
        {5 * 60 * 1000 * 1000, <<"5m">>}
    ];
build_bins(fraction) ->
    [
        {0.1, <<"10">>},
        {0.2, <<"20">>},
        {0.3, <<"30">>},
        {0.4, <<"40">>},
        {0.5, <<"50">>},
        {0.6, <<"60">>},
        {0.7, <<"70">>},
        {0.8, <<"80">>},
        {0.9, <<"90">>},
        {1.0, <<"100">>}
    ];
build_bins(offset) ->
    [
        {1, <<"1s">>},
        {10, <<"10s">>},
        {60, <<"1m">>},
        {10 * 60, <<"10m">>},
        {60 * 60, <<"1h">>},
        {24 * 60 * 60, <<"1d">>},
        {7  * 24 * 60 * 60, <<"7d">>},
        {30 * 24 * 60 * 60, <<"30d">>},
        {365 * 24 * 60 * 60, <<"1y">>},
        {5 * 365 * 24 * 60 * 60, <<"5y">>}
    ];
build_bins(queue_length) ->
    [
        {1, <<"1">>},
        {5, <<"5">>},
        {10, <<"10">>},
        {20, <<"20">>},
        {50, <<"50">>},
        {100, <<"100">>},
        {1000, <<"1000">>},
        {10000, <<"10000">>}
    ].
