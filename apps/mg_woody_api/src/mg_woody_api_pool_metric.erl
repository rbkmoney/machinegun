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

%%%
%%% Pooler metric handler
%%%

-module(mg_woody_api_pool_metric).

-export([notify/3]).

%% internal types

-type metric() :: how_are_you:metric().
-type metrics() :: [metric()].
-type nested_metrics() :: metrics() | [nested_metrics()].
-type metric_key() :: how_are_you:metric_key().
-type bin_type() :: mg_woody_api_metric_utils:bin_type().

-type metric_type() :: counter | histogram | history | meter.
-type metric_value() :: {inc, non_neg_integer()} | integer() | any().

%%
%% mg_pulse handler
%%

-spec notify(metric_key(), metric_value(), metric_type()) ->
    ok.
notify(Key, {inc, Value}, counter) ->
    ok = push([create_inc([mg, Key], Value)]);
notify(_Key, _Value, meter) ->
    ok;
notify(_Key, _Value, history) ->
    ok;
notify(Key, Value, histogram) ->
    ok = push([create_bin_inc([mg, Key], queue_length, Value)]);
notify(Key, Value, Type) ->
    logger:warning("Unexpected pool metric ~p ~p=~p", [Type, Key, Value]).

%% Utils

-spec push(metrics()) ->
    ok.
push(Metrics) ->
    mg_woody_api_metric_utils:push(Metrics).

-spec create_inc(metric_key(), non_neg_integer()) ->
    metric().
create_inc(Key, Number) ->
    mg_woody_api_metric_utils:create_inc(Key, Number).

-spec create_bin_inc(metric_key(), bin_type(), number()) ->
    metric().
create_bin_inc(KeyPrefix, BinType, Value) ->
    mg_woody_api_metric_utils:create_bin_inc(KeyPrefix, BinType, Value).
