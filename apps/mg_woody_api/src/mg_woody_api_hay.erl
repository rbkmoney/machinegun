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

-module(mg_woody_api_hay).
-behaviour(hay_metrics_handler).

-export([child_spec/3]).

%% how_are_you callbacks
-export([init/1]).
-export([get_interval/1]).
-export([gather_metrics/1]).

%% Types

-type options() :: #{
    interval => timeout()
}.

-export_type([options/0]).

%% Internal types

-record(state, {
    interval :: timeout(),
    namespace :: mg:ns(),
    registry :: mg_procreg:options()
}).
-type state() :: #state{}.
-type worker() :: {mg:ns(), mg:id(), pid()}.
-type metric() :: how_are_you:metric().
-type metric_key() :: how_are_you:metric_key().
-type metric_value() :: how_are_you:metric_value().
-type metrics() :: [metric()].

%% API

-spec child_spec(options() | undefined, mg_workers_manager:options(), _ChildID) ->
    supervisor:child_spec().
child_spec(Options, ManagerOptions, ChildID) ->
    HandlerOptions = {genlib:define(Options, #{}), ManagerOptions},
    hay_metrics_handler:child_spec({?MODULE, HandlerOptions}, ChildID).

-spec init({options(), mg_workers_manager:options()}) -> {ok, state()}.
init({Options, #{name := NS, registry := Registry}}) ->
    {ok, #state{
        interval = maps:get(interval, Options, 10 * 1000),
        namespace = NS,
        registry = Registry
    }}.

-spec get_interval(state()) -> timeout().
get_interval(#state{interval = Interval}) ->
    Interval.

-spec gather_metrics(state()) -> [hay_metrics:metric()].
gather_metrics(#state{namespace = NS, registry = Procreg}) ->
    Workers = mg_worker:list(Procreg, NS),
    WorkerStats = workers_stats([mg, workers, NS], Workers),
    WorkerStats.

%% Internals

-spec workers_stats(metric_key(), [worker()]) ->
    metrics().
workers_stats(KeyPrefix, Workers) ->
    Metrics = [gauge([KeyPrefix, number], erlang:length(Workers))],
    WorkersStats = lists:foldl(fun extract_worker_stats/2, #{}, Workers),
    maps:fold(
        fun (Info, Values, Acc) ->
            stat_metrics([KeyPrefix, Info], Values, Acc)
        end,
        Metrics,
        WorkersStats
    ).

-spec extract_worker_stats(worker(), Acc) ->
    Acc when Acc :: #{atom() => [number()]}.
extract_worker_stats({_NS, _ID, Pid}, Acc) ->
    case erlang:process_info(Pid, interest_worker_info()) of
        undefined ->
            Acc;
        ProcessInfo ->
            append_list(ProcessInfo, Acc)
    end.

-spec append_list([{K, V}], #{K => [V]}) -> #{K => [V]}.
append_list(L, Acc) ->
    lists:foldl(
        fun ({K, V}, A) -> maps:update_with(K, fun (Vs) -> [V | Vs] end, [V], A) end,
        Acc,
        L
    ).

-spec interest_worker_info() ->
    [atom()].
interest_worker_info() ->
    [
        memory,
        message_queue_len
    ].

-spec stat_metrics(metric_key(), [number()], metrics()) -> metrics().
stat_metrics(KeyPrefix, Values, Acc) ->
    BearKeys = [
        min,
        max,
        {percentile, [50, 75, 90, 95, 99]},
        skewness,
        kurtosis,
        variance
    ],
    Statistics = bear:get_statistics_subset(Values, BearKeys),
    lists:foldl(
        fun (S, Acc1) -> bear_metric(KeyPrefix, S, Acc1) end,
        Acc,
        Statistics
    ).

-spec bear_metric(metric_key(), BearStat, metrics()) -> metrics() when
    BearStat :: {StatKey, StatValue},
    StatKey :: atom(),
    StatValue :: number() | PercentileValues,
    PercentileValues :: [{integer(), number()}].
bear_metric(KeyPrefix, {percentile, Percentiles}, Acc) ->
    [bear_percentile_metric(KeyPrefix, P) || P <- Percentiles] ++ Acc;
bear_metric(KeyPrefix, {StatKey, StatValue}, Acc) ->
    [gauge([KeyPrefix, StatKey], StatValue) | Acc].

-spec bear_percentile_metric(metric_key(), {integer(), number()}) ->
    metric().
bear_percentile_metric(KeyPrefix, {Percentile, Value}) ->
    gauge([KeyPrefix, <<"p", (erlang:integer_to_binary(Percentile))/binary>>], Value).

-spec gauge(metric_key(), metric_value()) ->
    metric().
gauge(Key, Value) ->
    how_are_you:metric_construct(gauge, Key, Value).
