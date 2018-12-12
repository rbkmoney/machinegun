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

%% how_are_you callbacks
-export([init/1]).
-export([get_interval/1]).
-export([gather_metrics/1]).

%% Types

-type options() :: #{
    interval => timeout(),
    namespaces => [mg:ns()]
}.

-export_type([options/0]).

%% Internal types

-record(state, {
    interval :: timeout(),
    namespaces :: [mg:ns()]
}).
-record(worker, {
    ns :: mg:ns(),
    id :: mg:id(),
    pid :: pid(),
    stats :: #{
        memory := non_neg_integer(),
        message_queue_len := non_neg_integer()
    }
}).
-type state() :: #state{}.
-type worker() :: #worker{}.
-type worker_key() :: {mg:ns(), mg:id(), pid()}.
-type metric() :: how_are_you:metric().
-type metric_key() :: how_are_you:metric_key().
-type metric_value() :: how_are_you:metric_value().
-type nested_metrics() :: [metric() | nested_metrics()].

%% API

-spec init(options()) -> {ok, state()}.
init(Options) ->
    {ok, #state{
        interval = maps:get(interval, Options, 10 * 1000),
        namespaces = maps:get(namespaces, Options, [])
    }}.

-spec get_interval(state()) -> timeout().
get_interval(#state{interval = Interval}) ->
    Interval.

-spec gather_metrics(state()) -> [hay_metrics:metric()].
gather_metrics(#state{namespaces = []}) ->
    [];
gather_metrics(#state{namespaces = Namespaces}) ->
    WorkerKeys = mg_worker:list_all(),
    Workers = enrich_workers_info(WorkerKeys),
    NsWorkers = group_workers_by_ns(Workers),
    NsStats = [
        workers_stats([mg, workers, NS], maps:get(NS, NsWorkers, []))
        || NS <- Namespaces
    ],
    lists:flatten([
        workers_stats([mg, workers_total], Workers),
        NsStats
    ]).

%% Internals

-spec workers_stats(metric_key(), [worker()]) ->
    nested_metrics().
workers_stats(KeyPrefix, Workers) ->
    [
        gauge([KeyPrefix, number], erlang:length(Workers)),
        [
            stat_metrics([KeyPrefix, StatKey], extract_workers_stat(StatKey, Workers))
            || StatKey <- interest_worker_info()
        ]
    ].

-spec enrich_workers_info([worker_key()]) ->
    [worker()].
enrich_workers_info(WorkerKeys) ->
    enrich_workers_info(WorkerKeys, []).

-spec enrich_workers_info(worker_key(), [worker()]) ->
    [worker()].
enrich_workers_info([], Acc) ->
    Acc;
enrich_workers_info([{NS, ID, Pid} | WorkerKeys], Acc) ->
    case erlang:process_info(Pid, interest_worker_info()) of
        undefined ->
            enrich_workers_info(WorkerKeys, Acc);
        ProcessInfo ->
            Worker = #worker{
                id = ID,
                ns = NS,
                pid = Pid,
                stats = maps:from_list(ProcessInfo)
            },
            enrich_workers_info(WorkerKeys, [Worker | Acc])
    end.

-spec group_workers_by_ns([worker()]) ->
    #{mg:ns() => [worker()]}.
group_workers_by_ns(Workers) ->
    lists:foldl(fun do_group_workers_by_ns/2, #{}, Workers).

-spec do_group_workers_by_ns(worker(), Acc) -> Acc when
    Acc :: #{mg:ns() => [worker()]}.
do_group_workers_by_ns(#worker{ns = NS} = Worker, Acc) ->
    NsAcc = maps:get(NS, Acc, []),
    Acc#{NS => [Worker | NsAcc]}.

-spec interest_worker_info() ->
    [atom()].
interest_worker_info() ->
    [
        memory,
        message_queue_len
    ].

-spec extract_workers_stat(atom(), [worker()]) ->
    [number()].
extract_workers_stat(StatKey, Workers) ->
    [maps:get(StatKey, Stats) || #worker{stats = Stats} <- Workers].

-spec stat_metrics(metric_key(), [number()]) ->
    nested_metrics().
stat_metrics(KeyPrefix, Values) ->
    BearKeys = [
        min,
        max,
        {percentile, [50, 75, 90, 95, 99]},
        skewness,
        kurtosis,
        variance
    ],
    Statistics = bear:get_statistics_subset(Values, BearKeys),
    [bear_metric(KeyPrefix, S) || S <- Statistics].

-spec bear_metric(metric_key(), BearStat) -> nested_metrics() when
    BearStat :: {StatKey, StatValue},
    StatKey :: atom(),
    StatValue :: number() | PercentileValues,
    PercentileValues :: [{integer(), number()}].
bear_metric(KeyPrefix, {percentile, Percentiles}) ->
    [bear_percentile_metric(KeyPrefix, P) || P <- Percentiles];
bear_metric(KeyPrefix, {StatKey, StatValue}) ->
    [gauge([KeyPrefix, StatKey], StatValue)].

-spec bear_percentile_metric(metric_key(), {integer(), number()}) ->
    metric().
bear_percentile_metric(KeyPrefix, {Percentile, Value}) ->
    gauge([KeyPrefix, <<"p", (erlang:integer_to_binary(Percentile))/binary>>], Value).

-spec gauge(metric_key(), metric_value()) ->
    metric().
gauge(Key, Value) ->
    how_are_you:metric_construct(gauge, Key, Value).
