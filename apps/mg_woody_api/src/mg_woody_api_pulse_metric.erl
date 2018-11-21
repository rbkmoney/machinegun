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

-module(mg_woody_api_pulse_metric).

-include_lib("mg/include/pulse.hrl").
-include_lib("mg_woody_api/include/pulse.hrl").

-export([handle_beat/2]).

%% metrics API
-export([get_all_metrics/1]).

%% internal types
-type metric() :: how_are_you:metric().
-type metrics() :: [metric()].
-type nested_metrics() :: metrics() | [nested_metrics()].
-type metric_key() :: how_are_you:metric_key().
-type bin_type() :: duration | offset.
-type beat() ::
      mg_pulse:beat()
    | #woody_request_handle_error{}
    | #woody_request_handle_retry{}.

%%
%% mg_pulse handler
%%

-spec handle_beat(undefined, beat()) ->
    ok.
handle_beat(undefined, Beat) ->
    ok = push(create_metric(Beat)).

%%
%% Metrics API
%%

-spec get_all_metrics([mg:ns()]) ->
    metrics().
get_all_metrics(Namespaces) ->
    lists:flatten([get_metrics(NS) || NS <- Namespaces]).

%% Internals

-define(DURATION_BINS, 5).
-define(DURATION_UNIT, <<"mcs">>).
-define(OFFSET_BINS, 7).
-define(OFFSET_UNIT, <<"s">>).

%% Metrics handling

-spec create_metric(beat()) ->
    metrics() | undefined.
% Machine lifecycle
create_metric(#mg_machine_lifecycle_loaded{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, loaded])];
create_metric(#mg_machine_lifecycle_unloaded{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, unloaded])];
create_metric(#mg_machine_lifecycle_created{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, created])];
create_metric(#mg_machine_lifecycle_failed{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, failed])];
create_metric(#mg_machine_lifecycle_committed_suicide{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, committed_suicide])];
create_metric(#mg_machine_lifecycle_loading_error{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, loading_error])];
% Machine processing
create_metric(#mg_machine_process_continuation_started{namespace = NS}) ->
    [create_inc([mg, machine, process, NS, continuation, started])];
create_metric(#mg_machine_process_continuation_finished{namespace = NS, duration = Duration}) ->
    [
        create_inc([mg, machine, process, NS, continuation, finished]),
        create_bin_inc([mg, machine, process, NS, continuation, duration], duration, Duration)
    ];
% Timer lifecycle
create_metric(#mg_timer_lifecycle_created{namespace = NS, target_timestamp = Timestamp}) ->
    [
        create_inc([mg, timer, lifecycle, NS, created]),
        create_bin_inc([mg, timer, lifecycle, NS, created, ts_offset], offset, Timestamp)
    ];
create_metric(#mg_timer_lifecycle_rescheduled{namespace = NS, target_timestamp = Timestamp}) ->
    [
        create_inc([mg, timer, lifecycle, NS, rescheduled]),
        create_bin_inc([mg, timer, lifecycle, NS, rescheduled, ts_offset], offset, Timestamp)
    ];
create_metric(#mg_timer_lifecycle_rescheduling_error{namespace = NS}) ->
    [create_inc([mg, timer, lifecycle, NS, rescheduling_error])];
create_metric(#mg_timer_lifecycle_removed{namespace = NS}) ->
    [create_inc([mg, timer, lifecycle, NS, removed])];
% Timer processing
create_metric(#mg_timer_process_started{namespace = NS, queue = Queue}) ->
    [create_inc([mg, timer, process, NS, Queue, started])];
create_metric(#mg_timer_process_finished{namespace = NS, queue = Queue, duration = Duration}) ->
    [
        create_inc([mg, timer, process, NS, Queue, finished]),
        create_bin_inc([mg, timer, process, NS, Queue, duration], duration, Duration)
    ];
% Sheduler
create_metric(#mg_scheduler_error{tag = Tag, namespace = NS}) when
    Tag =:= timer_handling_failed orelse
    Tag =:= timer_retry_handling_failed
->
    [create_inc([mg, sheduler, error, NS, Tag, error])];
% Unknown
create_metric(_Beat) ->
    [].

%% Metrics init

-spec get_metrics(mg:ns()) -> nested_metrics().
get_metrics(NS) ->
    [
        get_machine_lifecycle_metrics(NS),
        get_sheduler_metrics(NS),
        get_machine_processing_metrics(NS),
        get_timer_lifecycle_metrics(NS),
        get_timer_process_metrics(NS)
    ].

-spec get_machine_lifecycle_metrics(mg:ns()) -> nested_metrics().
get_machine_lifecycle_metrics(NS) ->
    Events = [loaded, unloaded, created, failed, committed_suicide, loading_error],
    [
        create_inc([mg, machine, lifecycle, NS, E])
        || E <- Events
    ].

-spec get_sheduler_metrics(mg:ns()) -> nested_metrics().
get_sheduler_metrics(NS) ->
    Tasks = [timer_handling_failed, timer_retry_handling_failed],
    [
        create_inc([mg, sheduler, error, NS, T, error])
        || T <- Tasks
    ].

-spec get_machine_processing_metrics(mg:ns()) -> nested_metrics().
get_machine_processing_metrics(NS) ->
    Impact = [continuation],
    Events = [started, finished],
    Counters = [
        create_inc([mg, machine, process, NS, I, E])
        || E <- Events, I <- Impact
    ],
    Bins = [
        list_bin_metric([mg, machine, process, NS, I, duration], duration)
        || I <- Impact
    ],
    [Counters, Bins].

-spec get_timer_lifecycle_metrics(mg:ns()) -> nested_metrics().
get_timer_lifecycle_metrics(NS) ->
    TSEvents = [created, rescheduled],
    NotTSEvents = [rescheduling_error, removed],
    Counters = [
        create_inc([mg, timer, lifecycle, NS, E])
        || E <- (TSEvents ++ NotTSEvents)
    ],
    Bins = [
        list_bin_metric([mg, timer, lifecycle, NS, E, ts_offset], offset)
        || E <- TSEvents
    ],
    [Counters, Bins].

-spec get_timer_process_metrics(mg:ns()) -> nested_metrics().
get_timer_process_metrics(NS) ->
    Queues = [normal, retries],
    Events = [started, finished],
    Counters = [
        create_inc([mg, timer, process, NS, Q, E])
        || E <- Events, Q <- Queues
    ],
    Bins = [
        list_bin_metric([mg, timer, process, NS, Q, duration], duration)
        || Q <- Queues
    ],
    [Counters, Bins].

%% Utils

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
    how_are_you:metric_construct(meter, Key, 1).

-spec list_bin_metric(metric_key(), bin_type()) ->
    [metric()].
list_bin_metric(KeyPrefix, duration) ->
    list_bin_metric(KeyPrefix, ?DURATION_BINS, ?DURATION_UNIT);
list_bin_metric(KeyPrefix, offset) ->
    list_bin_metric(KeyPrefix, ?OFFSET_BINS, ?OFFSET_UNIT).

-spec list_bin_metric(metric_key(), MaxBin :: pos_integer(), Unit :: binary()) ->
    [metric()].
list_bin_metric(KeyPrefix, MaxBin, Unit) ->
    do_list_bin_metric(KeyPrefix, MaxBin, Unit, MaxBin, []).

-spec do_list_bin_metric(metric_key(), pos_integer(), binary(), non_neg_integer(), [metric()]) ->
    [metric()].
do_list_bin_metric(_KeyPrefix, _MaxBin, _Unit, Bin, Acc) when Bin < 1 ->
    Acc;
do_list_bin_metric(KeyPrefix, MaxBin, Unit, Bin, Acc) ->
    BinSampleValue = erlang:trunc(math:pow(10, Bin)) + 1,
    BinKey = create_bin_key(BinSampleValue, MaxBin, Unit),
    Metric = how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1),
    do_list_bin_metric(KeyPrefix, MaxBin, Unit, Bin - 1, [Metric | Acc]).

-spec create_bin_inc(metric_key(), bin_type(), integer()) ->
    metric().
create_bin_inc(KeyPrefix, duration, Duration) ->
    DurationMCS = erlang:convert_time_unit(Duration, native, microsecond),
    BinKey = create_bin_key(DurationMCS, ?DURATION_BINS, ?DURATION_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1);
create_bin_inc(KeyPrefix, offset, Timestamp) ->
    Offset = erlang:max(genlib_time:unow() - Timestamp, 0),
    BinKey = create_bin_key(Offset, ?OFFSET_BINS, ?OFFSET_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1).

-spec create_bin_key(Value :: integer(), BinNumber :: pos_integer(), Unit :: binary()) ->
    metric_key().
create_bin_key(Value, MaxBin, Unit) ->
    BinNumber = erlang:min(erlang:trunc(math:log10(Value)), MaxBin),
    case BinNumber of
        0 ->
            <<"less_then_10", Unit/binary>>;
        MaxBin ->
            <<"greater_then_1e", (erlang:integer_to_binary(MaxBin))/binary, Unit/binary>>;
        _Other ->
            Bin = erlang:integer_to_binary(BinNumber),
            NexBin = erlang:integer_to_binary(BinNumber + 1),
            <<"from_1e", Bin/binary, Unit/binary, "_to_1e", NexBin/binary, Unit/binary>>
    end.
