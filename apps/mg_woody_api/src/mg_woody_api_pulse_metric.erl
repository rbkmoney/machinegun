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
-type bin_type() :: duration | offset | queue_length | fraction.
-type beat() :: mg_woody_api_pulse:beat().
-type impact_tag() :: atom().

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
-define(QUEUE_LEN_BINS, 5).
-define(QUEUE_LEN_UNIT, <<"">>).
-define(OFFSET_BINS, 7).
-define(OFFSET_UNIT, <<"s">>).
-define(FRACTION_BINS, 10).
-define(FRACTION_UNIT, <<"">>).

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
create_metric(#mg_machine_lifecycle_removed{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, removed])];
create_metric(#mg_machine_lifecycle_failed{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, failed])];
create_metric(#mg_machine_lifecycle_committed_suicide{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, committed_suicide])];
create_metric(#mg_machine_lifecycle_loading_error{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, loading_error])];
create_metric(#mg_machine_lifecycle_transient_error{namespace = NS}) ->
    [create_inc([mg, machine, lifecycle, NS, transient_error])];
% Machine processing
create_metric(#mg_machine_process_started{processor_impact = Impact, namespace = NS}) ->
    ImpactTag = decode_impact(Impact),
    [create_inc([mg, machine, process, NS, ImpactTag, started])];
create_metric(#mg_machine_process_finished{processor_impact = Impact, namespace = NS, duration = Duration}) ->
    ImpactTag = decode_impact(Impact),
    [
        create_inc([mg, machine, process, NS, ImpactTag, finished]),
        create_bin_inc([mg, machine, process, NS, ImpactTag, duration], duration, Duration)
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
create_metric(#mg_scheduler_task_error{scheduler_name = Name, namespace = NS}) ->
    [create_inc([mg, sheduler, NS, Name, task, error])];
create_metric(#mg_scheduler_new_tasks{scheduler_name = Name, namespace = NS, new_tasks_count = Count}) ->
    [create_inc([mg, sheduler, NS, Name, task, created], Count)];
create_metric(#mg_scheduler_task_started{scheduler_name = Name, namespace = NS, task_delay = DelayMS}) ->
    Delay = erlang:convert_time_unit(DelayMS, millisecond, native),
    [
        create_inc([mg, sheduler, NS, Name, task, started]),
        create_bin_inc([mg, sheduler, NS, Name, task, delay], duration, Delay)
    ];
create_metric(#mg_scheduler_task_finished{} = Beat) ->
    #mg_scheduler_task_finished{
        scheduler_name = Name,
        namespace = NS,
        waiting_in_queue = Waiting,
        process_duration = Processing
    } = Beat,
    [
        create_inc([mg, sheduler, NS, Name, task, finished]),
        create_bin_inc([mg, sheduler, NS, Name, task, queue_waiting], duration, Waiting),
        create_bin_inc([mg, sheduler, NS, Name, task, processing], duration, Processing)
    ];
create_metric(#mg_scheduler_quota_reserved{} = Beat) ->
    #mg_scheduler_quota_reserved{
        scheduler_name = Name,
        namespace = NS,
        active_tasks = Active,
        waiting_tasks = Waiting,
        quota_reserved = Reserved
    } = Beat,
    [
        create_gauge([mg, sheduler, NS, Name, quota, active], Active),
        create_gauge([mg, sheduler, NS, Name, quota, waiting], Waiting),
        create_gauge([mg, sheduler, NS, Name, quota, reserved], Reserved)
    ];
% Workers management
create_metric(#mg_worker_call_attempt{namespace = NS, msg_queue_len = QLen, msg_queue_limit = QLimit}) ->
    QUsage = calc_queue_usage(QLen, QLimit),
    [
        create_inc([mg, workers, NS, call_attempt]),
        create_bin_inc([mg, workers, NS, call_attempt, queue_usage], fraction, QUsage),
        create_bin_inc([mg, workers, NS, call_attempt, queue_len], queue_length, QLen)
    ];
create_metric(#mg_worker_start_attempt{namespace = NS, msg_queue_len = QLen, msg_queue_limit = QLimit}) ->
    QUsage = calc_queue_usage(QLen, QLimit),
    [
        create_inc([mg, workers, NS, start_attempt]),
        create_bin_inc([mg, workers, NS, start_attempt, queue_usage], fraction, QUsage),
        create_bin_inc([mg, workers, NS, start_attempt, queue_len], queue_length, QLen)
    ];
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
        get_timer_process_metrics(NS),
        get_workers_management_metrics(NS)
    ].

-spec get_machine_lifecycle_metrics(mg:ns()) -> nested_metrics().
get_machine_lifecycle_metrics(NS) ->
    Events = [loaded, unloaded, created, removed, failed, committed_suicide, loading_error, transient_error],
    [
        create_inc([mg, machine, lifecycle, NS, E])
        || E <- Events
    ].

-spec get_sheduler_metrics(mg:ns()) -> nested_metrics().
get_sheduler_metrics(NS) ->
    Names = [timers, timers_retries, overseer],
    TaskKeys = [error, created, started, finished],
    TaskBins = [delay, queue_waiting, processing],
    TaskMetrics = [
        [
            create_inc([mg, sheduler, NS, N, task, M])
            || N <- Names, M <- TaskKeys
        ],
        [
            list_bin_metric([mg, sheduler, NS, N, task, B], duration)
            || N <- Names, B <- TaskBins
        ]
    ],
    QuotaKeys = [active, waiting, reserved],
    QuotaMetrics = [
        create_gauge([mg, sheduler, NS, N, quota, Q], 0)
        || N <- Names, Q <- QuotaKeys
    ],
    [TaskMetrics, QuotaMetrics].

-spec get_machine_processing_metrics(mg:ns()) -> nested_metrics().
get_machine_processing_metrics(NS) ->
    Impacts = all_impact_tags(),
    Events = [started, finished],
    Counters = [
        create_inc([mg, machine, process, NS, I, E])
        || E <- Events, I <- Impacts
    ],
    Bins = [
        list_bin_metric([mg, machine, process, NS, I, duration], duration)
        || I <- Impacts
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

-spec get_workers_management_metrics(mg:ns()) -> nested_metrics().
get_workers_management_metrics(NS) ->
    Attempts = [start_attempt, call_attempt],
    Counters = [
        create_inc([mg, workers, NS, A])
        || A <- Attempts
    ],
    Bins = [
        [
            list_bin_metric([mg, workers, NS, A, queue_usage], fraction),
            list_bin_metric([mg, workers, NS, A, queue_len], queue_length)
        ]
        || A <- Attempts
    ],
    [Counters, Bins].

%% Utils

-spec decode_impact(mg_machine:processor_impact()) ->
    impact_tag().
decode_impact({init, _Args}) ->
    init;
decode_impact({repair, _Args}) ->
    repair;
decode_impact({call, _Args}) ->
    call;
decode_impact(timeout) ->
    timeout;
decode_impact(continuation) ->
    continuation.

-spec all_impact_tags() ->
    [impact_tag()].
all_impact_tags() ->
    [init, repair, call, timeout, continuation].

-spec calc_queue_usage(non_neg_integer(), mg_workers_manager:queue_limit()) ->
    float().
calc_queue_usage(Len, 0) ->
    erlang:float(Len);
calc_queue_usage(Len, Limit) ->
    Len / Limit.

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
    how_are_you:metric_construct(meter, Key, Number).

-spec create_gauge(metric_key(), integer()) ->
    metric().
create_gauge(Key, Value) ->
    how_are_you:metric_construct(gauge, Key, Value).

-spec list_bin_metric(metric_key(), bin_type()) ->
    [metric()].
list_bin_metric(KeyPrefix, duration) ->
    list_exp_bin_metric(KeyPrefix, ?DURATION_BINS, ?DURATION_UNIT);
list_bin_metric(KeyPrefix, offset) ->
    list_exp_bin_metric(KeyPrefix, ?OFFSET_BINS, ?OFFSET_UNIT);
list_bin_metric(KeyPrefix, queue_length) ->
    list_exp_bin_metric(KeyPrefix, ?QUEUE_LEN_BINS, ?QUEUE_LEN_UNIT);
list_bin_metric(KeyPrefix, fraction) ->
    list_fraction_bin_metric(KeyPrefix, ?FRACTION_BINS, ?FRACTION_UNIT).

-spec list_exp_bin_metric(metric_key(), MaxBin :: pos_integer(), Unit :: binary()) ->
    [metric()].
list_exp_bin_metric(KeyPrefix, MaxBin, Unit) ->
    do_list_exp_bin_metric(KeyPrefix, MaxBin, Unit, MaxBin, []).

-spec list_fraction_bin_metric(metric_key(), MaxBin :: pos_integer(), Unit :: binary()) ->
    [metric()].
list_fraction_bin_metric(KeyPrefix, MaxBin, Unit) ->
    do_list_fraction_bin_metric(KeyPrefix, MaxBin, Unit, MaxBin + 1, []).

-spec do_list_exp_bin_metric(metric_key(), pos_integer(), binary(), non_neg_integer(), [metric()]) ->
    [metric()].
do_list_exp_bin_metric(_KeyPrefix, _MaxBin, _Unit, Bin, Acc) when Bin < 0 ->
    Acc;
do_list_exp_bin_metric(KeyPrefix, MaxBin, Unit, Bin, Acc) ->
    BinSampleValue = erlang:trunc(math:pow(10, Bin)) + 1,
    BinKey = create_exp_bin_key(BinSampleValue, MaxBin, Unit),
    Metric = how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1),
    do_list_exp_bin_metric(KeyPrefix, MaxBin, Unit, Bin - 1, [Metric | Acc]).

-spec do_list_fraction_bin_metric(metric_key(), pos_integer(), binary(), non_neg_integer(), [metric()]) ->
    [metric()].
do_list_fraction_bin_metric(_KeyPrefix, _MaxBin, _Unit, Bin, Acc) when Bin < 0 ->
    Acc;
do_list_fraction_bin_metric(KeyPrefix, MaxBin, Unit, Bin, Acc) ->
    BinSampleValue = (Bin + 0.1) / MaxBin,
    BinKey = create_fraction_bin_key(BinSampleValue, MaxBin, Unit),
    Metric = how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1),
    do_list_fraction_bin_metric(KeyPrefix, MaxBin, Unit, Bin - 1, [Metric | Acc]).

-spec create_bin_inc(metric_key(), bin_type(), number()) ->
    metric().
create_bin_inc(KeyPrefix, duration, Duration) ->
    DurationMCS = erlang:convert_time_unit(Duration, native, microsecond),
    BinKey = create_exp_bin_key(DurationMCS, ?DURATION_BINS, ?DURATION_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1);
create_bin_inc(KeyPrefix, offset, Timestamp) ->
    Offset = erlang:max(genlib_time:unow() - Timestamp, 0),
    BinKey = create_exp_bin_key(Offset, ?OFFSET_BINS, ?OFFSET_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1);
create_bin_inc(KeyPrefix, queue_length, Length0) ->
    Length = erlang:max(Length0, 0),
    BinKey = create_exp_bin_key(Length, ?QUEUE_LEN_BINS, ?QUEUE_LEN_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1);
create_bin_inc(KeyPrefix, fraction, Fraction0) ->
    Fraction = erlang:max(Fraction0, 0.0),
    BinKey = create_fraction_bin_key(Fraction, ?FRACTION_BINS, ?FRACTION_UNIT),
    how_are_you:metric_construct(meter, [KeyPrefix, BinKey], 1).

-spec create_exp_bin_key(Value :: number(), MaxBin :: pos_integer(), Unit :: binary()) ->
    metric_key().
create_exp_bin_key(Value, MaxBin, Unit) ->
    BinNumber = calc_exp_bin(Value, MaxBin),
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

-spec calc_exp_bin(Value :: number(), MaxBin :: pos_integer()) ->
    non_neg_integer().
calc_exp_bin(Value, _MaxBin) when Value < 1 ->
    0;
calc_exp_bin(Value, MaxBin) ->
    erlang:min(erlang:trunc(math:log10(Value)), MaxBin).

-spec create_fraction_bin_key(Value :: number(), BinNumber :: pos_integer(), Unit :: binary()) ->
    metric_key().
create_fraction_bin_key(Value, MaxBin, Unit) ->
    case erlang:trunc(Value * MaxBin) of
        BinNumber when BinNumber =< MaxBin ->
            BinStart = BinNumber / MaxBin,
            BinStartPercent = erlang:integer_to_binary(erlang:trunc(BinStart * 100)),
            BinEnd = (BinNumber + 1) / MaxBin,
            BinEndPercent = erlang:integer_to_binary(erlang:trunc(BinEnd * 100)),
            <<"from_", BinStartPercent/binary, Unit/binary, "_to_", BinEndPercent/binary, Unit/binary>>;
        BinNumber when BinNumber > MaxBin ->
            <<"overflow">>
    end.
