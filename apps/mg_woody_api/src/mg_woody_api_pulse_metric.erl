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

%% internal types
-type metric() :: how_are_you:metric().
-type metrics() :: [metric()].
-type metric_key() :: how_are_you:metric_key().
-type beat() :: mg_woody_api_pulse:beat().
-type impact_tag() :: atom().
-type bin_type() :: mg_woody_api_metric_utils:bin_type().

%%
%% mg_pulse handler
%%

-spec handle_beat(undefined, beat()) ->
    ok.
handle_beat(undefined, Beat) ->
    ok = push(create_metric(Beat)).

%% Internals

%% Metrics handling

-spec create_metric(beat()) ->
    metrics().
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
% scheduler
create_metric(#mg_scheduler_search_success{
    scheduler_name = Name,
    namespace = NS,
    delay = DelayMs,
    duration = Duration
}) ->
    create_delay_inc([mg, scheduler, NS, Name, scan, delay], DelayMs) ++ [
        create_inc([mg, scheduler, NS, Name, scan, success]),
        create_bin_inc([mg, scheduler, NS, Name, scan, duration], duration, Duration)
    ];
create_metric(#mg_scheduler_search_error{scheduler_name = Name, namespace = NS}) ->
    [create_inc([mg, scheduler, NS, Name, scan, error])];
create_metric(#mg_scheduler_task_error{scheduler_name = Name, namespace = NS}) ->
    [create_inc([mg, scheduler, NS, Name, task, error])];
create_metric(#mg_scheduler_new_tasks{scheduler_name = Name, namespace = NS, new_tasks_count = Count}) ->
    [create_inc([mg, scheduler, NS, Name, task, created], Count)];
create_metric(#mg_scheduler_task_started{scheduler_name = Name, namespace = NS, task_delay = DelayMS}) ->
    DelayMetrics = create_delay_inc([mg, scheduler, NS, Name, task, delay], DelayMS),
    [create_inc([mg, scheduler, NS, Name, task, started]) | DelayMetrics];
create_metric(#mg_scheduler_task_finished{} = Beat) ->
    #mg_scheduler_task_finished{
        scheduler_name = Name,
        namespace = NS,
        process_duration = Processing
    } = Beat,
    [
        create_inc([mg, scheduler, NS, Name, task, finished]),
        create_bin_inc([mg, scheduler, NS, Name, task, processing], duration, Processing)
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
        create_gauge([mg, scheduler, NS, Name, quota, active], Active),
        create_gauge([mg, scheduler, NS, Name, quota, waiting], Waiting),
        create_gauge([mg, scheduler, NS, Name, quota, reserved], Reserved)
    ];
% Workers management
create_metric(#mg_worker_call_attempt{namespace = NS}) ->
    [
        create_inc([mg, workers, NS, call_attempt])
    ];
create_metric(#mg_worker_start_attempt{namespace = NS, msg_queue_len = QLen, msg_queue_limit = QLimit}) ->
    QUsage = calc_queue_usage(QLen, QLimit),
    [
        create_inc([mg, workers, NS, start_attempt]),
        create_bin_inc([mg, workers, NS, start_attempt, queue_usage], fraction, QUsage),
        create_bin_inc([mg, workers, NS, start_attempt, queue_len], queue_length, QLen)
    ];
% Storage operations
create_metric(#mg_storage_call_get{namespace = NS}) ->
    [
        create_inc([mg, storage, NS, get])
    ];
create_metric(#mg_storage_call_put{namespace = NS}) ->
    [
        create_inc([mg, storage, NS, put])
    ];
% Unknown
create_metric(_Beat) ->
    [].

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

-spec calc_queue_usage(non_neg_integer(), mg_workers_manager:queue_limit()) ->
    float().
calc_queue_usage(Len, 0) ->
    erlang:float(Len);
calc_queue_usage(Len, Limit) ->
    Len / Limit.

-spec push(metrics()) ->
    ok.
push(Metrics) ->
    mg_woody_api_metric_utils:push(Metrics).

-spec create_inc(metric_key()) ->
    metric().
create_inc(Key) ->
    mg_woody_api_metric_utils:create_inc(Key).

-spec create_inc(metric_key(), non_neg_integer()) ->
    metric().
create_inc(Key, Number) ->
    mg_woody_api_metric_utils:create_inc(Key, Number).

-spec create_gauge(metric_key(), non_neg_integer()) ->
    metric().
create_gauge(Key, Number) ->
    mg_woody_api_metric_utils:create_gauge(Key, Number).

-spec create_delay_inc(metric_key(), number() | undefined) ->
    [metric()].
create_delay_inc(KeyPrefix, Number) ->
    mg_woody_api_metric_utils:create_delay_inc(KeyPrefix, Number).

-spec create_bin_inc(metric_key(), bin_type(), number()) ->
    metric().
create_bin_inc(KeyPrefix, BinType, Value) ->
    mg_woody_api_metric_utils:create_bin_inc(KeyPrefix, BinType, Value).
