%%%
%%% Copyright 2020 RBKmoney
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

-module(machinegun_pulse_prometheus).

-include_lib("machinegun_core/include/pulse.hrl").
-include_lib("machinegun_woody_api/include/pulse.hrl").

-export([setup/0]).
-export([handle_beat/2]).

%% internal types
-type beat() :: machinegun_pulse:beat().
-type options() :: machinegun_pulse:options().
-type metric_name() :: prometheus_metric:name().
-type metric_label_value() :: term().

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) ->
    ok.
handle_beat(_Options, Beat) ->
    ok = dispatch_metrics(Beat).

%%
%% management API
%%

%% Sets all metrics up. Call this when the app starts.
-spec setup() ->
    ok.
setup() ->
    % Machine lifecycle
    true = prometheus_counter:declare([
        {name, mg_machine_lifecycle_changes_total},
        {registry, registry()},
        {labels, [namespace, change]},
        {help, "Total number of Machinegun machine processes status changes."}
    ]),
    % Machine processing
    true = prometheus_counter:declare([
        {name, mg_machine_processing_changes_total},
        {registry, registry()},
        {labels, [namespace, impact, change]},
        {help, "Total number of Machinegun machine processing actions."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_machine_processing_duration_seconds},
        {registry, registry()},
        {labels, [namespace, impact]},
        {buckets, duration_buckets()},
        {help, "Machinegun machine processing actions durations."}
    ]),
    % Timer lifecycle
    true = prometheus_counter:declare([
        {name, mg_timer_lifecycle_changes_total},
        {registry, registry()},
        {labels, [namespace, change]},
        {help, "Total number of Machinegun timer status changes."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_timer_lifecycle_change_offset_seconds},
        {registry, registry()},
        {labels, [namespace, change]},
        {buckets, ts_offset_buckets()},
        {help, "Machinegun timer offsets."}
    ]),
    % Timer processing
    true = prometheus_counter:declare([
        {name, mg_timer_processing_changes_total},
        {registry, registry()},
        {labels, [namespace, queue, change]},
        {help, "Total number of Machinegun timer processings."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_timer_processing_duration_seconds},
        {registry, registry()},
        {labels, [namespace, queue]},
        {buckets, duration_buckets()},
        {help, "Machinegun timer processing durations."}
    ]),
    % Scheduler
    true = prometheus_counter:declare([
        {name, mg_scheduler_scan_changes_total},
        {registry, registry()},
        {labels, [namespace, name, change]},
        {help, "Total number of Machinegun scheduler scans."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_scheduler_scan_delay_seconds},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, duration_buckets()},
        {help, "Machinegun scheduler scan delay."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_scheduler_scan_duration_seconds},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, duration_buckets()},
        {help, "Machinegun scheduler scan duration."}
    ]),
    true = prometheus_counter:declare([
        {name, mg_scheduler_task_changes_total},
        {registry, registry()},
        {labels, [namespace, name, change]},
        {help, "Total number of Machinegun scheduler tasks."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_scheduler_task_processing_delay_seconds},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, duration_buckets()},
        {help, "Machinegun scheduler task processing delay."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_scheduler_task_processing_duration_seconds},
        {registry, registry()},
        {labels, [namespace, name]},
        {buckets, duration_buckets()},
        {help, "Machinegun scheduler task processing duration."}
    ]),
    true = prometheus_gauge:declare([
        {name, mg_scheduler_task_quota_usage},
        {registry, registry()},
        {labels, [namespace, name, status]},
        {help, "Machinegun scheduler task quota usage."}
    ]),
    % Workers management
    true = prometheus_counter:declare([
        {name, mg_worker_actions_total},
        {registry, registry()},
        {labels, [namespace, action]},
        {help, "Total number of Machinegun worker actions."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_worker_action_queue_length},
        {registry, registry()},
        {labels, [namespace, action]},
        {buckets, queue_length_buckets()},
        {help, "Machinegun worker queue length."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_worker_action_queue_usage},
        {registry, registry()},
        {labels, [namespace, action]},
        {buckets, fraction_buckets()},
        {help, "Machinegun worker queue usage."}
    ]),
    % Storage operations
    true = prometheus_counter:declare([
        {name, mg_storage_operation_changes_total},
        {registry, registry()},
        {labels, [namespace, name, operation, change]},
        {help, "Total number of Machinegun storage opertions."}
    ]),
    true = prometheus_histogram:declare([
        {name, mg_storage_operation_duration_seconds},
        {registry, registry()},
        {labels, [namespace, name, operation]},
        {buckets, duration_buckets()},
        {help, "Machinegun storage operation duration."}
    ]),
    ok.

%% Internals

-spec dispatch_metrics(beat()) ->
    ok.
% Machine lifecycle
dispatch_metrics(#mg_core_machine_lifecycle_loaded{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, loaded]);
dispatch_metrics(#mg_core_machine_lifecycle_unloaded{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, unloaded]);
dispatch_metrics(#mg_core_machine_lifecycle_created{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, created]);
dispatch_metrics(#mg_core_machine_lifecycle_removed{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, removed]);
dispatch_metrics(#mg_core_machine_lifecycle_failed{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, failed]);
dispatch_metrics(#mg_core_machine_lifecycle_committed_suicide{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, committed_suicide]);
dispatch_metrics(#mg_core_machine_lifecycle_loading_error{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, loading_error]);
dispatch_metrics(#mg_core_machine_lifecycle_transient_error{namespace = NS}) ->
    ok = inc(mg_machine_lifecycle_changes_total, [NS, transient_error]);
% Machine processing
dispatch_metrics(#mg_core_machine_process_started{processor_impact = Impact, namespace = NS}) ->
    ok = inc(mg_machine_processing_changes_total, [NS, decode_impact(Impact), started]);
dispatch_metrics(#mg_core_machine_process_finished{processor_impact = Impact, namespace = NS, duration = Duration}) ->
    ok = inc(mg_machine_processing_changes_total, [NS, decode_impact(Impact), finished]),
    ok = observe(mg_machine_processing_duration_seconds, [NS, decode_impact(Impact)], decode_duration(Duration));
% Timer lifecycle
dispatch_metrics(#mg_core_timer_lifecycle_created{namespace = NS, target_timestamp = Timestamp}) ->
    ok = inc(mg_timer_lifecycle_changes_total, [NS, created]),
    ok = observe(mg_timer_lifecycle_change_offset_seconds, [NS, created], decode_ts_offset(Timestamp));
dispatch_metrics(#mg_core_timer_lifecycle_rescheduled{namespace = NS, target_timestamp = Timestamp}) ->
    ok = inc(mg_timer_lifecycle_changes_total, [NS, rescheduled]),
    ok = observe(mg_timer_lifecycle_change_offset_seconds, [NS, rescheduled], decode_ts_offset(Timestamp));
dispatch_metrics(#mg_core_timer_lifecycle_rescheduling_error{namespace = NS}) ->
    ok = inc(mg_timer_lifecycle_changes_total, [NS, rescheduling_error]);
dispatch_metrics(#mg_core_timer_lifecycle_removed{namespace = NS}) ->
    ok = inc(mg_timer_lifecycle_changes_total, [NS, removed]);
% Timer processing
dispatch_metrics(#mg_core_timer_process_started{namespace = NS, queue = Queue}) ->
    ok = inc(mg_timer_processing_changes_total, [NS, Queue, started]);
dispatch_metrics(#mg_core_timer_process_finished{namespace = NS, queue = Queue, duration = Duration}) ->
    ok = inc(mg_timer_processing_changes_total, [NS, Queue, finished]),
    ok = observe(mg_timer_processing_duration_seconds, [NS, Queue], decode_duration(Duration));
% Scheduler
dispatch_metrics(#mg_core_scheduler_search_success{
    scheduler_name = Name,
    namespace = NS,
    delay = DelayMS,
    duration = Duration
}) ->
    ok = inc(mg_scheduler_scan_changes_total, [NS, Name, success]),
    ok = observe(mg_scheduler_scan_delay_seconds, [NS, Name], decode_delay(DelayMS)),
    ok = observe(mg_scheduler_scan_duration_seconds, [NS, Name], decode_duration(Duration));
dispatch_metrics(#mg_core_scheduler_search_error{scheduler_name = Name, namespace = NS}) ->
    ok = inc(mg_scheduler_scan_changes_total, [NS, Name, error]);
dispatch_metrics(#mg_core_scheduler_task_error{scheduler_name = Name, namespace = NS}) ->
    ok = inc(mg_scheduler_task_changes_total, [NS, Name, error]);
dispatch_metrics(#mg_core_scheduler_new_tasks{scheduler_name = Name, namespace = NS, new_tasks_count = Count}) ->
    ok = inc(mg_scheduler_task_changes_total, [NS, Name, created], Count);
dispatch_metrics(#mg_core_scheduler_task_started{scheduler_name = Name, namespace = NS, task_delay = DelayMS}) ->
    ok = inc(mg_scheduler_task_changes_total, [NS, Name, started]),
    ok = observe(mg_scheduler_task_processing_delay_seconds, [NS, Name], decode_delay(DelayMS));
dispatch_metrics(#mg_core_scheduler_task_finished{} = Beat) ->
    #mg_core_scheduler_task_finished{
        scheduler_name = Name,
        namespace = NS,
        process_duration = Duration
    } = Beat,
    ok = inc(mg_scheduler_task_changes_total, [NS, Name, finished]),
    ok = observe(mg_scheduler_task_processing_duration_seconds, [NS, Name], decode_duration(Duration));
dispatch_metrics(#mg_core_scheduler_quota_reserved{} = Beat) ->
    #mg_core_scheduler_quota_reserved{
        scheduler_name = Name,
        namespace = NS,
        active_tasks = Active,
        waiting_tasks = Waiting,
        quota_reserved = Reserved
    } = Beat,
    ok = set(mg_scheduler_task_quota_usage, [NS, Name, active], Active),
    ok = set(mg_scheduler_task_quota_usage, [NS, Name, waiting], Waiting),
    ok = set(mg_scheduler_task_quota_usage, [NS, Name, reserved], Reserved);
% Workers management
dispatch_metrics(#mg_core_worker_call_attempt{namespace = NS}) ->
    ok = inc(mg_worker_actions_total, [NS, call_attempt]);
dispatch_metrics(#mg_core_worker_start_attempt{namespace = NS, msg_queue_len = QLen, msg_queue_limit = QLimit}) ->
    QUsage = calc_queue_usage(QLen, QLimit),
    ok = inc(mg_worker_actions_total, [NS, start_attempt]),
    ok = observe(mg_worker_action_queue_length, [NS, start_attempt], QLen),
    ok = observe(mg_worker_action_queue_usage, [NS, start_attempt], QUsage);
% Storage operations
% TODO: it is currently assumed that the name of a storage follows a specific format
dispatch_metrics(#mg_core_storage_get_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, get, start]);
dispatch_metrics(#mg_core_storage_get_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, get, finish]),
    ok = observe(mg_storage_operation_duration_seconds, [NS, Type, get], decode_duration(Duration));
dispatch_metrics(#mg_core_storage_put_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, put, start]);
dispatch_metrics(#mg_core_storage_put_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, put, finish]),
    ok = observe(mg_storage_operation_duration_seconds, [NS, Type, put], decode_duration(Duration));
dispatch_metrics(#mg_core_storage_search_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, search, start]);
dispatch_metrics(#mg_core_storage_search_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, search, finish]),
    ok = observe(mg_storage_operation_duration_seconds, [NS, Type, search], decode_duration(Duration));
dispatch_metrics(#mg_core_storage_delete_start{name = {NS, _Caller, Type}}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, delete, start]);
dispatch_metrics(#mg_core_storage_delete_finish{name = {NS, _Caller, Type}, duration = Duration}) ->
    ok = inc(mg_storage_operation_changes_total, [NS, Type, delete, finish]),
    ok = observe(mg_storage_operation_duration_seconds, [NS, Type, delete], decode_duration(Duration));
% Unknown
dispatch_metrics(_Beat) ->
    ok.

-spec inc(metric_name(), [metric_label_value()]) ->
    ok.
inc(Name, Labels) ->
    prometheus_counter:inc(registry(), Name, Labels, 1).

-spec inc(metric_name(), [metric_label_value()], number()) ->
    ok.
inc(Name, Labels, Value) ->
    prometheus_counter:inc(registry(), Name, Labels, Value).

-spec set(metric_name(), [metric_label_value()], number()) ->
    ok.
set(Name, Labels, Value) ->
    prometheus_gauge:set(registry(), Name, Labels, Value).

-spec observe(metric_name(), [metric_label_value()], number()) ->
    ok.
observe(Name, Labels, Value) ->
    prometheus_histogram:observe(registry(), Name, Labels, Value).

-spec registry() ->
    prometheus_registry:registry().
registry() ->
    default.

-spec decode_impact(mg_core_machine:processor_impact()) ->
    atom().
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

-spec decode_duration(number()) ->
    number().
decode_duration(Duration) ->
    erlang:convert_time_unit(Duration, native, microsecond) / 1000000.

-spec decode_ts_offset(number()) ->
    number().
decode_ts_offset(Timestamp) ->
    erlang:max(genlib_time:unow() - Timestamp, 0).

-spec decode_delay(number()) ->
    number().
decode_delay(DelayMs) ->
    DelayMs / 1000.

-spec calc_queue_usage(non_neg_integer(), mg_core_workers_manager:queue_limit()) ->
    float().
calc_queue_usage(Len, 0) ->
    erlang:float(Len);
calc_queue_usage(Len, Limit) ->
    Len / Limit.

-spec duration_buckets() ->
    [number()].
duration_buckets() ->
    [
        0.001,
        0.005,
        0.010,
        0.025,
        0.050,
        0.100,
        0.250,
        0.500,
        1000,
        10 * 1000,
        30 * 1000,
        60 * 1000,
        300 * 1000
    ].

-spec ts_offset_buckets() ->
    [number()].
ts_offset_buckets() ->
    [
        0,
        1,
        10,
        60,
        10 * 60,
        60 * 60,
        24 * 60 * 60,
        7  * 24 * 60 * 60,
        30 * 24 * 60 * 60,
        365 * 24 * 60 * 60,
        5 * 365 * 24 * 60 * 60
    ].

-spec queue_length_buckets() ->
    [number()].
queue_length_buckets() ->
    [
        1,
        5,
        10,
        20,
        50,
        100,
        500,
        1000,
        5000,
        10000
    ].

-spec fraction_buckets() ->
    [number()].
fraction_buckets() ->
    [
        0.1,
        0.2,
        0.3,
        0.4,
        0.5,
        0.6,
        0.7,
        0.8,
        0.9,
        1.0
    ].
