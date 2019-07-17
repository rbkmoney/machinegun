
%% Timer operations

-record(mg_timer_lifecycle_created, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    target_timestamp :: genlib_time:ts()
}).

-record(mg_timer_lifecycle_removed, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context()
}).

-record(mg_timer_lifecycle_rescheduled, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    target_timestamp :: genlib_time:ts(),
    attempt :: non_neg_integer()
}).

-record(mg_timer_lifecycle_rescheduling_error, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception()
}).

%% Timer processing

-record(mg_timer_process_started, {
    queue :: normal | retries,
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_utils:deadline()
}).

-record(mg_timer_process_finished, {
    queue :: normal | retries,
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    target_timestamp :: genlib_time:ts(),
    deadline :: mg_utils:deadline(),
    duration :: non_neg_integer()  % in native units
}).

%% Scheduler

-record(mg_scheduler_search_error, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    exception :: mg_utils:exception()
}).

-record(mg_scheduler_task_error, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    exception :: mg_utils:exception(),
    machine_id :: mg:id() | undefined
}).

-record(mg_scheduler_task_add_error, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    exception :: mg_utils:exception(),
    machine_id :: mg:id(),
    request_context :: mg:request_context()
}).

-record(mg_scheduler_new_tasks, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    new_tasks_count :: non_neg_integer()
}).

-record(mg_scheduler_task_started, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    machine_id :: mg:id() | undefined,
    task_delay :: timeout() | undefined
}).

-record(mg_scheduler_task_finished, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    machine_id :: mg:id() | undefined,
    task_delay :: timeout() | undefined,
    waiting_in_queue :: non_neg_integer(),  % in native units
    process_duration :: non_neg_integer()  % in native units
}).

-record(mg_scheduler_quota_reserved, {
    namespace :: mg:ns(),
    scheduler_name :: mg_scheduler:name(),
    active_tasks :: non_neg_integer() ,
    waiting_tasks :: non_neg_integer(),
    quota_name :: mg_quota_worker:name(),
    quota_reserved :: mg_quota:resource()
}).

%% Machine

-record(mg_machine_process_transient_error, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    exception :: mg_utils:exception(),
    request_context :: mg:request_context()
}).

-record(mg_machine_process_started, {
    processor_impact :: mg_machine:processor_impact(),
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline()
}).

-record(mg_machine_process_finished, {
    processor_impact :: mg_machine:processor_impact(),
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    duration :: non_neg_integer()  % in native units
}).

%% Machines state

-record(mg_machine_lifecycle_loaded, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context()
}).

-record(mg_machine_lifecycle_created, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context()
}).

-record(mg_machine_lifecycle_removed, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context()
}).

-record(mg_machine_lifecycle_unloaded, {
    namespace :: mg:ns(),
    machine_id :: mg:id()
}).

-record(mg_machine_lifecycle_committed_suicide, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    suicide_probability :: mg_machine:suicide_probability()
}).

-record(mg_machine_lifecycle_failed, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception()
}).

-record(mg_machine_lifecycle_loading_error, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    exception :: mg_utils:exception()
}).

-record(mg_machine_lifecycle_transient_error, {
    context :: atom(),
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    exception :: mg_utils:exception(),
    request_context :: mg:request_context(),
    retry_strategy :: mg_retry:strategy(),
    retry_action :: {wait, timeout(), mg_retry:strategy()} | finish
}).

%% Workers management

-record(mg_worker_call_attempt, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline()
}).

-record(mg_worker_start_attempt, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    msg_queue_len :: non_neg_integer(),
    msg_queue_limit :: mg_workers_manager:queue_limit()
}).

%% Events sink operations

-record(mg_events_sink_kafka_sent, {
    name :: atom(),
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    encode_duration :: non_neg_integer(),  % in native units
    send_duration :: non_neg_integer(),  % in native units
    data_size :: non_neg_integer(),  % in bytes
    partition :: brod:partition(),
    offset :: brod:offset()
}).
