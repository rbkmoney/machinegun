
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

-record(mg_scheduler_error, {
    tag :: atom(),
    namespace :: mg:ns(),
    exception :: mg_utils:exception(),
    machine_id :: mg:id() | undefined,
    request_context :: mg:request_context()
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

%% Workers management

-record(mg_worker_call_attempt, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    msg_queue_len :: non_neg_integer(),
    msg_queue_limit :: mg_workers_manager:queue_limit()
}).

-record(mg_worker_start_attempt, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    request_context :: mg:request_context(),
    msg_queue_len :: non_neg_integer(),
    msg_queue_limit :: mg_workers_manager:queue_limit()
}).
