
%% Timer

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
    request_context :: mg:request_context(),
    retry_strategy :: mg_retry:strategy()
}).

-record(mg_machine_process_retry, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    exception :: mg_utils:exception(),
    request_context :: mg:request_context(),
    retry_strategy :: mg_retry:strategy(),
    wait_timeout :: timeout()
}).

-record(mg_machine_process_retries_exhausted, {
    namespace :: mg:ns(),
    machine_id :: mg:id(),
    exception :: mg_utils:exception(),
    request_context :: mg:request_context(),
    retry_strategy :: mg_retry:strategy()
}).

%% Machines state

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
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception()
}).
