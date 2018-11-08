
-record(woody_request_handle_error, {
    namespace :: mg:ns(),
    machine_ref :: mg_events_machine:ref(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception(),
    retry_strategy :: mg_retry:strategy()
}).

-record(woody_request_handle_retry, {
    namespace :: mg:ns(),
    machine_ref :: mg_events_machine:ref(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception(),
    retry_strategy :: mg_retry:strategy(),
    wait_timeout :: timeout()
}).
