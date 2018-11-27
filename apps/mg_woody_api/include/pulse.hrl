
-record(woody_request_handle_error, {
    namespace :: mg:ns(),
    machine_ref :: mg_events_machine:ref(),
    request_context :: mg:request_context(),
    deadline :: mg_utils:deadline(),
    exception :: mg_utils:exception(),
    retry_strategy :: mg_retry:strategy(),
    error_reaction :: mg_woody_api_utils:error_reaction()
}).

-record(woody_event, {
    event :: woody_event_handler:event(),
    rpc_id :: woody:rpc_id(),
    event_meta :: woody_event_handler:event_meta()
}).
