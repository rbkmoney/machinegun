-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_events_machine handler
-behaviour(mg_events_machine).
-export_type([options/0]).
-export([processor_child_spec/1, process_signal/3, process_call/3]).

%%
%% mg_events_machine handler
%%
-type options() :: woody_client:options().

-spec processor_child_spec(options()) ->
    supervisor:child_spec().
processor_child_spec(Options) ->
    woody_client:connection_pool_spec(Options).

-spec process_signal(options(), mg_events_machine:request_context(), mg_events_machine:signal_args()) ->
    mg_events_machine:signal_result().
process_signal(Options, ReqCtx, {Signal, Machine}) ->
    SignalResult =
        call_processor(
            Options,
            ReqCtx,
            'ProcessSignal',
            [mg_woody_api_packer:pack(signal_args, {Signal, Machine})]
        ),
    mg_woody_api_packer:unpack(signal_result, SignalResult).

-spec process_call(options(), mg_events_machine:request_context(), mg_events_machine:call_args()) ->
    mg_events_machine:call_result().
process_call(Options, ReqCtx, {Call, Machine}) ->
    CallResult =
        call_processor(
            Options,
            ReqCtx,
            'ProcessCall',
            [mg_woody_api_packer:pack(call_args, {Call, Machine})]
        ),
    mg_woody_api_packer:unpack(call_result, CallResult).

% для совместимости со старыми машинами, где нет контекста
-spec call_processor(options(), mg_events_machine:request_context(), atom(), list(_)) ->
    _Result.
call_processor(Options, ReqCtx, Function, Args) ->
    % TODO сделать нормально!
    {ok, TRef} = timer:kill_after(maps:get(recv_timeout, Options, 5000) + 3000),
    try
        {ok, R} =
            woody_client:call(
                {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
                Options,
                request_context_to_woody_context(ReqCtx)
            ),
        R
    catch
        error:Reason={woody_error, {_, resource_unavailable, _}} ->
            throw({transient, {processor_unavailable, Reason}});
        error:Reason={woody_error, {_, result_unknown, _}} ->
            throw({transient, {processor_unavailable, Reason}})
    after
        {ok, cancel} = timer:cancel(TRef)
    end.

-spec request_context_to_woody_context(mg_events_machine:request_context()) ->
    woody_context:ctx().
request_context_to_woody_context(null) ->
    woody_context:new();
request_context_to_woody_context(ReqCtx) ->
    mg_woody_api_utils:opaque_to_woody_context(ReqCtx).

%%
%% logging
%%
% -spec handle_log_event(options(), mg:ns(), mg:id(), mg_events_machine:log_event()) ->
%     ok.
% handle_log_event(_Options, NS, ID, LogEvent) ->
%     ok = mg_woody_api_logger:log(append_machine_id(NS, ID, format_log_event(LogEvent))).

% -spec append_machine_id(mg:ns(), mg:id(), mg_woody_api_logger:log_msg()) ->
%     mg_woody_api_logger:log_msg().
% append_machine_id(NS, ID, {Level, Msg, Meta}) ->
%     {Level, Msg, [{machine_ns, NS}, {machine_id, ID} | Meta]}.

% -spec format_log_event(mg_events_machine:log_event()) ->
%     mg_woody_api_logger:log_msg().
% format_log_event({loading_failed, Reason}) ->
%     {error, {"loading failed ~p", [Reason]}, []};
% format_log_event({processing_failed, Reason, WoodyContext}) ->
%     add_woody_context(
%         WoodyContext,
%         {error, {"processing failed ~p", [Reason]}, []}
%     );
% format_log_event({timer_handling_failed, Reason}) ->
%     {error, {"timer handling failed ~p", [Reason]}, []};
% format_log_event({retryable error, Reason, RetryTimeout, WoodyContext}) ->
%     add_woody_context(
%         WoodyContext,
%         {warning, {"retryable error ~p, retrying in ~p msec", [Reason, RetryTimeout]}}
%     );
% format_log_event(UnknownLogEvent) ->
%     {warning, {"unknown log event ~p", [UnknownLogEvent]}, []}.

% -spec add_woody_context(woody_context:ctx(), mg_woody_api_logger:log_msg()) ->
%     mg_woody_api_logger:log_msg().
% add_woody_context(WoodyContext, {Level, Msg, Meta}) ->
%     #{rpc_id := RPCID} = mg_woody_api_utils:opaque_to_woody_context(WoodyContext),
%     {Level, Msg, mg_woody_api_logger:woody_rpc_id_to_meta(RPCID) ++ Meta}.
