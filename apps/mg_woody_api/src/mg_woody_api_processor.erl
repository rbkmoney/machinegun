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
    woody_client:child_spec(Options).

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

-spec call_processor(options(), mg_events_machine:request_context(), atom(), list(_)) ->
    _Result.
call_processor(Options, ReqCtx, Function, Args) ->
    % TODO сделать нормально!
    RecvTimeout = proplists:get_value(recv_timeout, maps:get(transport_opts, Options, #{}), 5000),
    {ok, TRef} = timer:kill_after(RecvTimeout + 3000),
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
