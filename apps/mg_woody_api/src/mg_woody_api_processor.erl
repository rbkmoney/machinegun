-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_events_machine handler
-behaviour(mg_events_machine).
-export_type([options/0]).
-export([process_signal/2, process_call/2]).

%%
%% mg_events_machine handler
%%
-type options() :: woody_client:options().

-spec process_signal(options(), mg_events_machine:signal_args()) ->
    mg_events_machine:signal_result().
process_signal(Options, {SignalAndWoodyContext, Machine}) ->
    {Signal, WoodyContext} = signal_and_woody_context(SignalAndWoodyContext),
    SignalResult =
        call_processor(
            Options,
            WoodyContext,
            'ProcessSignal',
            [mg_woody_api_packer:pack(signal_args, {Signal, Machine})]
        ),
    mg_woody_api_packer:unpack(signal_result, SignalResult).

-spec process_call(options(), mg_events_machine:call_args()) ->
    mg_events_machine:call_result().
process_call(Options, {{Call, WoodyContext}, Machine}) ->
    CallResult =
        call_processor(
            Options,
            WoodyContext,
            'ProcessCall',
            [mg_woody_api_packer:pack(call_args, {Call, Machine})]
        ),
    mg_woody_api_packer:unpack(call_result, CallResult).

%%
%% local
%%
-spec call_processor(options(), woody_context:ctx(), atom(), list(_)) ->
    _Result.
call_processor(Options, WoodyContext, Function, Args) ->
    % TODO сделать нормально!
    {ok, TRef} = timer:kill_after(maps:get(recv_timeout, Options, 5000) + 3000),
    try
        {ok, R} =
            woody_client:call(
                {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
                Options,
                WoodyContext
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

%% TODO такой хак пока в таймауте нет контекста
-spec signal_and_woody_context({mg_events_machine:signal(), woody_context:ctx()} | mg_events_machine:signal()) ->
    {mg_events_machine:signal(), woody_context:ctx()}.
signal_and_woody_context(Signal=timeout) ->
    {Signal, woody_context:new()};
signal_and_woody_context({init, {Arg, WoodyContext}}) ->
    {{init, Arg}, WoodyContext};
signal_and_woody_context({repair, {Arg, WoodyContext}}) ->
    {{repair, Arg}, WoodyContext}.
