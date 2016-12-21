-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_processor handler
-behaviour(mg_processor).
-export_type([options/0]).
-export([process_signal/2, process_call/2]).

%%
%% mg_processor handler
%%
-type options() :: woody_client:options().

-spec process_signal(options(), mg:signal_args()) ->
    mg:signal_result().
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

-spec process_call(options(), mg:call_args()) ->
    mg:call_result().
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
    end.

%% TODO такой хак пока в таймауте нет контекста
-spec signal_and_woody_context({mg:signal(), woody_context:ctx()} | mg:signal()) ->
    {mg:signal(), woody_context:ctx()}.
signal_and_woody_context(Signal=timeout) ->
    {Signal, woody_context:new()};
signal_and_woody_context({init, {Arg, WoodyContext}}) ->
    {{init, Arg}, WoodyContext};
signal_and_woody_context({repair, {Arg, WoodyContext}}) ->
    {{repair, Arg}, WoodyContext}.
