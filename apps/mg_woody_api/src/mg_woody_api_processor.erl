-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_processor handler
-behaviour(mg_processor).
-export_type([options/0]).
-export([process_signal/3, process_call/3]).

%%
%% mg_processor handler
%%
-type options() :: woody_t:url().

-spec process_signal(options(), mg_woody_api:id(), mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, _, {SignalAndWoodyContext, Machine}) ->
    {Signal, WoodyContext} = signal_and_woody_context(SignalAndWoodyContext),
    {SignalResult, _} =
        call_processor(
            Options,
            WoodyContext,
            'ProcessSignal',
            [mg_woody_api_packer:pack(signal_args, {Signal, Machine})]
        ),
    mg_woody_api_packer:unpack(signal_result, SignalResult).

-spec process_call(options(), mg_woody_api:id(), mg:call_args()) ->
    mg:call_result().
process_call(Options, _, {{Call, WoodyContext}, Machine}) ->
    {CallResult, _} =
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
-spec call_processor(options(), woody_client:context(), atom(), list(_)) ->
    _.
call_processor(Options, WoodyContext, Function, Args) ->
    URL = Options,
    woody_client:call(
        WoodyContext,
        {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
        #{url => URL}
    ).

%% TODO такой хак пока в таймауте нет контекста
-spec signal_and_woody_context({mg:signal(), woody_client:context()} | mg:signal()) ->
    {mg:signal(), woody_client:context()}.
signal_and_woody_context(Signal=timeout) ->
    {Signal, woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler)};
signal_and_woody_context({init, ID, {Arg, WoodyContext}}) ->
    {{init, ID, Arg}, WoodyContext};
signal_and_woody_context({repair, {Arg, WoodyContext}}) ->
    {{repair, Arg}, WoodyContext}.
