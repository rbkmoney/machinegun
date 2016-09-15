-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_processor handler
-behaviour(mg_processor).
-export_type([options/0]).
-export([process_signal/2, process_call/2]).

%%
%% mg_processor handler
%%
-type options() :: woody_t:url().

-spec process_signal(options(), mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, SignalArgs) ->
    {SignalResult, _} =
        call_processor(
            Options,
            woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler),
            'ProcessSignal',
            [mg_woody_api_packer:pack(signal_args, SignalArgs)]
        ),
    mg_woody_api_packer:unpack(signal_result, SignalResult).

-spec process_call(options(), mg:call_args()) ->
    mg:call_result().
process_call(Options, CallArgs) ->
    {SignalResult, _} =
        call_processor(
            Options,
            % TODO woody context
            woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler),
            'ProcessCall',
            [mg_woody_api_packer:pack(call_args, CallArgs)]
        ),
    mg_woody_api_packer:unpack(call_result, SignalResult).

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

