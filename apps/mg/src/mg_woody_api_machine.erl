-module(mg_woody_api_machine).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_machine handler
-behaviour(mg_machine).
-export([process_signal/2, process_call/3]).

%%
%% mg_machine handler
%%
-spec process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, SignalArgs) ->
    {R, _} =
        call_processor(
            Options,
            woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler),
            processSignal,
            [SignalArgs]
        ),
    R.

-spec process_call(_Options, mg:call_args(), mg:call_context()) ->
    mg:call_result().
process_call(Options, Call, WoodyContext) ->
    call_processor(
        Options,
        WoodyContext,
        processCall,
        [Call]
    ).

%%
%% local
%%
-spec call_processor(_URL, woody_client:context(), atom(), list(_)) ->
    _.
call_processor(URL, WoodyContext, Function, Args) ->
    {{ok, Result}, NewWoodyContext} =
        woody_client:call(
            WoodyContext,
            {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
            #{url => URL}
        ),
    {Result, NewWoodyContext}.
