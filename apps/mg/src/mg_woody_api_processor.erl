-module(mg_woody_api_processor).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_processor handler
-behaviour(mg_processor).
-export([process_signal/2, process_call/3]).

%%
%% mg_processor handler
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
-spec call_processor(_Options, woody_client:context(), atom(), list(_)) ->
    _.
call_processor(Options, WoodyContext, Function, Args) ->
    URL = Options,
    {Result, NewWoodyContext} =
        woody_client:call_safe(
            WoodyContext,
            {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
            #{url => URL}
        ),
    %% TODO woody_context
    case Result of
        {ok, Value} ->
            {Value, NewWoodyContext};
        {error, Reason} ->
            mg_processor:throw_error(Reason)
    end.
