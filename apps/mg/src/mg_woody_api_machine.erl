-module(mg_woody_api_machine).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% mg_machine handler
-behaviour(mg_machine).
-export([process_signal/2, process_call/2]).

%%
%% mg_machine handler
%%
-spec process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, SignalArgs) ->
    call_processor(Options, processSignal, [SignalArgs]).

-spec process_call(_Options, mg:call_args()) ->
    mg:call_result().
process_call(Options, CallArgs) ->
    call_processor(Options, processCall, [CallArgs]).

%%
%% local
%%
-spec call_processor(_URL, atom(), list(_)) ->
    _.
call_processor(URL, Function, Args) ->
    {{ok, R}, _} =
        woody_client:call(
            woody_client:new_context(woody_client:make_id(<<"mg">>), mg_woody_api_event_handler),
            {{mg_proto_state_processing_thrift, 'Processor'}, Function, Args},
            #{url => URL}
        ),
    R.
