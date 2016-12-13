-module(mg_event_sink_client).

%% API
-export_type([options/0]).
-export([get_history/3]).

%%
%% API
%%
-type options() :: URL::string().

-spec get_history(options(), mg_machine_event_sink:id(), mg:history_range()) ->
    mg:history().
get_history(BaseURL, EventSinkID, Range) ->
    call_service(BaseURL, 'GetHistory', [EventSinkID, Range]).

%%
%% local
%%
-spec call_service(_BaseURL, atom(), [_arg]) ->
    _.
call_service(BaseURL, Function, Args) ->
    try
        woody_client:call(
            {{mg_proto_state_processing_thrift, 'EventSink'}, Function, Args},
            #{url => BaseURL ++ "/v1/event_sink"},
            woody_context:new(undefined, {mg_woody_api_event_handler, undefined})
        )
    catch throw:{{exception, Exception}, _} ->
        throw(Exception)
    end.
