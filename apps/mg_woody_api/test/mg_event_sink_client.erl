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
        {R, _} =
            woody_client:call(
                woody_client:new_context(
                    woody_client:make_id(<<"ev_cl">>),
                    mg_woody_api_event_handler
                ),
                {{mg_proto_state_processing_thrift, 'EventSink'}, Function, Args},
                #{url => BaseURL ++ "/v1/event_sink"}
            ),
        R
    catch throw:{{exception, Exception}, _} ->
        throw(Exception)
    end.
