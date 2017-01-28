-module(mg_event_sink_client).

%% API
-export_type([options/0]).
-export([get_history/3]).

%%
%% API
%%
-type options() :: URL::string().

-spec get_history(options(), mg:id(), mg_events:history_range()) ->
    mg_events:history().
get_history(BaseURL, EventSinkID, Range) ->
    call_service(BaseURL, 'GetHistory', [EventSinkID, Range]).

%%
%% local
%%
-spec call_service(_BaseURL, atom(), [_arg]) ->
    _.
call_service(BaseURL, Function, Args) ->
    WR = woody_client:call(
            {{mg_proto_state_processing_thrift, 'EventSink'}, Function, Args},
            #{
                url           => BaseURL ++ "/v1/event_sink",
                event_handler => {mg_woody_api_event_handler, undefined}
            },
            woody_context:new()
        ),
    case WR of
        {ok, R} ->
            R;
        {exception, Exception} ->
            erlang:throw(Exception)
    end.
