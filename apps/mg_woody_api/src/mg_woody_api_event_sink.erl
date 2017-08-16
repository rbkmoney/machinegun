-module(mg_woody_api_event_sink).

%% API
-export([handler/1]).
-export_type([options/0]).

%% woody handler
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%%
%% API
%%
-type options() :: {[mg:id()], mg_events_sink:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/event_sink", {{mg_proto_state_processing_thrift, 'EventSink'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('GetHistory', [EventSinkID, Range], WoodyContext, {AvaliableEventSinks, Options}) ->
    SinkHistory =
        mg_woody_api_utils:handle_safe_with_retry(
            EventSinkID, mg_woody_api_utils:woody_context_to_opaque(WoodyContext),
            fun() ->
                _ = check_event_sink(AvaliableEventSinks, EventSinkID),
                mg_events_sink:get_history(
                    Options,
                    EventSinkID,
                    mg_woody_api_packer:unpack(history_range, Range)
                )
            end,
            mg_utils:default_deadline(), logger(Options)
        ),
    {ok, mg_woody_api_packer:pack(sink_history, SinkHistory)}.

-spec check_event_sink([mg:id()], mg:id()) ->
    ok | no_return().
check_event_sink(AvaliableEventSinks, EventSinkID) ->
    case lists:member(EventSinkID, AvaliableEventSinks) of
        true ->
            ok;
        false ->
            throw(event_sink_not_found)
    end.

-spec logger(mg_events_sink:options()) ->
    mg_machine_logger:handler().
logger(#{logger := Logger}) ->
    Logger.
