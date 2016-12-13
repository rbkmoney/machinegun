-module(mg_woody_api_event_sink).

%% API
-export([handler/1]).
-export_type([options/0]).

%% woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%%
%% API
%%
-type options() :: {[mg_machine_event_sink:id()], mg_machine_event_sink:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/event_sink", {{mg_proto_state_processing_thrift, 'EventSink'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    _Result | no_return().

handle_function('GetHistory', [EventSinkID, Range], _WoodyContext, {AvaliableEventSinks, Options}) ->
    _ = check_event_sink(AvaliableEventSinks, EventSinkID),
    SinkHistory =
        mg_machine_event_sink:get_history(
            Options,
            EventSinkID,
            mg_woody_api_packer:unpack(history_range, Range)
        ),
    mg_woody_api_packer:pack(sink_history, SinkHistory).

-spec check_event_sink([mg_machine_event_sink:id()], mg_machine_event_sink:id()) ->
    ok | no_return().
check_event_sink(AvaliableEventSinks, EventSinkID) ->
    case lists:member(EventSinkID, AvaliableEventSinks) of
        true ->
            ok;
        false ->
            throw(#'EventSinkNotFound'{})
    end.
