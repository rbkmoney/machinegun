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
-type options() :: {[mg_event_sink:id()], mg_event_sink:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/event_sink", {{mg_proto_state_processing_thrift, 'EventSink'}, ?MODULE, Options}}.

%%
%% woody handler
%%
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), options()) ->
    {term(), woody_client:context()} | no_return().

handle_function('GetHistory', {EventSinkID, Range}, WoodyContext, {AvaliableEventSinks, Options}) ->
    _ = check_event_sink(AvaliableEventSinks, EventSinkID, WoodyContext),
    SinkHistory = mg_event_sink:get_history(Options, EventSinkID, mg_woody_api_packer:unpack(history_range, Range)),
    {mg_woody_api_packer:pack(sink_history, SinkHistory), WoodyContext}.

-spec check_event_sink([mg_event_sink:id()], mg_event_sink:id(), woody_client:context()) ->
    ok | no_return().
check_event_sink(AvaliableEventSinks, EventSinkID, WoodyContext) ->
    case lists:member(EventSinkID, AvaliableEventSinks) of
        true ->
            ok;
        false ->
            throw({#'EventSinkNotFound'{}, WoodyContext})
    end.
