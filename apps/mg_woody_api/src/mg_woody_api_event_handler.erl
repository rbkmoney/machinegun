-module(mg_woody_api_event_handler).

%% woody_event_handler callbacks
-behaviour(woody_event_handler).
-export([handle_event/4]).

%%
%% woody_event_handler callbacks
%%
-spec handle_event(Event, RpcID, EventMeta, _)
    -> _ when
        Event     :: woody_event_handler:event     (),
        RpcID     :: woody              :rpc_id    (),
        EventMeta :: woody_event_handler:event_meta().
handle_event(Event, RpcID, EventMeta, _) ->
    {Level, Msg} = woody_event_handler:format_event(Event, EventMeta, RpcID),
    mg_woody_api_logger:log({Level, Msg, mg_woody_api_logger:woody_rpc_id_to_meta(RpcID)}).
