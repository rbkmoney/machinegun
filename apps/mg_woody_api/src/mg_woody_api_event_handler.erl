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
handle_event(Event, RpcID, Meta, _) ->
    {Level, {MsgFormat, MsgArgs}} = woody_event_handler:format_event(Event, Meta, RpcID),
    ok = lager:log(Level, [{pid, erlang:self()}], MsgFormat, MsgArgs).
