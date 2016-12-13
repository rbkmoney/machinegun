-module(mg_woody_api_event_handler).

%% woody_event_handler callbacks
-behaviour(woody_event_handler).
-export([handle_event/4]).

%%
%% woody_event_handler callbacks
%%
-spec handle_event(EventType, RpcID, EventMeta, _)
    -> _ when
        EventType :: woody_event_handler:event     (),
        RpcID     :: woody              :rpc_id    (),
        EventMeta :: woody_event_handler:event_meta().
handle_event(EventType, RpcID, Meta, _) ->
    {Level, Msg} = format_event(EventType, Meta),
    _ = log(EventType, RpcID, Meta, {Level, append_msg(format_rpc_id(RpcID), Msg)}).

%%
%% local
%%
%% TODO описать нормально
-type msg    () :: {_, _    }.
-type log_msg() :: {_, msg()}.

-spec lager_meta(woody_event_handler:event(), woody:rpc_id(), woody_event_handler:event_meta()) ->
    list().
lager_meta(EventType, RpcID, Meta) ->
    maps:to_list(add_rpc_id(RpcID, Meta#{woody_role => server, woody_event => EventType})).

-spec add_rpc_id(woody:rpc_id(), woody_event_handler:event_meta()) ->
    #{}.
add_rpc_id(RpcID, Meta) ->
    maps:merge(RpcID, Meta).

-spec log(woody_event_handler:event(), woody:rpc_id(), woody_event_handler:event_meta(), log_msg()) ->
    ok.
log(EventType, RpcID, Meta, {Level, {Format, Args}}) ->
    ok = lager:log(Level, lager_meta(EventType, RpcID, Meta), Format, Args).

%%
%% events formatting
%%
-spec format_event(woody_event_handler:event(), woody_event_handler:event_meta()) ->
    log_msg().
format_event('call service', Meta) ->
    {info, append_msg({"[client] calling ", []}, format_service_request(Meta))};
format_event('service result', #{status:=error, result:=Result}) ->
    {warning, {"[client] error while handling request ~p", [Result]}};
format_event('service result', #{status:=ok, result:=Result}) ->
    {info, {"[client] request handled successfully ~p", [Result]}};
format_event('client send', #{url:=URL}) ->
    {debug, {"[client] sending request to ~s", [URL]}};
format_event('client receive', #{status:=ok, code:=Code}) ->
    {debug, {"[client] received response with code ~p", [Code]}};
format_event('client receive', #{status:=error, code:=Code}) ->
    {warning, {"[client] received response with code ~p", [Code]}};
format_event('client receive', #{status:=error, reason:=Reason}) ->
    {warning, {"[client] sending request error ~p", [Reason]}};
format_event('server receive', #{url:=URL, status:=ok}) ->
    {debug, {"[server] request to ~s received", [URL]}};
format_event('server receive', #{url:=URL, status:=error, reason:=Reason}) ->
    {debug, {"[server] request to ~s unpacking error ~p", [URL, Reason]}};
format_event('server send', #{status:=ok, code:=Code}) ->
    {debug, {"[server] response send with code ~p", [Code]}};
format_event('server send', #{status:=error, code:=Code}) ->
    {warning, {"[server] response send with code ~p", [Code]}};
format_event('server send', #{status:=error, reason:=Reason}) ->
    {warning, {"[server] request handling error ~p", [Reason]}};
format_event('invoke service handler', Meta) ->
    {info, append_msg({"[server] handling ", []}, format_service_request(Meta))};
format_event('service handler result', #{status:=ok, result:={Result, _}}) ->
    {info, {"[server] handling result ~p", [Result]}};
format_event('service handler result', #{status:=error, class:=throw, reason:=Reason}) ->
    {info,  {"[server] handling result logic error: ~p", [Reason]}};
format_event('service handler result', #{status:=error, class:=Class, reason:=Reason, stack:=Stacktrace}) ->
    {warning,  append_msg({"[server] handling exception ", []}, format_exception({Class, Reason, Stacktrace}))};
format_event('thrift error', #{stage:=Stage, reason:=Reason}) ->
    {warning, {" thrift error ~p, ~p", [Stage, Reason]}};
format_event('internal error', #{error:=Error, reason:=Reason}) ->
    {warning, {" internal error ~p, ~p", [Error, Reason]}};
format_event('trace_event', #{event:=Event}) ->
    {debug, {"trace ~s", [Event]}};
format_event(UnknownEventType, Meta) ->
    {warning, {" unknown woody event type '~s' with meta ~p", [UnknownEventType, Meta]}}.

-spec format_exception(_) -> % TODO exception type
    msg().
format_exception({Class, Reason, Stacktrace}) ->
    {"~s:~p ~s", [Class, Reason, genlib_format:format_stacktrace(Stacktrace, [newlines])]}.

-spec format_rpc_id(woody:rpc_id()) ->
    msg().
format_rpc_id(#{span_id:=Span, trace_id:=Trace, parent_id:=Parent}) ->
    {"[~s ~s ~s]", [Trace, Parent, Span]}.

-spec format_service_request(woody_event_handler:event_meta()) ->
    msg().
format_service_request(#{service:=Service, function:=Function, args:=Args}) ->
    {ArgsFormat, ArgsArgs} = format_args(Args),
    {"~s:~s(" ++ ArgsFormat ++ ")", [Service, Function] ++ ArgsArgs}.

-spec format_args(undefined | term() | tuple()) ->
    msg().
format_args(undefined) ->
    {"", []};
format_args(Args) when is_tuple(Args) ->
    format_args(tuple_to_list(Args));
format_args([]) ->
    {"", []};
format_args([FirstArg | Args]) ->
    lists:foldl(
        fun(Arg, AccMsg) ->
            append_msg(append_msg(AccMsg, {",", []}), format_arg(Arg))
        end,
        format_arg(FirstArg),
        Args
    ).

-spec format_arg(term()) ->
    msg().
format_arg(Arg) ->
    {"~p", [Arg]}.

-spec append_msg(msg(), msg()) ->
    msg().
append_msg({F1, A1}, {F2, A2}) ->
    {F1 ++ F2, A1 ++ A2}.
