-module(mg_woody_api_logger).

-export_type([log_msg/0]).
-export_type([msg    /0]).
-export_type([meta   /0]).
-export_type([level  /0]).
-export([log/1]).
-export([woody_rpc_id_to_meta/1]).

-behaviour(mg_machine_logger).
-export([handle_machine_logging_event/2]).

-type log_msg() :: {level(), msg(), meta()}.
-type msg() :: expanded_msg() | string().
-type expanded_msg() :: {Format::string(), Args::list()}.
-type meta() :: list({_, _}). % there is no such exported type in lager
-type level() :: lager:log_level().

-spec log(log_msg()) ->
    ok.
log({Level, Msg, Meta}) ->
    {MsgFormat, MsgArgs} = expand_msg(Msg),
    ok = lager:log(Level, [{pid, erlang:self()} | Meta], MsgFormat, MsgArgs).

-spec expand_msg(msg()) ->
    expanded_msg().
expand_msg(Msg={_, _}) ->
    Msg;
expand_msg(Str) ->
    {Str, []}.

-spec woody_rpc_id_to_meta(woody:rpc_id()) ->
    meta().
woody_rpc_id_to_meta(RPCID) ->
    maps:to_list(RPCID).

-spec handle_machine_logging_event(_, mg_machine_logger:event()) ->
    ok.
handle_machine_logging_event(_, {NS, ID, ReqCtx, SubEvent}) ->
    ok = log(append_machine_id(NS, ID, add_woody_context(ReqCtx, format_machine_log_event(SubEvent)))).

-spec add_woody_context(mg:request_context(), log_msg()) ->
    log_msg().
add_woody_context(null, Msg) ->
    % в старых данных конекста ещё нет
    Msg;
add_woody_context(ReqCtx, {Level, Msg, Meta}) ->
    #{rpc_id := RPCID} = mg_woody_api_utils:opaque_to_woody_context(ReqCtx),
    {Level, Msg, woody_rpc_id_to_meta(RPCID) ++ Meta}.

-spec append_machine_id(mg:ns(), mg:id(), log_msg()) ->
    log_msg().
append_machine_id(NS, ID, {Level, Msg, Meta}) ->
    {Level, Msg, [{machine_ns, NS}, {machine_id, ID} | Meta]}.

-spec format_machine_log_event(mg_machine_logger:sub_event()) ->
    log_msg().
format_machine_log_event({loading_failed, Exception}) ->
    {error, {"loading failed ~p", [Exception]}, []};
format_machine_log_event({machine_failed, Exception}) ->
    {error, {"machine failed ~p", [Exception]}, []};
format_machine_log_event({timer_handling_failed, Exception}) ->
    {error, {"timer handling failed ~p", [Exception]}, []};
format_machine_log_event({resuming_interrupted_failed, Exception}) ->
    {error, {"resuming interrupted failed ~p", [Exception]}, []};
format_machine_log_event({transient_error, Exception}) ->
    {warning, {"transient error ~p", [Exception]}, []};
format_machine_log_event({retrying, RetryTimeout}) ->
    {warning, {"retrying in ~p msec", [RetryTimeout]}, []};
format_machine_log_event(UnknownLogEvent) ->
    {warning, {"unknown log event ~p", [UnknownLogEvent]}, []}.
