-module(mg_woody_api_logger).

-export_type([log_msg/0]).
-export_type([msg    /0]).
-export_type([meta   /0]).
-export_type([level  /0]).
-export([log/1]).
-export([woody_rpc_id_to_meta/1]).

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
