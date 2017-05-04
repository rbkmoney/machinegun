-module(mg_woody_api_utils).
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").

%% API
-export([handle_safe/1]).
-export([opaque_to_woody_context/1]).
-export([woody_context_to_opaque/1]).

%%
%% API
%%
-spec handle_safe(fun(() -> R)) ->
    R.
handle_safe(F) ->
    try
        F()
    catch throw:Error ->
        case map_error(Error) of
            {throw, NewError} ->
                erlang:throw(NewError);
            {woody_error, {WoodyErrorClass, Description}} ->
                BinaryDescription = erlang:list_to_binary(io_lib:format("~9999p", [Description])),
                woody_error:raise(system, {internal, WoodyErrorClass, BinaryDescription})
        end
    end.

-spec map_error(mg_machine:thrown_error() | namespace_not_found) ->
    {throw, _} | {woody_error, {woody_error:class(), _Details}}.
map_error(machine_not_found    ) -> {throw, #'MachineNotFound'     {}};
map_error(machine_already_exist) -> {throw, #'MachineAlreadyExists'{}};
map_error(machine_failed       ) -> {throw, #'MachineFailed'       {}};
map_error(namespace_not_found  ) -> {throw, #'NamespaceNotFound'   {}};
map_error(event_sink_not_found ) -> {throw, #'EventSinkNotFound'   {}};

% может Reason не прокидывать дальше?
map_error({transient, Reason}) -> {woody_error, {resource_unavailable, Reason}};
map_error(Reason={timeout, _}) -> {woody_error, {result_unknown      , Reason}};

map_error(UnknownError) -> erlang:error(badarg, [UnknownError]).

%%
%% packer to opaque
%%
-spec opaque_to_woody_context(mg_storage:opaque()) ->
    woody_context:ctx().
opaque_to_woody_context([1, RPCID, ContextMeta]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID),
        meta   => ContextMeta
    };
opaque_to_woody_context([1, RPCID]) ->
    #{
        rpc_id => opaque_to_woody_rpc_id(RPCID)
    }.

-spec woody_context_to_opaque(woody_context:ctx()) ->
    mg_storage:opaque().
woody_context_to_opaque(#{rpc_id := RPCID, meta := ContextMeta}) ->
    [1, woody_rpc_id_to_opaque(RPCID), ContextMeta];
woody_context_to_opaque(#{rpc_id := RPCID}) ->
    [1, woody_rpc_id_to_opaque(RPCID)].

-spec woody_rpc_id_to_opaque(woody:rpc_id()) ->
    mg_storage:opaque().
woody_rpc_id_to_opaque(#{span_id := SpanID, trace_id := TraceID, parent_id := ParentID}) ->
    [SpanID, TraceID, ParentID].

-spec opaque_to_woody_rpc_id(mg_storage:opaque()) ->
    woody:rpc_id().
opaque_to_woody_rpc_id([SpanID, TraceID, ParentID]) ->
    #{span_id => SpanID, trace_id => TraceID, parent_id => ParentID}.
