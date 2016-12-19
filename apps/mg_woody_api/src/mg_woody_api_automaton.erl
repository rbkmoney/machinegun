-module(mg_woody_api_automaton).

%% API
-export_type([options/0]).
-export([handler/1]).

%% woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: #{mg:ns() => mg_machine_complex:options()}.

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/automaton", {{mg_proto_state_processing_thrift, 'Automaton'}, {?MODULE, Options}}}.

%%
%% woody handler
%%
-define(safe_handle(Expr),
    try
        Expr
    catch throw:Error ->
        case map_error(Error) of
            {throw, NewError} ->
                erlang:throw(NewError);
            {woody_error, {WoodyErrorClass, Description}} ->
                woody_error:raise(system, {internal, WoodyErrorClass, genlib:to_binary(Description)})
        end
    end
).

-spec map_error(mg_machine:thrown_error() | namespace_not_found) ->
    {throw, _} | {woody_error, {woody_error:class(), _Details}}.
map_error(machine_not_found    ) -> {throw, #'MachineNotFound'     {}};
map_error(machine_already_exist) -> {throw, #'MachineAlreadyExists'{}};
map_error(machine_failed       ) -> {throw, #'MachineFailed'       {}};
map_error(namespace_not_found  ) -> {throw, #'NamespaceNotFound'   {}};

map_error({transient, Error}) -> {woody_error, {resource_unavailable, Error}};
map_error( timeout          ) -> {woody_error, {result_unexpected, <<"">>}};

map_error(UnknownError) -> erlang:error(badarg, [UnknownError]).


-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, _Result} | no_return().

handle_function('Start', [NS, ID, Args], WoodyContext, Options) ->
    ok = ?safe_handle(
            mg_machine_complex:start(
                get_ns_options(NS, Options),
                unpack(id, ID),
                {unpack(args, Args), WoodyContext}
            )
        ),
    {ok, ok};

handle_function('Repair', [MachineDesc, Args], WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    ok = ?safe_handle(
            mg_machine_complex:repair(
                get_ns_options(NS, Options),
                Ref,
                {unpack(args, Args), WoodyContext},
                Range
            )
        ),
    {ok, ok};

handle_function('Call', [MachineDesc, Args], WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Response =
        ?safe_handle(
            mg_machine_complex:call(
                get_ns_options(NS, Options),
                Ref,
                {unpack(args, Args), WoodyContext},
                Range
            )
        ),
    {ok, pack(call_response, Response)};

handle_function('GetMachine', [MachineDesc], _WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    History =
        ?safe_handle(
            mg_machine_complex:get_machine(get_ns_options(NS, Options), Ref, Range)
        ),
    {ok, pack(machine, History)}.

%%
%% local
%%
-spec get_ns_options(mg_woody_api:ns(), options()) ->
    mg_machine_complex:options().
get_ns_options(Namespace, Options) ->
    try
        maps:get(Namespace, Options)
    catch
        error:{badkey, Namespace} ->
            throw(namespace_not_found)
    end.
