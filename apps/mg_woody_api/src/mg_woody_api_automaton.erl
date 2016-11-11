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
    {"/v1/automaton",
        {{mg_proto_state_processing_thrift, 'Automaton'}, ?MODULE, Options}}.

%%
%% woody handler
%%
%% TODO errors
-define(safe_handle(Expr, WoodyContext),
    try
        Expr
    catch throw:Error ->
        throw({map_error(Error), WoodyContext})
    end
).

-spec map_error(_) ->
    _.
map_error(machine_not_found) ->
    #'MachineNotFound'{};
map_error(machine_already_exist) ->
    #'MachineAlreadyExists'{};
map_error(machine_failed) ->
    #'MachineFailed'{};
map_error(Other) ->
    Other.

%% в вуди сейчас Options — это list()
-dialyzer({nowarn_function, handle_function/4}).
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), options()) ->
    {ok | term(), woody_client:context()} | no_return().

handle_function('Start', {NS, ID, Args}, WoodyContext, Options) ->
    ok = ?safe_handle(
            mg_machine_complex:start(
                get_ns_options(NS, Options),
                unpack(id, ID),
                {unpack(args, Args), WoodyContext}
            ),
            WoodyContext
        ),
    {ok, WoodyContext};

handle_function('Repair', {MachineDesc, Args}, WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    ok = ?safe_handle(
            mg_machine_complex:repair(
                get_ns_options(NS, Options),
                Ref,
                {unpack(args, Args), WoodyContext},
                Range
            ),
            WoodyContext
        ),
    {ok, WoodyContext};

handle_function('Call', {MachineDesc, Args}, WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    Response =
        ?safe_handle(
            mg_machine_complex:call(
                get_ns_options(NS, Options),
                Ref,
                {unpack(args, Args), WoodyContext},
                Range
            ),
            WoodyContext
        ),
    {pack(call_response, Response), WoodyContext};

handle_function('GetMachine', {MachineDesc}, WoodyContext, Options) ->
    {NS, Ref, Range} = unpack(machine_descriptor, MachineDesc),
    History =
        ?safe_handle(
            mg_machine_complex:get_machine(get_ns_options(NS, Options), Ref, Range),
            WoodyContext
        ),
    {pack(machine, History), WoodyContext}.

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
            throw(#'NamespaceNotFound'{})
    end.
