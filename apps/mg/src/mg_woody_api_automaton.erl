-module(mg_woody_api_automaton).

%% API
-export_type([options/0]).
-export([handler/1]).

%% woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%%
%% API
%%
-type options() :: #{mg:ns() => mg_machine:options()}.

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
    catch throw:Exception ->
        throw({Exception, WoodyContext})
    end
).


%% в вуди сейчас Options — это list()
-dialyzer({nowarn_function, handle_function/4}).
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), options()) ->
    {{ok, term()}, woody_client:context()} | no_return().

handle_function('Start', {NS, ID, Args}, WoodyContext, Options) ->
    ok = ?safe_handle(
            mg_machine:start(get_ns_options(NS, Options), ID, Args),
            WoodyContext
        ),
    {ok, WoodyContext};

handle_function('Repair', {NS, Ref, Args}, WoodyContext, Options) ->
    ok = ?safe_handle(
            mg_machine:repair(get_ns_options(NS, Options), Ref, Args),
            WoodyContext
        ),
    {ok, WoodyContext};

handle_function('Call', {NS, Ref, Call}, WoodyContext, Options) ->
    {Response, NewWoodyContext} =
        ?safe_handle(
            mg_machine:call(get_ns_options(NS, Options), Ref, Call, WoodyContext),
            WoodyContext
        ),
    {{ok, Response}, NewWoodyContext};

handle_function('GetHistory', {NS, Ref, Range}, WoodyContext, Options) ->
    History =
        ?safe_handle(
            mg_machine:get_history(get_ns_options(NS, Options), Ref, Range),
            WoodyContext
        ),
    {{ok, History}, WoodyContext}.

%%
%% local
%%
-spec get_ns_options(mg:ns(), options()) ->
    mg_machine:options().
get_ns_options(NS, Options) ->
    try
        maps:get(NS, Options)
    catch error:{badkey, NS} ->
        throw(#'NamespaceNotFound'{})
    end.
