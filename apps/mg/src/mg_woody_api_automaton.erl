-module(mg_woody_api_automaton).

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
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), _Options) ->
    {{ok, term()}, woody_client:context()} | no_return().

handle_function('Start', {NS, ID, Args}, WoodyContext, Options) ->
    ok = mg_machine:start(get_options(NS, Options), ID, Args),
    {ok, WoodyContext};

handle_function('Repair', {NS, Ref, Args}, WoodyContext, Options) ->
    ok = mg_machine:repair(get_options(NS, Options), Ref, Args),
    {ok, WoodyContext};

handle_function('Call', {NS, Ref, Call}, WoodyContext, Options) ->
    Response = mg_machine:call(get_options(NS, Options), Ref, Call),
    {{ok, Response}, WoodyContext};

handle_function('GetHistory', {NS, Ref, Range}, WoodyContext, Options) ->
    History = mg_machine:get_history(get_options(NS, Options), Ref, Range),
    {{ok, History}, WoodyContext}.

%%
%% local
%%
-spec get_options(_,_) ->
    _.
get_options(NS, Options) ->
    maps:get(NS, Options).
