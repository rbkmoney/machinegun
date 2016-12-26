-module(mg_test_processor).

-export([start/1]).

%% processor woody handler
-export([handle_function/4]).

%% processor woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%%
%% API
%%
start(Args) ->
    erlang:log_info("Start processor with args: ~p", [Args]),
    _NS = maps:get(ns, Args),
    _Functors = maps:get(functors, Args),
    ok.

%%
%% processor woody handler
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), _Options) ->
                             {ok, _Result} | no_return().
handle_function('ProcessSignal', [_SignalArgs], _WoodyContext, _Options) ->
    ok;
handle_function('ProcessCall', [_CallArgs], _WoodyContext, _Options) ->
    ok.
%%
%% local
%%
