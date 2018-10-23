%%%
%%% Copyright 2017 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(mg_test_processor).

-export([start_link    /1]).
-export([default_result/1]).

%% processor handlers
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

-export_type([processor_function/0]).
-type processor_function() :: fun((mg:signal_args()) -> mg:signal_result()) |
                              fun((mg:call_args()) -> mg:call_result()) | default_func.
-type host_address() :: {integer(), integer(), integer(), integer()}.


%%
%% API
%%
-spec start_link({host_address(), integer(), string(), {processor_function(), processor_function()}}) ->
    mg_utils:gen_start_ret().
start_link({Host, Port, Path, Fun}) ->
    Flags = #{strategy => one_for_all},
    ChildsSpecs = [
        woody_server:child_spec(
            ?MODULE,
            #{
                ip            => Host,
                port          => Port,
                net_opts      => [],
                event_handler => {mg_woody_api_event_handler, undefined},
                handlers      => [{Path, {{mg_proto_state_processing_thrift, 'Processor'}, {?MODULE, Fun}}}]
            }
        )
    ],
    mg_utils_supervisor_wrapper:start_link(Flags, ChildsSpecs).

%%
%% processor handlers
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), {processor_function(), processor_function()}) ->
                         {ok, _Result} | no_return().
handle_function('ProcessSignal', [Args], _WoodyContext, {SignalFun, _CallFun}) ->
    UnpackedArgs = mg_woody_api_packer:unpack(signal_args, Args),
    Result = invoke_function(signal, SignalFun, UnpackedArgs),
    {ok, mg_woody_api_packer:pack(signal_result, Result)};
handle_function('ProcessCall', [Args], _WoodyContext, {_SignalFun, CallFun}) ->
    UnpackedArgs = mg_woody_api_packer:unpack(call_args, Args),
    Result = invoke_function(call, CallFun, UnpackedArgs),
    {ok, mg_woody_api_packer:pack(call_result, Result)}.

%%
%% helpers
%%
-spec invoke_function(signal, processor_function(), term()) -> mg:signal_result()
                   ; (call,   processor_function(), term()) -> mg:call_result().
invoke_function(Type, default_func, _Args) ->
    default_result(Type);
invoke_function(_Type, Func, Args) ->
    Func(Args).

-spec default_result(signal) -> mg:signal_result()
                  ; (call  ) -> mg:call_result().
default_result(signal) ->
    {{default_content(), []}, #{timer => undefined, tag => undefined}};
default_result(call) ->
    {<<>>, {default_content(), []}, #{timer => undefined, tag => undefined}}.

-spec default_content() -> mg_events:content().
default_content() ->
    {#{}, <<>>}.
