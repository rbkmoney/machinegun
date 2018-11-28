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

-export([start/3]).
-export([start_link/3]).
-export([default_result/2]).

%% processor handlers
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

-export_type([processor_functions/0]).
-export_type([modernizer_function/0]).

-type processor_signal_function() ::
    fun((mg:signal_args()) -> mg:signal_result()) | default_func.

-type processor_call_function() ::
    fun((mg:call_args()) -> mg:call_result()) | default_func.

-type modernizer_function() ::
    fun((mg_events_modernizer:machine_event()) -> mg_events_modernizer:modernized_event_body()) | default_func.

-type processor_functions() :: {processor_signal_function(), processor_call_function()}.
-type host_address() :: {integer(), integer(), integer(), integer()}.

-type options() :: #{
    processor  => {string(), processor_functions()},
    modernizer => {string(), modernizer_function()}
}.

%%
%% API
%%
-spec start(host_address(), integer(), options()) ->
    mg_utils:gen_start_ret().
start(Host, Port, Options) ->
    case start_link(Host, Port, Options) of
        {ok, ProcessorPid} ->
            true = erlang:unlink(ProcessorPid),
            {ok, ProcessorPid};
        ErrorOrIgnore ->
            ErrorOrIgnore
    end.

-spec start_link(host_address(), integer(), options()) ->
    mg_utils:gen_start_ret().
start_link(Host, Port, Options) ->
    Flags = #{strategy => one_for_all},
    ChildsSpecs = [
        woody_server:child_spec(
            ?MODULE,
            #{
                ip            => Host,
                port          => Port,
                event_handler => {mg_woody_api_event_handler, mg_woody_api_pulse},
                handlers      => maps:values(maps:map(
                    fun
                        (processor, {Path, Functions}) ->
                            {Path, {{mg_proto_state_processing_thrift, 'Processor'}, {?MODULE, Functions}}};
                        (modernizer, {Path, Function}) ->
                            {Path, {{mg_proto_state_processing_thrift, 'Modernizer'}, {?MODULE, Function}}}
                    end,
                    Options
                ))
            }
        )
    ],
    mg_utils_supervisor_wrapper:start_link(Flags, ChildsSpecs).

%%
%% processor handlers
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), processor_functions()) ->
                         {ok, _Result} | no_return().
handle_function('ProcessSignal', [Args], _WoodyContext, {SignalFun, _CallFun}) ->
    UnpackedArgs = mg_woody_api_packer:unpack(signal_args, Args),
    Result = invoke_function(signal, SignalFun, UnpackedArgs),
    {ok, mg_woody_api_packer:pack(signal_result, Result)};
handle_function('ProcessCall', [Args], _WoodyContext, {_SignalFun, CallFun}) ->
    UnpackedArgs = mg_woody_api_packer:unpack(call_args, Args),
    Result = invoke_function(call, CallFun, UnpackedArgs),
    {ok, mg_woody_api_packer:pack(call_result, Result)};

handle_function('ModernizeEvent', [Args], _WoodyContext, ModernizeFun) ->
    MachineEvent = mg_woody_api_packer:unpack(machine_event, Args),
    Result = invoke_function(modernize, ModernizeFun, MachineEvent),
    {ok, mg_woody_api_packer:pack(modernize_result, Result)}.

%%
%% helpers
%%
-spec invoke_function(signal,    processor_signal_function(), term()) -> mg:signal_result()
                   ; (call,      processor_call_function(), term())   -> mg:call_result()
                   ; (modernize, modernizer_function(), term())       -> mg_events_modernizer:modernized_event_body().
invoke_function(Type, default_func, Args) ->
    default_result(Type, Args);
invoke_function(_Type, Func, Args) ->
    Func(Args).

-spec default_result(signal    , term()) -> mg:signal_result()
                  ; (call      , term()) -> mg:call_result()
                  ; (modernize , term()) -> mg_events_modernizer:modernized_event_body().
default_result(signal, _Args) ->
    {{default_content(), []}, #{timer => undefined, tag => undefined}};
default_result(call, _Args) ->
    {<<>>, {default_content(), []}, #{timer => undefined, tag => undefined}};
default_result(modernize, #{event := #{body := Body}}) ->
    Body.

-spec default_content() -> mg_events:content().
default_content() ->
    {#{}, <<>>}.
