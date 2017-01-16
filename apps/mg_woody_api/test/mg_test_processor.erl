-module(mg_test_processor).

-export([start_link    /1]).
-export([default_result/1]).

%% processor handlers
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

-export_type([processor_function/0]).
-type processor_function() :: fun((mg:signal_args() | mg:call_args()) -> processor_function_result()).
-type processor_function_result() :: mg:signal_result() | mg:call_result().


%%
%% API
%%
-spec start_link(_Opts) ->
    mg_utils:gen_start_ret().
start_link({Host, Port, Path, Fun}) ->
    Flags = #{strategy => one_for_all},
    ChildsSpecs = [
        woody_server:child_spec(
            ?MODULE,
            #{
                ip            => Host,
                port          => Port,
                net_opts      => #{},
                event_handler => {mg_woody_api_event_handler, undefined},
                handlers      => [{Path, {{mg_proto_state_processing_thrift, 'Processor'}, {?MODULE, Fun}}}]
            }
        )
    ],
    mg_utils_supervisor_wrapper:start_link(Flags, ChildsSpecs).

%%
%% processor handlers
%%
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), processor_function()) ->
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
%% supervisor callbacks
%%
-spec init(_Opts) ->
    mg_utils:supervisor_ret().
init(Opts) ->
    mg_utils_supervisor_wrapper:init(Opts).

%%
%% helpers
%%
-spec invoke_function(signal | call, default_func | processor_function(), term()) -> processor_function_result().
invoke_function(Type, default_func, _Args) ->
    default_result(Type);
invoke_function(_Type, Func, Args) ->
    Func(Args).

-spec default_result(signal | call) ->
    processor_function_result().
default_result(signal) ->
    {{<<>>, []}, #{timer => undefined, tag => undefined}};
default_result(call) ->
    {<<>>, {<<>>, []}, #{timer => undefined, tag => undefined}}.
