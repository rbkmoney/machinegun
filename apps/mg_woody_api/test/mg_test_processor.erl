-module(mg_test_processor).

-export([start_link   /1]).

%% processor woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

-export_type([processor_function/0]).
-type processor_function() :: fun((term()) -> term()).

%%
%% API
%%

-spec start_link(_Opts) ->
    mg_utils:gen_start_ret().
start_link(Options) ->
    supervisor:start_link(?MODULE, Options).

%%
%% processor woody handler
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
init({Host, Port, Path, Fun}) ->
    SupFlags = #{strategy => one_for_all},
    {ok, {SupFlags, [
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
    ]}}.

-spec invoke_function(signal | call, default_func | processor_function(), term()) -> term().
invoke_function(Type, default_func, _Args) ->
    case Type of
        signal -> {{<<>>, []}, #{timer => undefined, tag => undefined}};
        call -> {<<>>, {<<>>, []}, #{timer => undefined, tag => undefined}}
    end;
invoke_function(_Type, Func, Args) ->
    Func(Args).
