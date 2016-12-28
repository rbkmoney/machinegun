-module(mg_test_processor).

-export([start_link/1]).

%% processor woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% supervisor callbacks
-behaviour(supervisor).
-export([init/1]).

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
-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), _Options) ->
                         {ok, _Result} | no_return().
handle_function('ProcessSignal', [_SignalArgs], _WoodyContext, Fun) ->
    Result = Fun(),
    {ok, Result};
handle_function('ProcessCall', [_CallArgs], _WoodyContext, _Fun) ->
    {ok, ok}.

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
