-module(mg_woody_api_event_sink).

%% API
-export([handler/1]).
-export_type([options/0]).

%% woody handler
-include_lib("mg_proto/include/mg_proto_state_processing_thrift.hrl").
-behaviour(woody_server_thrift_handler).
-export([handle_function/4]).

%% уменьшаем писанину
-import(mg_woody_api_packer, [pack/2, unpack/2]).

%%
%% API
%%
-type options() :: mg_event_sink:options().

-spec handler(options()) ->
    mg_utils:woody_handler().
handler(Options) ->
    {"/v1/event_sink", {{mg_proto_state_processing_thrift, 'EventSink'}, ?MODULE, Options}}.

%%
%% woody handler
%%
-spec handle_function(woody_t:func(), woody_server_thrift_handler:args(), woody_client:context(), options()) ->
    {{ok, term()}, woody_client:context()} | no_return().

handle_function('GetHistory', {Range}, WoodyContext, Options) ->
    SinkHistory = mg_event_sink:get_history(Options, unpack(history_range, Range)),
    {{ok, pack(sink_history, SinkHistory)}, WoodyContext}.
