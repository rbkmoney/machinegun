-module(mg_processor).

%% API
-export([process_signal/3]).
-export([process_call  /3]).

%%
%% API
%%
-callback process_signal(_Options, mg:id(), mg:signal_args()) ->
    mg:signal_result().
-callback process_call(_Options, mg:id(), mg:call_args()) ->
    mg:call_result().

%%

-spec process_signal(mg_utils:mod_opts(), mg:id(), mg:signal_args()) ->
    mg:signal_result().
process_signal(ModOpts, ID, Args) ->
    mg_utils:apply_mod_opts(ModOpts, process_signal, [ID, Args]).

-spec process_call(mg_utils:mod_opts(), mg:id(), mg:call_args()) ->
    mg:call_result().
process_call(ModOpts, ID, Args) ->
    mg_utils:apply_mod_opts(ModOpts, process_call, [ID, Args]).
