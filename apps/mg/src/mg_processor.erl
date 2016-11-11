-module(mg_processor).

%% API
-export([process_signal/2]).
-export([process_call  /2]).

%%
%% API
%%
-callback process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
-callback process_call(_Options, mg:call_args()) ->
    mg:call_result().

%%

-spec process_signal(mg_utils:mod_opts(), mg:signal_args()) ->
    mg:signal_result().
process_signal(ModOpts, Args) ->
    mg_utils:apply_mod_opts(ModOpts, process_signal, [Args]).

-spec process_call(mg_utils:mod_opts(), mg:call_args()) ->
    mg:call_result().
process_call(ModOpts, Args) ->
    mg_utils:apply_mod_opts(ModOpts, process_call, [Args]).
