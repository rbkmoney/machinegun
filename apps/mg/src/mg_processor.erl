-module(mg_processor).

-export([process_signal/2]).
-export([process_call  /3]).

%%
%% API
%%
-callback process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
-callback process_call(_Options, mg:call_args(), mg:call_context()) ->
    {mg:call_result(), mg:call_context()}.


-spec process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, Args) ->
    mg_utils:apply_mod_opts(Options, process_signal, [Args]).

-spec process_call(_Options, mg:call_args(), mg:call_context()) ->
    {mg:call_result(), mg:call_context()}.
process_call(Options, Args, Context) ->
    mg_utils:apply_mod_opts(Options, process_call, [Args, Context]).
