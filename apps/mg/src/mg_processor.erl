-module(mg_processor).

-export_type([error       /0]).
-export_type([thrown_error/0]).

-export([process_signal/2]).
-export([process_call  /2]).

-export([throw_error   /1]).

%%
%% API
%%
-type error       () :: term().
-type thrown_error() :: {processor, error()}.

%%

-callback process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
-callback process_call(_Options, mg:call_args()) ->
    mg:call_result().

%%

-spec process_signal(_Options, mg:signal_args()) ->
    mg:signal_result().
process_signal(Options, Args) ->
    mg_utils:apply_mod_opts(Options, process_signal, [Args]).

-spec process_call(_Options, mg:call_args()) ->
    mg:call_result().
process_call(Options, Args) ->
    mg_utils:apply_mod_opts(Options, process_call, [Args]).


%% все ошибки из модулей с поведением mg_processor должны кидаться через эту функцию
-spec throw_error(error()) ->
    no_return().
throw_error(Error) ->
    erlang:throw({processor, Error}).
