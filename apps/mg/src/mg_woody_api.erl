%%%
%%% Про woody context.
%%% В процеессе обработки process_signal новый, т.к. нет вызывающего запроса,
%%% в process_call же вызывающий запрос есть и контекст берётся от него.
%%%
-module(mg_woody_api).

%% API
-export([child_spec/1]).


%%
%% API
%%
-spec child_spec(mg_automoton:options()) ->
    supervisor:child_spec().
child_spec(Options) ->
    woody_server:child_spec(
        api,
        #{
            ip            => {0, 0, 0, 0},
            port          => 8820,
            net_opts      => [],
            event_handler => mg_woody_api_event_handler,
            handlers      => [mg_woody_api_automaton:handler(Options)]
        }
    ).
