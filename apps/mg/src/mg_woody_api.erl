%%%
%%% Про woody context.
%%% В процеессе обработки process_signal новый, т.к. нет вызывающего запроса,
%%% в process_call же вызывающий запрос есть и контекст берётся от него.
%%%
-module(mg_woody_api).

%% API
-export_type([options/0]).
-export([child_spec/1]).

%%
%% API
%%
-type options() :: #{
    automaton  => mg_woody_api_automaton :options(),
    event_sink => mg_woody_api_event_sink:options()
}.

% TODO прокидывание сетевых настроек
-spec child_spec(options()) ->
    supervisor:child_spec().
child_spec(Options) ->
    woody_server:child_spec(
        api,
        #{
            ip            => {0, 0, 0, 0},
            port          => 8820,
            net_opts      => [],
            event_handler => mg_woody_api_event_handler,
            handlers      => [
                mg_woody_api_automaton :handler(maps:get(automaton , Options)),
                mg_woody_api_event_sink:handler(maps:get(event_sink, Options))
            ]
        }
    ).
